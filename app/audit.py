"""Snapshot audit for the water-level Delta table.

The lakehouse operator uses this module (exposed as ``GET /table/audit``) to
find out which historical snapshots are still usable for recomputation jobs
after storage trouble.  Given a cutoff version, the audit replays the
committed transaction log from version 0 up to the cutoff and verifies
**every parquet file any of those snapshots ever referenced** - including
files a later compaction removed from the active set but old snapshots still
point at (physical files are retained for time travel, so they must be
checked too).

For each referenced file the audit verifies that it

* exists on disk,
* has exactly the size the Delta ``add`` action recorded,
* can be read back in full and matches the table schema (column names,
  types, nullability and the non-null data contract), and
* has the row count the commit statistics recorded (when present).

Concurrency contract
--------------------

External writers keep committing while an audit runs.  The audit therefore

* fixes the committed version range **once, at the start**.  Committed log
  versions are immutable and in-flight ``.tmp-*`` log files are ignored, so
  commits landing mid-audit simply are not part of this report;
* never looks at ``_staging`` files or unreferenced parquet files, so
  half-written products of in-flight writers are never mistaken for table
  data and cannot be flagged;
* keeps checking the remaining files when a single file fails.

If the transaction log itself is broken (a missing version or unparsable
JSON), replay stops at the last verified version: every version after it is
reported as *unconfirmed* and the requested range is never reported as
passed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from . import delta
from .delta import TableCorruptError, TableNotFoundError, VersionNotFoundError

# Per-file verdicts ordered by severity (worst first).  When several checks
# fail for one file, the most severe failure becomes ``status`` while
# ``problems`` still lists every reason.
_SEVERITY = [
    "check_error",
    "invalid_path",
    "missing",
    "unreadable",
    "schema_mismatch",
    "row_count_mismatch",
    "size_mismatch",
]


@dataclass
class _FileRef:
    """Book-keeping for one parquet path seen while replaying the log."""

    expected_size: int | None = None
    expected_rows: int | None = None
    # Inclusive [start, end] snapshot-version ranges in which the path is
    # part of a snapshot.  The last range is open (end=None) while the file
    # is still active at the replay frontier.
    ranges: list[list[int | None]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Log scanning (audit-specific: must tolerate a broken log instead of raising)
# ---------------------------------------------------------------------------


def _present_log_versions(table: Path) -> list[int]:
    """Sorted committed version numbers visible right now.

    In-flight commits (``.tmp-*`` files) are ignored, exactly like
    :func:`delta.list_versions` does, so a writer that is mid-commit can
    never be mistaken for corruption.
    """
    log_dir = delta.log_path(table, 0).parent
    if not log_dir.is_dir():
        raise TableNotFoundError(f"no Delta transaction log under {table}")
    versions = sorted(
        int(entry.stem)
        for entry in log_dir.glob("*.json")
        if len(entry.stem) == 20 and entry.stem.isdigit()
    )
    if not versions:
        raise TableCorruptError("transaction log directory contains no JSON files")
    return versions


def _parse_add_stats(add: dict[str, Any]) -> tuple[int | None, int | None]:
    """Best-effort extraction of (size, numRecords) from an add action."""
    size: int | None = None
    try:
        size = int(add["size"])
    except (KeyError, TypeError, ValueError):
        pass
    rows: int | None = None
    stats = add.get("stats")
    if isinstance(stats, str):
        try:
            num_records = json.loads(stats).get("numRecords")
            rows = int(num_records) if num_records is not None else None
        except (ValueError, TypeError, AttributeError):
            rows = None
    return size, rows


def _apply_actions(
    actions: list[dict[str, Any]],
    version: int,
    refs: dict[str, _FileRef],
    order: list[str],
) -> str | None:
    """Apply one version's add/remove actions to the reference table.

    The whole version is validated before anything is applied, so a
    structurally corrupt version (valid JSON but not valid Delta actions)
    is rejected as a unit and never partially replayed.  Returns an error
    string on corruption, ``None`` on success.
    """
    for action in actions:
        if not isinstance(action, dict):
            return f"non-object action in log version {version}"
        for kind in ("add", "remove"):
            if kind in action:
                body = action[kind]
                path = body.get("path") if isinstance(body, dict) else None
                if not isinstance(path, str) or not path:
                    return (
                        f"{kind} action without a valid path in "
                        f"log version {version}"
                    )
    for action in actions:
        if "add" in action:
            path = action["add"]["path"]
            ref = refs.get(path)
            if ref is None:
                ref = refs[path] = _FileRef()
                order.append(path)
            if ref.ranges and ref.ranges[-1][1] is None:
                # Path re-added while still active (foreign writer): close
                # the previous stint before opening a new one.
                ref.ranges[-1][1] = version - 1
            ref.ranges.append([version, None])
            ref.expected_size, ref.expected_rows = _parse_add_stats(action["add"])
        elif "remove" in action:
            path = action["remove"]["path"]
            ref = refs.get(path)
            if ref is not None and ref.ranges and ref.ranges[-1][1] is None:
                ref.ranges[-1][1] = version - 1
    return None


# ---------------------------------------------------------------------------
# Range arithmetic on inclusive [start, end] version ranges
# ---------------------------------------------------------------------------


def _merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Union of inclusive ranges; overlapping or adjacent ranges merge."""
    merged: list[list[int]] = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def _complement(
    lo: int, hi: int, blocked: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """The part of ``[lo, hi]`` not covered by the (merged) *blocked* ranges."""
    free: list[tuple[int, int]] = []
    cursor = lo
    for start, end in blocked:
        if start > cursor:
            free.append((cursor, start - 1))
        cursor = max(cursor, end + 1)
    if cursor <= hi:
        free.append((cursor, hi))
    return free


# ---------------------------------------------------------------------------
# Per-file verification
# ---------------------------------------------------------------------------


def _check_file(table: Path, path: str, ref: _FileRef) -> dict[str, Any]:
    """Verify one referenced parquet file; never raises.

    A failure of a single check (or an unexpected error) is recorded in the
    entry and the audit continues with the next file.
    """
    problems: list[tuple[str, str]] = []
    actual_size: int | None = None
    rows: int | None = None
    try:
        try:
            abs_path = delta._relative_data_path(table, path)
        except delta.InvalidRequestError as exc:
            problems.append(
                (
                    "invalid_path",
                    f"path recorded in the delta log is not a table data file: {exc}",
                )
            )
        else:
            if not abs_path.is_file():
                problems.append(
                    (
                        "missing",
                        "file referenced by the delta log does not exist on disk",
                    )
                )
            else:
                actual_size = abs_path.stat().st_size
                if ref.expected_size is not None and actual_size != ref.expected_size:
                    problems.append(
                        (
                            "size_mismatch",
                            f"delta log records {ref.expected_size} bytes but the "
                            f"file on disk has {actual_size} bytes",
                        )
                    )
                try:
                    data = pq.read_table(abs_path)
                    rows = data.num_rows
                except Exception as exc:  # pyarrow raises several error types
                    problems.append(
                        ("unreadable", f"parquet cannot be read back: {exc}")
                    )
                else:
                    if data.schema != delta.TABLE_SCHEMA:
                        problems.append(
                            (
                                "schema_mismatch",
                                f"file schema {data.schema} does not match the "
                                f"table schema {delta.TABLE_SCHEMA}",
                            )
                        )
                    elif data.num_rows > 0 and sum(
                        data.column(i).null_count for i in range(data.num_columns)
                    ) != 0:
                        problems.append(
                            (
                                "schema_mismatch",
                                "file contains null values, violating the "
                                "non-null table contract",
                            )
                        )
                    if ref.expected_rows is not None and rows != ref.expected_rows:
                        problems.append(
                            (
                                "row_count_mismatch",
                                f"commit statistics record {ref.expected_rows} "
                                f"rows but the file contains {rows}",
                            )
                        )
    except Exception as exc:  # one bad file must never abort the audit
        problems.append(("check_error", f"unexpected error while checking: {exc}"))

    status = "ok"
    if problems:
        status = min(problems, key=lambda p: _SEVERITY.index(p[0]))[0]
    return {
        "path": path,
        "status": status,
        "problems": [reason for _, reason in problems],
        "expected_size_bytes": ref.expected_size,
        "actual_size_bytes": actual_size,
        "expected_rows": ref.expected_rows,
        "rows": rows,
        "active_in_versions": [(r[0], r[1]) for r in ref.ranges],
    }


# ---------------------------------------------------------------------------
# Audit entry point
# ---------------------------------------------------------------------------


def audit_table(table: Path | str, upto_version: int | None = None) -> dict[str, Any]:
    """Audit every parquet file referenced by snapshots 0..*upto_version*.

    *upto_version* defaults to the newest committed version visible when the
    audit starts; the committed range is fixed at that moment and commits
    landing while the audit runs are not part of the report.

    The returned mapping has the shape of the API response (minus the table
    name); version ranges are inclusive ``(start, end)`` tuples.
    """
    table = Path(table)
    # --- fix the committed range once, at the start -------------------------
    present = _present_log_versions(table)
    newest_at_start = present[-1]
    if upto_version is None:
        requested = newest_at_start
    else:
        if upto_version > newest_at_start:
            raise VersionNotFoundError(f"version {upto_version} does not exist")
        requested = upto_version

    # --- replay the fixed range, stopping at the first broken version -------
    refs: dict[str, _FileRef] = {}
    order: list[str] = []
    last_verified: int | None = None
    log_error: dict[str, Any] | None = None
    for version in range(0, requested + 1):
        path = delta.log_path(table, version)
        if not path.is_file():
            log_error = {
                "version": version,
                "reason": f"transaction log file {path.name} is missing "
                "(gap in the contiguous log)",
            }
            break
        try:
            actions = delta.read_log(table, version)
        except VersionNotFoundError:
            # Lost a race with a log cleaner: same outcome as a gap.
            log_error = {
                "version": version,
                "reason": f"transaction log file {path.name} is missing "
                "(gap in the contiguous log)",
            }
            break
        except TableCorruptError as exc:
            log_error = {"version": version, "reason": str(exc)}
            break
        structural_error = _apply_actions(actions, version, refs, order)
        if structural_error is not None:
            log_error = {"version": version, "reason": structural_error}
            break
        last_verified = version

    # Close ranges still open at the verified frontier: a file active when
    # replay stopped is referenced through the last verified version, and we
    # know nothing beyond it.
    for ref in refs.values():
        if ref.ranges and ref.ranges[-1][1] is None:
            ref.ranges[-1][1] = last_verified if last_verified is not None else -1

    # --- verify every referenced file, one failure never stops the rest -----
    files = [_check_file(table, path, refs[path]) for path in order]
    failed = [f for f in files if f["status"] != "ok"]

    # --- summarise snapshot usability over the verified range ---------------
    unusable = _merge_ranges(
        [r for f in failed for r in f["active_in_versions"]]
    )
    usable = (
        _complement(0, last_verified, unusable)
        if last_verified is not None
        else []
    )
    unconfirmed: list[tuple[int, int]] = []
    if log_error is not None:
        lo = 0 if last_verified is None else last_verified + 1
        if lo <= requested:
            unconfirmed.append((lo, requested))

    return {
        "requested_upto_version": requested,
        "latest_committed_version_at_start": newest_at_start,
        "last_verified_version": last_verified,
        "unconfirmed_versions": unconfirmed,
        "log_error": log_error,
        # A broken log tail means the requested range can never be "passed".
        "passed": log_error is None and not failed,
        "usable_versions": usable,
        "unusable_versions": unusable,
        "files_checked": len(files),
        "files_failed": len(failed),
        "files": files,
    }
