"""Demo 4 - audit which historical snapshots remain usable for recomputation.

The audit endpoint replays the committed log up to a cutoff version and
verifies every parquet file those snapshots ever referenced - including
files a compaction removed but old snapshots still point at.  This demo:

1. audits the live table through the API (healthy -> passed),
2. audits an old cutoff version (time-travel audit),
3. compacts and re-audits: the compacted-away files are still checked,
4. on a throw-away COPY of the table, deletes/corrupts files and breaks the
   log to show per-file failure reasons, affected snapshot ranges and the
   "unconfirmed" tail (the shared demo table is never modified).

Usage:
    python scripts/demo_audit.py
    API_1=http://127.0.0.1:8001 TABLE_DIR=/data/water_level python scripts/demo_audit.py
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._demo_common import API_1, TABLE_DIR, banner, api  # noqa: E402


def _bounds(r) -> tuple[int, int]:
    # API responses use {"start": .., "end": ..}; app.audit returns tuples.
    return (r["start"], r["end"]) if isinstance(r, dict) else (r[0], r[1])


def _fmt(ranges) -> str:
    return ", ".join(f"v{s}..v{e}" for s, e in map(_bounds, ranges)) or "-"


def _show_audit(body: dict) -> None:
    print(
        f"  audited range 0..{body['requested_upto_version']} "
        f"(latest at start: v{body['latest_committed_version_at_start']}, "
        f"verified through: v{body['last_verified_version']})"
    )
    print(
        f"  passed={body['passed']}  files: {body['files_checked']} checked, "
        f"{body['files_failed']} failed"
    )
    print(f"  usable snapshots:     {_fmt(body['usable_versions'])}")
    print(f"  unusable snapshots:   {_fmt(body['unusable_versions'])}")
    print(f"  unconfirmed versions: {_fmt(body['unconfirmed_versions'])}")
    for f in body["files"]:
        if f["status"] != "ok":
            print(f"  FAILED {f['path']}: {f['status']}")
            for problem in f["problems"]:
                print(f"    - {problem}")
            print(f"    affected snapshots: {_fmt(f['active_in_versions'])}")


def main() -> None:
    client = api(API_1)
    banner("Demo 4a: audit the live table (all committed versions)")
    body = client.get("/table/audit").json()
    _show_audit(body)
    assert body["passed"], "fresh demo table should audit clean"

    banner("Demo 4b: time-travel audit pinned to an old cutoff version")
    old = client.get("/table/audit", params={"upto_version": 2}).json()
    _show_audit(old)
    assert old["requested_upto_version"] == 2
    assert old["last_verified_version"] == 2

    banner("Demo 4c: compaction keeps old snapshots auditable")
    files = client.get("/table/snapshot").json()["active_files"][:2]
    r = client.post("/compact", json={"files": files})
    r.raise_for_status()
    committed = r.json()["committed_version"]
    print(f"compacted {len(files)} files -> committed v{committed}")
    body = client.get("/table/audit").json()
    _show_audit(body)
    by_path = {f["path"]: f for f in body["files"]}
    for removed in files:
        entry = by_path[removed]
        assert entry["status"] == "ok"
        last = entry["active_in_versions"][-1]["end"]
        print(
            f"  compacted-away {removed[:36]}... still verified; "
            f"referenced by snapshots up to v{last}"
        )
        assert last == committed - 1
    assert body["passed"]

    banner("Demo 4d: missing/corrupt files and a broken log (on a copy)")
    with tempfile.TemporaryDirectory() as tmp:
        broken = Path(tmp) / "water_level"
        shutil.copytree(TABLE_DIR, broken)

        from app import delta
        from app.audit import audit_table

        snap = delta.load_snapshot(broken)
        victim = sorted(snap.active)[0]
        (broken / victim).unlink()  # lost data file
        corrupt = sorted(snap.active)[1]
        (broken / corrupt).write_bytes(b"garbage")  # corrupted data file

        report = audit_table(broken)
        print("after losing one file and corrupting another:")
        _show_audit(report)
        assert not report["passed"]
        statuses = {f["path"]: f["status"] for f in report["files"]}
        assert statuses[victim] == "missing"
        assert statuses[corrupt] == "unreadable"

        # Break the log itself: the tail becomes unconfirmable.
        latest = delta.list_versions(broken)[-1]
        (broken / "_delta_log" / f"{latest:020d}.json").write_text("{broken\n")
        report = audit_table(broken)
        print(f"\nafter corrupting log version {latest}:")
        print(f"  last verified version: v{report['last_verified_version']}")
        print(f"  log error: {report['log_error']}")
        print(f"  unconfirmed: {report['unconfirmed_versions']}")
        print(f"  passed: {report['passed']}  (never passes with a broken log)")
        assert report["last_verified_version"] == latest - 1
        assert report["unconfirmed_versions"] == [(latest, latest)]
        assert report["passed"] is False

    print("\nOK: audits pinpoint exactly which snapshots remain recomputable.")


if __name__ == "__main__":
    main()
