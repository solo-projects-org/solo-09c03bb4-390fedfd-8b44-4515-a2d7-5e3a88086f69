"""Tests for the snapshot audit (app.audit and GET /table/audit)."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import delta
from app.audit import audit_table
from app.external_writer import append_rows
from app.sample_data import create_sample_table


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def table(tmp_path: Path) -> Path:
    t = tmp_path / "water_level"
    create_sample_table(t)
    return t


def _by_path(report: dict) -> dict[str, dict]:
    return {f["path"]: f for f in report["files"]}


def _sample_files(table: Path) -> list[str]:
    # Sample files are added at v1..v4 in time order (time_ns name prefix).
    return sorted(delta.load_snapshot(table).active)


# ---------------------------------------------------------------------------
# Healthy table
# ---------------------------------------------------------------------------


def test_audit_healthy_table_passes(table: Path) -> None:
    report = audit_table(table)
    assert report["passed"] is True
    assert report["requested_upto_version"] == 4
    assert report["latest_committed_version_at_start"] == 4
    assert report["last_verified_version"] == 4
    assert report["log_error"] is None
    assert report["unconfirmed_versions"] == []
    assert report["files_checked"] == 4
    assert report["files_failed"] == 0
    assert report["usable_versions"] == [(0, 4)]
    assert report["unusable_versions"] == []

    files = _by_path(report)
    for i, rel in enumerate(_sample_files(table), start=1):
        entry = files[rel]
        assert entry["status"] == "ok"
        assert entry["problems"] == []
        # Added at version i, never removed -> referenced by snapshots i..4.
        assert entry["active_in_versions"] == [(i, 4)]
        assert entry["expected_size_bytes"] == (table / rel).stat().st_size
        assert entry["actual_size_bytes"] == entry["expected_size_bytes"]
        assert entry["rows"] == 6
        assert entry["expected_rows"] == 6


def test_audit_upto_version_limits_scope(table: Path) -> None:
    # Later commits (a compaction at v5) must not leak into an audit that
    # was pinned to an older cutoff.
    chosen = _sample_files(table)[:2]
    delta.compact(table, chosen)
    assert delta.list_versions(table)[-1] == 5

    report = audit_table(table, upto_version=2)
    assert report["passed"] is True
    assert report["requested_upto_version"] == 2
    assert report["last_verified_version"] == 2
    assert report["latest_committed_version_at_start"] == 5
    assert report["files_checked"] == 2  # only files added in v1..v2
    files = _by_path(report)
    assert files[chosen[0]]["active_in_versions"] == [(1, 2)]
    assert files[chosen[1]]["active_in_versions"] == [(2, 2)]
    assert report["usable_versions"] == [(0, 2)]


def test_audit_empty_table_passes(tmp_path: Path) -> None:
    t = tmp_path / "empty"
    delta.init_table(t)
    report = audit_table(t)
    assert report["passed"] is True
    assert report["files_checked"] == 0
    assert report["last_verified_version"] == 0
    assert report["usable_versions"] == [(0, 0)]


def test_audit_unknown_version_raises(table: Path) -> None:
    with pytest.raises(delta.VersionNotFoundError):
        audit_table(table, upto_version=99)


def test_audit_missing_table_raises(tmp_path: Path) -> None:
    with pytest.raises(delta.TableNotFoundError):
        audit_table(tmp_path / "nope")


# ---------------------------------------------------------------------------
# File-level failures
# ---------------------------------------------------------------------------


def test_audit_missing_file_reports_affected_ranges(table: Path) -> None:
    victim = _sample_files(table)[1]  # added at v2, active in snapshots 2..4
    (table / victim).unlink()

    report = audit_table(table)
    assert report["passed"] is False
    assert report["files_checked"] == 4  # remaining files still checked
    assert report["files_failed"] == 1

    entry = _by_path(report)[victim]
    assert entry["status"] == "missing"
    assert entry["actual_size_bytes"] is None
    assert entry["rows"] is None
    assert entry["active_in_versions"] == [(2, 4)]
    assert any("does not exist" in p for p in entry["problems"])

    # Snapshots 2..4 reference the lost file; 0..1 remain recomputable.
    assert report["unusable_versions"] == [(2, 4)]
    assert report["usable_versions"] == [(0, 1)]


def test_audit_size_mismatch_but_readable(table: Path) -> None:
    victim = _sample_files(table)[0]
    expected_size = delta.load_snapshot(table).active[victim].size_bytes
    data = pq.read_table(table / victim)
    # Same rows, different codec -> valid parquet, different byte size.
    pq.write_table(data, table / victim, compression="gzip")
    assert (table / victim).stat().st_size != expected_size
    report = audit_table(table)

    entry = _by_path(report)[victim]
    assert entry["status"] == "size_mismatch"
    assert entry["rows"] == 6  # still fully readable
    assert entry["actual_size_bytes"] != entry["expected_size_bytes"]
    assert report["passed"] is False
    assert report["unusable_versions"] == [(1, 4)]


def test_audit_unreadable_file(table: Path) -> None:
    victim = _sample_files(table)[2]
    (table / victim).write_bytes(b"this is not a parquet file")

    report = audit_table(table)
    entry = _by_path(report)[victim]
    assert entry["status"] == "unreadable"
    assert entry["rows"] is None
    # The truncation/corruption is also visible as a size mismatch.
    assert any("bytes" in p for p in entry["problems"])
    assert any("read" in p for p in entry["problems"])
    assert report["files_checked"] == 4
    assert report["files_failed"] == 1
    assert report["unusable_versions"] == [(3, 4)]


def test_audit_schema_mismatch(table: Path) -> None:
    victim = _sample_files(table)[0]
    wrong = pa.table({"station": ["S"], "utc_seconds": [1]})  # column missing
    pq.write_table(wrong, table / victim)

    report = audit_table(table)
    entry = _by_path(report)[victim]
    assert entry["status"] == "schema_mismatch"
    assert any("schema" in p for p in entry["problems"])
    assert report["passed"] is False


def test_audit_row_count_mismatch(table: Path) -> None:
    victim = _sample_files(table)[0]
    data = pq.read_table(table / victim).slice(0, 5)  # drop one row
    pq.write_table(data, table / victim, compression="snappy", version="2.6")

    report = audit_table(table)
    entry = _by_path(report)[victim]
    assert entry["status"] == "row_count_mismatch"
    assert entry["rows"] == 5
    assert entry["expected_rows"] == 6
    assert report["passed"] is False


def test_audit_single_file_error_does_not_stop_others(
    table: Path, monkeypatch
) -> None:
    victim = _sample_files(table)[1]
    real = delta._relative_data_path

    def boom(tbl, path):
        if path == victim:
            raise RuntimeError("injected failure")
        return real(tbl, path)

    monkeypatch.setattr(delta, "_relative_data_path", boom)
    report = audit_table(table)

    assert report["files_checked"] == 4  # all files were attempted
    assert report["files_failed"] == 1
    entry = _by_path(report)[victim]
    assert entry["status"] == "check_error"
    assert any("injected failure" in p for p in entry["problems"])
    others = [f for f in report["files"] if f["path"] != victim]
    assert all(f["status"] == "ok" for f in others)


def test_audit_flags_illegal_path_recorded_in_log(table: Path) -> None:
    bad_add = {
        "add": {
            "path": "../outside.parquet",
            "size": 10,
            "modificationTime": 0,
            "dataChange": True,
        }
    }
    log = table / "_delta_log" / "00000000000000000005.json"
    log.write_text(json.dumps(bad_add) + "\n", encoding="utf-8")

    report = audit_table(table)
    entry = _by_path(report)["../outside.parquet"]
    assert entry["status"] == "invalid_path"
    assert report["passed"] is False
    assert report["unusable_versions"] == [(5, 5)]


# ---------------------------------------------------------------------------
# Compaction-removed files (still referenced by old snapshots)
# ---------------------------------------------------------------------------


def test_audit_checks_files_removed_by_compaction(table: Path) -> None:
    chosen = _sample_files(table)[:2]  # added at v1/v2, removed at v5
    result = delta.compact(table, chosen)
    assert result["version"] == 5
    (table / chosen[0]).unlink()  # physically lose the compacted-away file

    report = audit_table(table)
    files = _by_path(report)
    assert report["files_checked"] == 5  # 4 originals + 1 compacted output

    lost = files[chosen[0]]
    assert lost["status"] == "missing"
    assert lost["active_in_versions"] == [(1, 4)]  # old snapshots still need it
    assert files[chosen[1]]["status"] == "ok"
    assert files[chosen[1]]["active_in_versions"] == [(2, 4)]
    assert files[result["output_file"]]["status"] == "ok"
    assert files[result["output_file"]]["active_in_versions"] == [(5, 5)]

    assert report["passed"] is False
    assert report["unusable_versions"] == [(1, 4)]
    assert report["usable_versions"] == [(0, 0), (5, 5)]


def test_audit_of_old_cutoff_ignores_later_compaction(table: Path) -> None:
    chosen = _sample_files(table)[:2]
    delta.compact(table, chosen)
    # Auditing only up to v2 must not even look at the compacted output.
    report = audit_table(table, upto_version=2)
    assert report["files_checked"] == 2
    assert report["passed"] is True


# ---------------------------------------------------------------------------
# Broken transaction log
# ---------------------------------------------------------------------------


def test_audit_log_gap_marks_tail_unconfirmed(table: Path) -> None:
    (table / "_delta_log" / "00000000000000000003.json").unlink()

    report = audit_table(table)
    assert report["last_verified_version"] == 2
    assert report["log_error"]["version"] == 3
    assert "missing" in report["log_error"]["reason"]
    assert report["unconfirmed_versions"] == [(3, 4)]
    # The requested range must never be reported as passed.
    assert report["passed"] is False
    # Only files referenced by the verified prefix are reported.
    assert report["files_checked"] == 2
    assert report["usable_versions"] == [(0, 2)]
    assert report["unusable_versions"] == []


def test_audit_corrupt_log_json_marks_tail_unconfirmed(table: Path) -> None:
    (table / "_delta_log" / "00000000000000000002.json").write_text(
        "{not valid json\n", encoding="utf-8"
    )

    report = audit_table(table)
    assert report["last_verified_version"] == 1
    assert report["log_error"]["version"] == 2
    assert "invalid JSON" in report["log_error"]["reason"]
    assert report["unconfirmed_versions"] == [(2, 4)]
    assert report["passed"] is False
    assert report["files_checked"] == 1


def test_audit_structurally_corrupt_log_actions(table: Path) -> None:
    log = table / "_delta_log" / "00000000000000000005.json"
    log.write_text(json.dumps({"add": {"size": 3}}) + "\n", encoding="utf-8")

    report = audit_table(table)
    assert report["last_verified_version"] == 4
    assert report["log_error"]["version"] == 5
    assert "path" in report["log_error"]["reason"]
    assert report["unconfirmed_versions"] == [(5, 5)]
    assert report["passed"] is False


def test_audit_log_broken_at_version_zero(tmp_path: Path) -> None:
    t = tmp_path / "broken"
    create_sample_table(t)
    (t / "_delta_log" / "00000000000000000000.json").unlink()

    report = audit_table(t)
    assert report["last_verified_version"] is None
    assert report["log_error"]["version"] == 0
    assert report["unconfirmed_versions"] == [(0, 4)]
    assert report["passed"] is False
    assert report["files_checked"] == 0
    assert report["usable_versions"] == []


# ---------------------------------------------------------------------------
# Concurrency: external writers keep committing during the audit
# ---------------------------------------------------------------------------


def test_audit_ignores_inflight_and_unreferenced_files(table: Path) -> None:
    staging = table / delta.STAGING_DIR
    staging.mkdir(exist_ok=True)
    # Half-written product of an in-flight external append.
    (staging / "append-halfwritten.parquet").write_bytes(b"not parquet yet")
    # In-flight log commit temp file.
    (table / "_delta_log" / ".tmp-inflight-00000000000000000005.json").write_text(
        "{}\n", encoding="utf-8"
    )
    # Orphaned final-path parquet no committed version references.
    (table / "9999999999999999999-deadbeefdeadbeefdeadbeefdeadbeef.parquet").write_bytes(
        b"junk"
    )

    report = audit_table(table)
    assert report["passed"] is True
    assert report["files_checked"] == 4
    paths = [f["path"] for f in report["files"]]
    assert all("_staging" not in p and "orphan" not in p for p in paths)


def test_audit_runs_while_external_writer_commits(table: Path) -> None:
    reports: list[dict] = []
    writer_errors: list[Exception] = []

    def writer() -> None:
        try:
            for i in range(8):
                append_rows(
                    table,
                    [{"station": "ST-LIVE", "utc_seconds": i, "level_mm": i}],
                )
        except Exception as exc:  # pragma: no cover - defensive
            writer_errors.append(exc)

    t = threading.Thread(target=writer)
    t.start()
    while t.is_alive():
        reports.append(audit_table(table))
    t.join()
    assert not writer_errors
    assert reports  # the loop ran at least once while commits landed

    for report in reports:
        # Each report only covers the committed range fixed at its start...
        assert report["last_verified_version"] == report["requested_upto_version"]
        assert report["unconfirmed_versions"] == []
        # ...and committed adds always have their data file published first,
        # so a live writer never causes a false alarm.
        assert report["passed"] is True, report["files"]
        assert all(
            "_staging" not in f["path"] and ".tmp" not in f["path"]
            for f in report["files"]
        )

    final = audit_table(table)
    assert final["requested_upto_version"] == 12  # 4 sample + 8 live appends
    assert final["files_checked"] == 12
    assert final["passed"] is True


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(tmp_path: Path, monkeypatch) -> TestClient:
    data_root = tmp_path / "data"
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    monkeypatch.setenv("TABLE_NAME", "water_level")
    import app.main as main

    monkeypatch.setattr(main, "DATA_ROOT", data_root)
    monkeypatch.setattr(main, "TABLE_DIR", data_root / "water_level")
    with TestClient(main.app) as c:
        yield c


def test_api_audit_healthy(client: TestClient) -> None:
    r = client.get("/table/audit")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["table"] == "water_level"
    assert body["passed"] is True
    assert body["requested_upto_version"] == 4
    assert body["latest_committed_version_at_start"] == 4
    assert body["last_verified_version"] == 4
    assert body["log_error"] is None
    assert body["unconfirmed_versions"] == []
    assert body["usable_versions"] == [{"start": 0, "end": 4}]
    assert body["unusable_versions"] == []
    assert body["files_checked"] == 4
    assert body["files_failed"] == 0
    assert len(body["files"]) == 4
    first = body["files"][0]
    assert first["status"] == "ok"
    assert first["problems"] == []
    assert first["active_in_versions"] == [{"start": 1, "end": 4}]
    assert first["rows"] == 6


def test_api_audit_with_upto_version(client: TestClient) -> None:
    r = client.get("/table/audit", params={"upto_version": 2})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["requested_upto_version"] == 2
    assert body["last_verified_version"] == 2
    assert body["files_checked"] == 2
    assert body["passed"] is True


def test_api_audit_unknown_version_is_404(client: TestClient) -> None:
    r = client.get("/table/audit", params={"upto_version": 99})
    assert r.status_code == 404


def test_api_audit_negative_version_is_422(client: TestClient) -> None:
    r = client.get("/table/audit", params={"upto_version": -1})
    assert r.status_code == 422


def test_api_audit_reports_broken_file(client: TestClient) -> None:
    from app import main

    victim = sorted(delta.load_snapshot(main.TABLE_DIR).active)[0]
    (main.TABLE_DIR / victim).unlink()

    r = client.get("/table/audit")
    assert r.status_code == 200, r.text  # audit ran; failures are data
    body = r.json()
    assert body["passed"] is False
    assert body["files_failed"] == 1
    entry = next(f for f in body["files"] if f["path"] == victim)
    assert entry["status"] == "missing"
    assert entry["active_in_versions"] == [{"start": 1, "end": 4}]
    assert body["unusable_versions"] == [{"start": 1, "end": 4}]
    assert body["usable_versions"] == [{"start": 0, "end": 0}]


def test_api_audit_broken_log_not_passed(client: TestClient) -> None:
    from app import main

    (main.TABLE_DIR / "_delta_log" / "00000000000000000004.json").write_text(
        "garbage\n", encoding="utf-8"
    )
    r = client.get("/table/audit")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["passed"] is False
    assert body["last_verified_version"] == 3
    assert body["log_error"]["version"] == 4
    assert body["unconfirmed_versions"] == [{"start": 4, "end": 4}]
