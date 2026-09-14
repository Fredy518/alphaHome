import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from alphahome.common.db_manager import DBManager
from alphahome.factors.governance import FactorGovernanceStore
from alphahome.factors.locks import repair_session, snapshot_gate
from alphahome.factors.persistence import FactorSnapshotWriter, G_FACTOR_COLUMNS, P_FACTOR_COLUMNS
from alphahome.factors.repair import FactorRepairService
from test_factor_governance_pipeline import _p_frame


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]
DAY = date(2026, 9, 11)


@pytest.fixture
def database(isolated_database_url):
    db = DBManager(isolated_database_url, mode="sync")
    if db.fetch_val_sync("SELECT to_regnamespace('factors') IS NOT NULL"):
        db.close_sync()
        pytest.skip("repair test needs an unused factors schema")
    had_status = db.fetch_val_sync("SELECT to_regclass('public.task_status') IS NOT NULL")
    db.execute_sync("CREATE SCHEMA factors")
    try:
        for kind, columns in (("p", P_FACTOR_COLUMNS), ("g", G_FACTOR_COLUMNS)):
            def column_type(name):
                if name in {"calc_date", "ann_date", "end_date"}:
                    return "date"
                if name in {"ts_code", "data_source", "data_quality", "calculation_status"}:
                    return "text"
                return "numeric"
            declarations = ", ".join(f'{column} {column_type(column)}' for column in columns)
            db.execute_sync(f"CREATE TABLE factors.{kind}_factor ({declarations}, PRIMARY KEY (ts_code, calc_date))")
        governance = FactorGovernanceStore(db)
        governance.ensure_schema()
        service = FactorRepairService(db)
        service._ensure_repair_tables()
        service._install_weekday_constraints(validate=True)
        yield db
    finally:
        db.execute_sync("DROP SCHEMA factors CASCADE")
        if not had_status:
            db.execute_sync("DROP TABLE public.task_status")
        db.close_sync()


def prepare(db, *, complete=True, write=True):
    service = FactorRepairService(db)
    governance = service.governance
    old = governance.start_run(["factor_p"], "full", DAY)
    writer = FactorSnapshotWriter(db)
    writer.write(_p_frame(score=50), "p", DAY, run_id=old, task_name="factor_p")
    repair_run = governance.start_run(["factor_p"], "repair", DAY)
    repair_id = uuid4()
    service._insert_manifest(repair_id, "running", datetime.now(timezone.utc), DAY, details={
        "rollback_contract": "repair_owner_v1", "factor_run_id": str(repair_run),
        "original_weekday_constraints": service._weekday_constraints(),
    })
    with repair_session(db):
        service._prepare_date(repair_id, "p", DAY, "test_repair")
        if write:
            count, checksum, _ = writer.write(_p_frame(score=90), "p", DAY, run_id=repair_run, task_name="factor_p")
            if complete:
                service._complete_date(repair_id, "p", DAY, count, checksum)
    return service, repair_id, old, repair_run


def state(db):
    return (
        db.fetch_val_sync("SELECT p_score FROM factors.p_factor"),
        db.fetch_val_sync("SELECT run_id::text FROM factors.factor_run_date WHERE is_current"),
        db.fetch_val_sync("SELECT status FROM factors.factor_repair_manifest"),
        db.fetch_val_sync("SELECT oid FROM pg_constraint WHERE conname='ck_p_factor_calc_date_friday'"),
    )


@pytest.mark.parametrize("complete,write", [(True, True), (False, True), (False, False)])
def test_immediate_and_crash_recovery_restore_exact_owner(database, complete, write):
    service, repair_id, old, _ = prepare(database, complete=complete, write=write)
    before = state(database)
    result = service.rollback(repair_id)
    assert result["restored"]["p"] == 1
    assert state(database) == (50, str(old), "rolled_back", before[3])
    assert service.rollback(repair_id)["status"] == "already_rolled_back"


@pytest.mark.parametrize("new_score", [90, 95])
def test_newer_run_rejects_old_rollback_even_when_values_match(database, new_score):
    service, repair_id, _, _ = prepare(database)
    newer = service.governance.start_run(["factor_p"], "manual", DAY)
    FactorSnapshotWriter(database).write(_p_frame(score=new_score), "p", DAY, run_id=newer, task_name="factor_p")
    before = state(database)
    with pytest.raises(RuntimeError, match="Stale"):
        service.rollback(repair_id)
    assert state(database) == before


@pytest.mark.parametrize("changed", ["current", "archive"])
def test_untracked_rewrite_or_archive_tampering_rejects_rollback(database, changed):
    service, repair_id, _, _ = prepare(database)
    if changed == "current":
        database.execute_sync("UPDATE factors.p_factor SET p_score=p_score")
    else:
        database.execute_sync("UPDATE factors.p_factor_repair_archive SET p_score=17")
    before = state(database)
    with pytest.raises(RuntimeError, match="Stale"):
        service.rollback(repair_id)
    assert state(database) == before


def test_restore_failure_rolls_back_data_ledger_and_manifest_together(database):
    service, repair_id, _, _ = prepare(database)
    database.execute_sync("""
        CREATE FUNCTION factors.fail_restore() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'restore rejected'; END $$;
        CREATE TRIGGER reject_restore BEFORE INSERT ON factors.p_factor
        FOR EACH ROW EXECUTE FUNCTION factors.fail_restore();
    """)
    before = state(database)
    with pytest.raises(Exception, match="restore rejected"):
        service.rollback(repair_id)
    assert state(database) == before


@pytest.mark.parametrize("replaced", [False, True])
def test_only_constraints_installed_by_this_repair_can_be_removed(database, replaced):
    service = FactorRepairService(database)
    service._drop_weekday_constraints()
    service, repair_id, old, _ = prepare(database)
    service._install_weekday_constraints(validate=True, repair_id=repair_id)
    if replaced:
        service._drop_weekday_constraints()
        service._install_weekday_constraints(validate=True)
        before = state(database)
        with pytest.raises(RuntimeError, match="Constraint ownership"):
            service.rollback(repair_id)
        assert state(database) == before
    else:
        service.rollback(repair_id)
        assert state(database) == (50, str(old), "rolled_back", None)


def test_concurrent_new_writer_finishes_before_rollback_rechecks_owner(database, isolated_database_url):
    _, repair_id, _, _ = prepare(database)
    newer = FactorGovernanceStore(database).start_run(["factor_p"], "manual", DAY)
    started, release = threading.Event(), threading.Event()

    def write_new():
        db = DBManager(isolated_database_url, mode="sync")
        try:
            conn = db._get_sync_connection()
            with conn.cursor() as cursor:
                snapshot_gate(cursor)
                cursor.execute("UPDATE factors.p_factor SET p_score=95")
                FactorGovernanceStore.record_date_cursor(cursor, newer, "factor_p", DAY, "success", output_count=1, is_current=True)
                started.set()
                assert release.wait(5)
            conn.commit()
        finally:
            db.close_sync()

    def roll_back():
        db = DBManager(isolated_database_url, mode="sync")
        try:
            return FactorRepairService(db).rollback(repair_id)
        finally:
            db.close_sync()

    # Release the main connection's read transaction before waiting on workers.
    database._get_sync_connection().rollback()
    with ThreadPoolExecutor(2) as pool:
        writer = pool.submit(write_new)
        assert started.wait(3)
        rollback = pool.submit(roll_back)
        try:
            for _ in range(100):
                if database.fetch_val_sync("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted"):
                    break
                time.sleep(0.01)
            else:
                pytest.fail("rollback never waited for the concurrent writer")
            assert not rollback.done()
        finally:
            release.set()
        writer.result(timeout=5)
        with pytest.raises(RuntimeError, match="Stale"):
            rollback.result(timeout=5)
    assert state(database)[:3] == (95, str(newer), "running")


def test_session_repair_lock_blocks_normal_snapshot_writer(database, isolated_database_url):
    _, _, _, _ = prepare(database)
    started = threading.Event()

    def write_new():
        db = DBManager(isolated_database_url, mode="sync")
        try:
            started.set()
            FactorSnapshotWriter(db).write(_p_frame(score=95), "p", DAY)
        finally:
            db.close_sync()

    with ThreadPoolExecutor(1) as pool:
        with repair_session(database):
            future = pool.submit(write_new)
            assert started.wait(3)
            for _ in range(100):
                if database.fetch_val_sync("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted"):
                    break
                time.sleep(0.01)
            else:
                pytest.fail("snapshot writer did not wait for repair")
            assert not future.done()
        future.result(timeout=5)
    assert state(database)[0] == 95
