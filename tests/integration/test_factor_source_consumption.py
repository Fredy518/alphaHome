from dataclasses import replace
from datetime import date, datetime, timezone
import asyncio

import pytest
import psycopg2

from alphahome.common.db_manager import DBManager
from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.governance import FactorGovernanceStore
from alphahome.factors.repository import FactorRepository
from alphahome.factors.source_boundary import WATERMARK_CONTRACT


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]
SOURCE = "pit.pit_financial_indicators"
T1 = datetime(2026, 9, 11, 10, tzinfo=timezone.utc)
T2 = datetime(2026, 9, 11, 11, tzinfo=timezone.utc)


@pytest.fixture
def database(isolated_database_url):
    db = DBManager(isolated_database_url, mode="sync")
    if db.fetch_val_sync("SELECT to_regnamespace('pit') IS NOT NULL OR to_regnamespace('factors') IS NOT NULL"):
        db.close_sync()
        pytest.skip("source-consumption test needs unused pit/factors schemas")
    db.execute_sync("""
        CREATE SCHEMA pit; CREATE SCHEMA factors;
        CREATE TABLE pit.pit_financial_indicators (
            ts_code text PRIMARY KEY, ann_date date, value integer,
            calculation_status text, updated_at timestamptz
        );
        CREATE TABLE factors.p_factor (ts_code text, calc_date date, value integer,
            updated_at timestamptz DEFAULT now(), PRIMARY KEY(ts_code,calc_date));
        CREATE TABLE factors.g_factor (ts_code text, calc_date date, value integer,
            updated_at timestamptz DEFAULT now(), PRIMARY KEY(ts_code,calc_date));
    """)
    db.execute_sync(f"INSERT INTO {SOURCE} VALUES ('A','2026-09-11',1,'success',%s)", (T1,))
    FactorGovernanceStore(db).ensure_schema()
    try:
        yield db
    finally:
        db.execute_sync("DROP SCHEMA factors CASCADE; DROP SCHEMA pit CASCADE; DROP TABLE public.task_status")
        db.close_sync()


def update_source(db):
    db.execute_sync(f"UPDATE {SOURCE} SET value=2, updated_at=%s", (T2,))


def test_stock_master_eligibility_projection_ignores_timestamp_only_updates(database):
    from alphahome.factors.source_boundary import SNAPSHOT_XMIN_KEY

    assert not database.fetch_val_sync("SELECT to_regnamespace('tushare') IS NOT NULL")
    database.execute_sync("""CREATE SCHEMA tushare;
        CREATE TABLE tushare.stock_basic (ts_code text PRIMARY KEY, list_status text,
            list_date date, delist_date date, update_time timestamptz);
        INSERT INTO tushare.stock_basic VALUES ('A','L','2020-01-01',NULL,now());""")
    try:
        contract = replace(FactorCoordinator.contracts()["factor_p"], source_tables=("tushare.stock_basic",))
        repository = FactorRepository(database)
        baseline = {**repository.source_watermarks(contract), SNAPSHOT_XMIN_KEY: repository.snapshot_xmin()}
        database.execute_sync("UPDATE tushare.stock_basic SET update_time=now()")
        assert repository.dirty_start_date(contract, baseline) is None
        database.execute_sync("UPDATE tushare.stock_basic SET list_status='D',delist_date='2026-09-01'")
        assert repository.dirty_start_date(contract, baseline) == date(1900,1,1)
        database.execute_sync("DELETE FROM tushare.stock_basic")
        assert repository.source_watermarks(contract) != {key: value for key, value in baseline.items() if key != SNAPSHOT_XMIN_KEY}
    finally:
        database.execute_sync("DROP SCHEMA tushare CASCADE")


def test_submitted_plan_rejects_source_drift_before_run_ledger(database, monkeypatch):
    from alphahome.common.run_models import RunPlan

    coordinator = FactorCoordinator(database)
    contract = replace(coordinator.contracts()["factor_p"], source_tables=(SOURCE,), readiness_dependencies=())
    monkeypatch.setattr(coordinator, "contracts", lambda: {"factor_p": contract})
    plan = coordinator.plan(["factor_p"], mode="manual", start_date="2026-09-11", end_date="2026-09-11", batch_started_at=date(2026,9,14))
    submitted = RunPlan.from_dict(plan.immutable_plan.to_dict())
    assert submitted.plan_hash == plan.plan_hash
    before = database.fetch_val_sync("SELECT count(*) FROM factors.factor_run")
    update_source(database)
    with pytest.raises(RuntimeError, match="changed"):
        coordinator.run(["factor_p"], mode="manual", submitted_plan=submitted)
    assert database.fetch_val_sync("SELECT count(*) FROM factors.factor_run") == before


async def test_gui_and_cli_publish_identical_immutable_factor_plan(database, monkeypatch):
    from types import SimpleNamespace
    from alphahome.gui.services import factor_service
    from alphahome.factors import command

    contract = replace(FactorCoordinator.contracts()["factor_p"], source_tables=(SOURCE,), readiness_dependencies=())
    monkeypatch.setattr(FactorCoordinator, "contracts", staticmethod(lambda: {"factor_p": contract}))
    monkeypatch.setattr(factor_service.UnifiedTaskFactory, "get_db_manager", lambda: SimpleNamespace(connection_string=database.connection_string))
    messages, cli_payloads = [], []
    monkeypatch.setattr(factor_service, "_send_response_callback", lambda kind, payload: messages.append((kind, payload)))
    monkeypatch.setattr(command.ConfigManager, "get_database_url", lambda self: database.connection_string)
    monkeypatch.setattr(command, "_print_json", cli_payloads.append)
    await factor_service.handle_preflight(["factor_p"], "manual", "2026-09-11", "2026-09-11")
    assert command.main(["run", "--tasks", "p", "--mode", "manual", "--start-date", "2026-09-11", "--end-date", "2026-09-11", "--dry-run"]) == 0
    assert messages[-1][1]["success"] is True
    assert messages[-1][1]["plan"]["plan_hash"] == cli_payloads[-1]["plan_hash"]


@pytest.mark.parametrize("second_kind", ["compute", "repair"])
def test_compute_pipeline_excludes_other_computations_and_repairs(database, second_kind):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    from alphahome.common.db_session import owned_sync_session
    from alphahome.factors.locks import compute_session, repair_session

    first_started, release, second_started = threading.Event(), threading.Event(), threading.Event()
    def first():
        with owned_sync_session(database.connection_string) as db, compute_session(db):
            first_started.set()
            assert release.wait(5)
    def second():
        lock = compute_session if second_kind == "compute" else repair_session
        with owned_sync_session(database.connection_string) as db, lock(db):
            second_started.set()
    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(first)
        assert first_started.wait(5)
        b = pool.submit(second)
        try:
            assert not second_started.wait(0.15)
        finally:
            release.set()
        a.result(timeout=5)
        b.result(timeout=5)
    assert second_started.is_set()


@pytest.mark.parametrize("timing", ["after_plan", "during_compute"])
def test_concurrent_source_revision_is_not_lost_and_g_sees_committed_p(database, monkeypatch, timing):
    observed = []

    class P:
        def __init__(self, db_manager, config):
            self.db, self.config = db_manager, config

        def _get_trading_stock_codes(self, _):
            return ["A"]

        def calculate_p_factors_pit(self, calc_date, _):
            before = self.db.fetch_val_sync(f"SELECT value FROM {SOURCE}")
            if timing == "during_compute":
                update_source(database)
            after = self.db.fetch_val_sync(f"SELECT value FROM {SOURCE}")
            observed.extend([before, after])
            database.execute_sync("INSERT INTO factors.p_factor (ts_code,calc_date,value) VALUES ('A',%s,%s)", (calc_date, after))
            FactorGovernanceStore(database).record_date(self.config["factor_run_id"], "factor_p", date.fromisoformat(calc_date), "success", output_count=1, is_current=True)
            return {"status": "success", "success_count": 1}

    class G(P):
        def calculate_g_factors_pit(self, calc_date, _):
            value = self.db.fetch_val_sync("SELECT value FROM factors.p_factor WHERE calc_date=%s", (calc_date,))
            observed.append(value)
            database.execute_sync("INSERT INTO factors.g_factor (ts_code,calc_date,value) VALUES ('A',%s,%s)", (calc_date, value))
            FactorGovernanceStore(database).record_date(self.config["factor_run_id"], "factor_g", date.fromisoformat(calc_date), "success", output_count=1, is_current=True)
            return {"status": "success", "success_count": 1}

    coordinator = FactorCoordinator(database)
    contracts = coordinator.contracts()
    contracts = {"factor_p": replace(contracts["factor_p"], source_tables=(SOURCE,), readiness_dependencies=(), calculator_class=P),
                 "factor_g": replace(contracts["factor_g"], calculator_class=G)}
    monkeypatch.setattr(coordinator, "contracts", lambda: contracts)
    original_plan = coordinator.plan
    if timing == "after_plan":
        def plan(*args, **kwargs):
            result = original_plan(*args, **kwargs)
            update_source(database)
            return result
        monkeypatch.setattr(coordinator, "plan", plan)
    result = coordinator.run(["factor_g"], mode="full", batch_started_at=date(2026, 9, 14))
    assert result.status == "success"
    assert observed == ([1, 1, 1] if timing == "during_compute" else [2, 2, 2])
    stored = coordinator.governance.latest_source_watermarks("factor_p")["factor_p"]
    assert datetime.fromisoformat(stored[SOURCE]) == T1
    assert FactorRepository(database).dirty_start_date(contracts["factor_p"], stored) == date(2026, 9, 11)
    assert result.details["end_observed_watermarks"]["factor_p"][SOURCE] == T2


def test_noop_does_not_consume_revision_arriving_after_plan(database, monkeypatch):
    database.execute_sync("INSERT INTO factors.p_factor (ts_code,calc_date,value) VALUES ('A','2026-09-11',1)")
    governance = FactorGovernanceStore(database)
    previous = governance.start_run(["factor_p"], "full", date(2026, 9, 11))
    baseline = {SOURCE: T1, "_snapshot_xmin": FactorRepository(database).snapshot_xmin()}
    governance.finish_run(previous, "success", source_watermarks={"factor_p": baseline}, details={"watermark_contract": WATERMARK_CONTRACT})
    coordinator = FactorCoordinator(database)
    contract = replace(coordinator.contracts()["factor_p"], source_tables=(SOURCE,), readiness_dependencies=())
    monkeypatch.setattr(coordinator, "contracts", lambda: {"factor_p": contract})
    original_plan = coordinator.plan

    def plan(*args, **kwargs):
        result = original_plan(*args, **kwargs)
        assert result.total_dates == 0
        update_source(database)
        return result

    monkeypatch.setattr(coordinator, "plan", plan)
    result = coordinator.run(["factor_p"], start_date="2026-09-11", batch_started_at=date(2026, 9, 14))
    assert result.details["consumed_watermarks"] == {}
    latest = governance.latest_source_watermarks("factor_p")["factor_p"]
    assert datetime.fromisoformat(latest[SOURCE]) == T1
    assert FactorRepository(database).dirty_start_date(contract, latest) == date(2026, 9, 11)


def test_late_commit_with_old_updated_at_is_detected_by_next_batch(database, monkeypatch, isolated_database_url):
    observed = []

    class P:
        def __init__(self, db_manager, config):
            self.db, self.config = db_manager, config

        def _get_trading_stock_codes(self, _):
            return ["A"]

        def calculate_p_factors_pit(self, calc_date, _):
            value = self.db.fetch_val_sync(f"SELECT value FROM {SOURCE}")
            observed.append(value)
            database.execute_sync("INSERT INTO factors.p_factor(ts_code,calc_date,value) VALUES('A',%s,%s)", (calc_date, value))
            FactorGovernanceStore(database).record_date(self.config["factor_run_id"], "factor_p", date.fromisoformat(calc_date), "success", output_count=1, is_current=True)
            return {"status": "success", "success_count": 1}

    late_writer = psycopg2.connect(isolated_database_url)
    try:
        with late_writer.cursor() as cursor:
            cursor.execute(f"UPDATE {SOURCE} SET value=2, updated_at='2026-09-10 00:00:00+00'")
        coordinator = FactorCoordinator(database)
        contract = replace(coordinator.contracts()["factor_p"], source_tables=(SOURCE,), readiness_dependencies=(), calculator_class=P)
        monkeypatch.setattr(coordinator, "contracts", lambda: {"factor_p": contract})
        result = coordinator.run(["factor_p"], mode="full", batch_started_at=date(2026, 9, 14))
        assert result.status == "success"
        assert observed == [1]
        baseline = coordinator.governance.latest_source_watermarks("factor_p")["factor_p"]
        assert baseline["_snapshot_xmin"] > 0
        assert FactorRepository(database).dirty_start_date(contract, baseline) is None
        late_writer.commit()
        assert database.fetch_val_sync(f"SELECT count(*) FROM {SOURCE} WHERE updated_at > %s", (baseline[SOURCE],)) == 0
        assert FactorRepository(database).dirty_start_date(contract, baseline) == date(2026, 9, 11)
        plan = coordinator.plan(["factor_p"], batch_started_at=date(2026, 9, 14))
        assert plan.task_plans[0].dates == [date(2026, 9, 11)]
    finally:
        late_writer.close()


def test_snapshot_initialization_failure_closes_session_and_finishes_run(database, monkeypatch):
    import alphahome.factors.coordinator as module

    closed = []

    class BrokenSession:
        def __init__(self, *args, **kwargs):
            pass

        def _get_sync_connection(self):
            raise RuntimeError("snapshot unavailable")

        def close_sync(self):
            closed.append(True)

    coordinator = FactorCoordinator(database)
    contract = replace(coordinator.contracts()["factor_p"], source_tables=(SOURCE,), readiness_dependencies=())
    monkeypatch.setattr(coordinator, "contracts", lambda: {"factor_p": contract})
    monkeypatch.setattr(module, "DBManager", BrokenSession)
    result = coordinator.run(["factor_p"], mode="full", batch_started_at=date(2026, 9, 14))
    assert result.status == "error"
    assert closed == [True]
    assert database.fetch_val_sync("SELECT status FROM factors.factor_run WHERE run_id=%s", (result.run_id,)) == "error"
    assert coordinator.governance.latest_source_watermarks("factor_p") == {}


def test_cancellation_finishes_run_ledger_before_propagating(database, monkeypatch):
    coordinator = FactorCoordinator(database)
    contract = replace(coordinator.contracts()["factor_p"], source_tables=(SOURCE,), readiness_dependencies=())
    monkeypatch.setattr(coordinator, "contracts", lambda: {"factor_p": contract})

    def cancel(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(coordinator, "_run_task_dates", cancel)
    with pytest.raises(asyncio.CancelledError):
        coordinator.run(["factor_p"], mode="full", batch_started_at=date(2026, 9, 14))
    assert database.fetch_val_sync("SELECT status FROM factors.factor_run") == "cancelled"
