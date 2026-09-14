import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from alphahome.factors import command
from alphahome.factors.coordinator import FactorCoordinator
from alphahome.factors.date_policy import FactorDatePolicy
from alphahome.factors.governance import FactorGovernanceStore, SCHEMA_COLUMNS
from alphahome.gui.services import factor_service


class ReadOnlyDB:
    """Reject every write and exercise the real planner's SELECT paths."""

    def __init__(self, missing_table=None, missing_column=None):
        self.queries = []
        self.closed = False
        self.catalog = [
            {"table_schema": table.split(".")[0],
             "table_name": table.split(".")[1], "column_name": column}
            for table, columns in SCHEMA_COLUMNS.items()
            for column in columns
            if table != missing_table and (table, column) != missing_column
        ]

    def _read(self, sql):
        assert sql.strip().upper().startswith(("SELECT", "WITH"))
        self.queries.append(sql)

    def _get_sync_connection(self):
        raise AssertionError("Write connection requested by a read-only preview")

    def execute_sync(self, *_args, **_kwargs):
        raise AssertionError("Write attempted by a read-only preview")

    def fetch_sync(self, sql, _params=()):
        self._read(sql)
        return self.catalog if "pg_catalog.pg_attribute" in sql else []

    def fetch_val_sync(self, sql, _params=()):
        self._read(sql)
        if "pg_snapshot_xmin" in sql:
            return "100"
        if "to_regclass" in sql:
            return True
        if "COUNT(*)" in sql:
            return 1
        return None

    def fetch_one_sync(self, sql, _params=()):
        self._read(sql)
        if "eligible_pit_input_gaps" in sql:
            return {"eligible_count": 1, "eligible_missing": 0}
        return {}

    def close_sync(self):
        self.closed = True


@pytest.mark.parametrize("tasks, kwargs", [
    (["factor_p"], {"mode": "bad"}),
    (["factor_p"], {"mode": "manual"}),
    (["factor_p"], {"date_range": ["2026-09-04"]}),
    (["factor_p"], {"date_range": ["2026-09-04", "2026-09-11"], "end_date": "2026-09-11"}),
    (["factor_p"], {"source_cutoff": datetime(2026, 9, 14)}),
    (["factor_p"], {"end_date": "2026-09-18"}),
    (["factor_p"], {"start_date": "2026-09-11", "end_date": "2026-09-04"}),
    (["factor_p"], {"start_date": "not-a-date"}),
    (["unknown"], {}),
    (["unknown"], {"expand_dependencies": False}),
    ([], {}),
])
def test_invalid_plan_arguments_have_zero_database_io(tasks, kwargs):
    db = ReadOnlyDB()
    with pytest.raises(ValueError):
        FactorCoordinator(db).plan(tasks, batch_started_at=date(2026, 9, 14), **kwargs)
    assert db.queries == []


@pytest.mark.parametrize("missing, expected", [
    ({"missing_table": "factors.factor_run"}, "missing_relation:factors.factor_run"),
    ({"missing_column": ("factors.factor_run", "source_watermarks")},
     "missing_column:factors.factor_run.source_watermarks"),
])
@pytest.mark.parametrize("method", ["plan", "run"])
def test_missing_governance_returns_migration_required_without_writes(missing, expected, method):
    db = ReadOnlyDB(**missing)
    result = getattr(FactorCoordinator(db), method)(
        ["factor_p"], batch_started_at=date(2026, 9, 14)
    )
    assert result.status == "migration_required"
    assert expected in result.message
    assert len(db.queries) == 1
    if method == "run":
        assert result.run_id is None


def test_existing_governance_plan_uses_only_reads_and_keeps_friday_contract():
    db = ReadOnlyDB()
    result = FactorCoordinator(db).plan(
        ["factor_g"], mode="manual", start_date="2026-09-04",
        batch_started_at=date(2026, 9, 14),
    )
    assert result.status == "ready"
    assert result.task_names == ["factor_p", "factor_g"]
    assert result.total_dates == 4
    assert len(db.queries) > 1


@pytest.fixture
def fixed_cutoff(monkeypatch):
    monkeypatch.setattr(FactorDatePolicy, "automatic_cutoff", lambda *_a, **_k: date(2026, 9, 11))


@pytest.mark.parametrize("missing, expected_status, expected_exit", [
    (None, "ready", 0), ("factors.factor_run", "migration_required", 2),
])
def test_cli_dry_run_is_read_only(monkeypatch, capsys, fixed_cutoff, missing, expected_status, expected_exit):
    db = ReadOnlyDB(missing_table=missing)
    monkeypatch.setattr(command, "ConfigManager", lambda: SimpleNamespace(get_database_url=lambda: "test-dsn"))
    monkeypatch.setattr(command, "DBManager", lambda *_a, **_k: db)
    exit_code = command.main([
        "run", "--tasks", "p", "--mode", "manual", "--start-date", "2026-09-04", "--dry-run"
    ])
    assert exit_code == expected_exit
    assert json.loads(capsys.readouterr().out)["status"] == expected_status
    assert db.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("missing, expected_status", [(None, "ready"), ("factors.factor_run", "migration_required")])
async def test_gui_preflight_is_read_only(monkeypatch, fixed_cutoff, missing, expected_status):
    db = ReadOnlyDB(missing_table=missing)
    monkeypatch.setattr(factor_service.UnifiedTaskFactory, "get_db_manager", lambda: SimpleNamespace(connection_string="test-dsn"))
    monkeypatch.setattr(factor_service, "DBManager", lambda *_a, **_k: db)
    messages = []
    monkeypatch.setattr(factor_service, "_send_response_callback", lambda *args: messages.append(args))
    await factor_service.handle_preflight(["factor_p"], "manual", "2026-09-04", "2026-09-11")
    assert messages[-1][0] == "FACTOR_PREFLIGHT_COMPLETE"
    assert messages[-1][1]["success"] is True
    assert messages[-1][1]["plan"]["status"] == expected_status
    assert db.closed


@pytest.mark.parametrize("apply", [False, True])
def test_schema_command_requires_explicit_apply_for_ddl(monkeypatch, capsys, apply):
    db = ReadOnlyDB()
    ddl = Mock()
    monkeypatch.setattr(command, "ConfigManager", lambda: SimpleNamespace(get_database_url=lambda: "test-dsn"))
    monkeypatch.setattr(command, "DBManager", lambda *_a, **_k: db)
    monkeypatch.setattr(FactorGovernanceStore, "ensure_schema", ddl)
    assert command.main(["schema"] + (["--apply"] if apply else [])) == 0
    assert ddl.call_count == int(apply)
    assert json.loads(capsys.readouterr().out)["applied"] is apply
    assert db.closed


def test_start_run_never_performs_schema_migration():
    connection = MagicMock()
    db = SimpleNamespace(_get_sync_connection=lambda: connection)
    store = FactorGovernanceStore(db)
    store.ensure_schema = Mock(side_effect=AssertionError("implicit migration"))
    store.start_run(["factor_p"], "manual", date(2026, 9, 11))
    store.ensure_schema.assert_not_called()
    executed = connection.cursor.return_value.__enter__.return_value.execute.call_args_list
    assert len(executed) == 1
    assert executed[0].args[0].strip().startswith("INSERT INTO factors.factor_run")
