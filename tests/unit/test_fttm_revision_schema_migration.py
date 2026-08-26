from __future__ import annotations

from pathlib import Path

from alphahome.pit.base.monthly_snapshot_manager import PITMonthlySnapshotManager


class _RecordingDBManager:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute_sync(self, statement: str) -> None:
        self.statements.append(statement)


class _Context:
    def __init__(self) -> None:
        self.db_manager = _RecordingDBManager()


class _SnapshotManager(PITMonthlySnapshotManager):
    def incremental_update(self, **kwargs):
        raise NotImplementedError

    def full_backfill(self, **kwargs):
        raise NotImplementedError


def test_idempotent_table_ddl_runs_alter_statements_for_existing_tables():
    manager = _SnapshotManager("pit_index_fttm_monthly")
    manager.context = _Context()

    manager._apply_idempotent_table_ddl()

    assert len(manager.context.db_manager.statements) == 1
    ddl = manager.context.db_manager.statements[0]
    assert "ADD COLUMN IF NOT EXISTS revision_rate" in ddl
    assert "pit_index_fttm_monthly_revision_weight_ck" in ddl


def test_revision_ddls_are_idempotent_migrations_with_quality_constraints():
    database_dir = (
        Path(__file__).resolve().parents[2] / "alphahome" / "pit" / "database"
    )

    for table in ("pit_industry_fttm_monthly", "pit_index_fttm_monthly"):
        ddl = (database_dir / f"create_{table}_table.sql").read_text(encoding="utf-8")
        assert "ADD COLUMN IF NOT EXISTS revision_rate" in ddl
        assert "ADD COLUMN IF NOT EXISTS horizon_roll_rate" in ddl
        assert "DO $$" in ddl
        assert f"{table}_revision_weight_ck" in ddl
        assert f"{table}_revision_activity_ck" in ddl
        assert f"{table}_revision_up_stock_ck" in ddl
        assert f"{table}_revision_up_weight_ck" in ddl
