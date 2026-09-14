from datetime import date

import pandas as pd
import pytest

from alphahome.factors.persistence import FactorSnapshotWriter, P_FACTOR_COLUMNS, factor_frame_checksum


def _frame():
    row = {column: 1.0 for column in P_FACTOR_COLUMNS}
    row.update(
        {
            "ts_code": "000001.SZ",
            "calc_date": "2026-09-11",
            "ann_date": "2026-08-31",
            "end_date": "2026-06-30",
            "data_source": "report",
            "p_rank": 1,
            "data_quality": "high",
            "calculation_status": "success",
        }
    )
    return pd.DataFrame([row])


class _Cursor:
    def __init__(self, fail_copy=False):
        self.fail_copy = fail_copy
        self.statements = []
        self.fetchone_values = iter([(1, 1), (0,)])
        self.rowcount = 3

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params=None):
        self.statements.append((statement, params))

    def copy_expert(self, statement, _buffer):
        self.statements.append((statement, None))
        if self.fail_copy:
            raise RuntimeError("copy failed")

    def fetchone(self):
        return next(self.fetchone_values)


class _Connection:
    def __init__(self, fail_copy=False):
        self.cursor_instance = _Cursor(fail_copy)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class _DB:
    def __init__(self, fail_copy=False):
        self.connection = _Connection(fail_copy)

    def _get_sync_connection(self):
        return self.connection


def test_snapshot_write_uses_lock_staging_and_one_transaction():
    db = _DB()
    count, checksum, validation = FactorSnapshotWriter(db).write(
        _frame(), "p", date(2026, 9, 11)
    )
    sql = "\n".join(item[0] for item in db.connection.cursor_instance.statements)
    assert count == 1
    assert len(checksum) == 64
    assert validation.coverage_rate is None
    assert "pg_advisory_xact_lock" in sql
    assert "CREATE TEMP TABLE" in sql
    assert "COPY factor_stage_" in sql
    assert "DELETE FROM factors.p_factor" in sql
    assert "INSERT INTO factors.p_factor" in sql
    assert db.connection.committed is True
    assert db.connection.rolled_back is False


def test_snapshot_copy_failure_rolls_back_without_commit():
    db = _DB(fail_copy=True)
    with pytest.raises(RuntimeError, match="copy failed"):
        FactorSnapshotWriter(db).write(_frame(), "p", "2026-09-11")
    assert db.connection.committed is False
    assert db.connection.rolled_back is True


def test_snapshot_write_rejects_non_friday_before_opening_transaction():
    db = _DB()
    with pytest.raises(ValueError, match="自然周五"):
        FactorSnapshotWriter(db).write(_frame(), "p", "2026-09-10")
    assert db.connection.committed is False


def test_expected_no_data_clear_is_locked_and_recorded_atomically():
    db = _DB()
    deleted = FactorSnapshotWriter(db).clear_expected_no_data(
        "p",
        "2026-09-11",
        run_id="00000000-0000-0000-0000-000000000001",
        task_name="factor_p",
    )

    sql = "\n".join(item[0] for item in db.connection.cursor_instance.statements)
    assert deleted == 3
    assert "pg_advisory_xact_lock" in sql
    assert "DELETE FROM factors.p_factor" in sql
    assert "INSERT INTO factors.factor_run_date" in sql
    assert db.connection.committed is True
    assert db.connection.rolled_back is False


def test_checksum_is_stable_across_integer_and_database_numeric_representations():
    from decimal import Decimal

    integer, decimal = _frame(), _frame()
    integer["p_score"] = [50]
    decimal["p_score"] = [Decimal("50.000000")]
    assert factor_frame_checksum(integer, P_FACTOR_COLUMNS) == factor_frame_checksum(decimal, P_FACTOR_COLUMNS)
