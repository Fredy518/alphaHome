"""Opt-in PostgreSQL integration tests for governed factor persistence.

Set ``ALPHAHOME_FACTOR_TEST_DATABASE_URL`` to a disposable database whose name
contains ``test``. The test refuses to touch a database with existing ``factors``
or ``pgs_factors`` schemas.
"""

from __future__ import annotations

import os
from datetime import date
from urllib.parse import urlparse

import pandas as pd
import psycopg2
import pytest

from alphahome.factors.governance import FactorGovernanceStore
from alphahome.factors.persistence import FactorSnapshotWriter, P_FACTOR_COLUMNS
from alphahome.factors.repair import FactorRepairService
from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


class _DB:
    def __init__(self, connection):
        self.connection = connection

    def _get_sync_connection(self):
        return self.connection

    def execute_sync(self, query, params=None):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def fetch_one_sync(self, query, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [item.name for item in cursor.description]
            return dict(zip(columns, row))


def _p_frame(data_source="report", score=50.0):
    row = {column: 1.0 for column in P_FACTOR_COLUMNS}
    row.update(
        {
            "ts_code": "000001.SZ",
            "calc_date": "2026-09-11",
            "ann_date": "2026-08-31",
            "end_date": "2026-06-30",
            "data_source": data_source,
            "p_score": score,
            "p_rank": 1,
            "data_quality": "high",
            "calculation_status": "success",
        }
    )
    return pd.DataFrame([row])


def test_live_factor_relations_obey_schema_ownership_contract():
    database_url = ConfigManager().get_database_url()
    if not database_url:
        pytest.skip("AlphaDB is not configured")
    db = DBManager(database_url, mode="sync")
    try:
        relations = db.fetch_sync(
            """
            SELECT n.nspname AS schema_name, c.relname, c.relkind,
                   CASE WHEN c.relkind = 'v' THEN pg_get_viewdef(c.oid, TRUE) END AS viewdef
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (n.nspname, c.relname) IN (
                ('factors', 'p_factor'), ('factors', 'g_factor'),
                ('pgs_factors', 'p_factor'), ('pgs_factors', 'g_factor')
            )
            ORDER BY n.nspname, c.relname
            """
        )
        keyed = {(row["schema_name"], row["relname"]): row for row in relations}
        assert keyed[("factors", "p_factor")]["relkind"] in {"r", "p"}
        assert keyed[("factors", "g_factor")]["relkind"] in {"r", "p"}
        assert keyed[("pgs_factors", "p_factor")]["relkind"] == "v"
        assert keyed[("pgs_factors", "g_factor")]["relkind"] == "v"
        assert "factors.p_factor" in keyed[("pgs_factors", "p_factor")]["viewdef"]
        assert "factors.g_factor" in keyed[("pgs_factors", "g_factor")]["viewdef"]
        assert (
            db.fetch_one_sync(
                """
            SELECT (SELECT COUNT(*) FROM pgs_factors.p_factor) =
                       (SELECT COUNT(*) FROM factors.p_factor) AS p_equal,
                   (SELECT COUNT(*) FROM pgs_factors.g_factor) =
                       (SELECT COUNT(*) FROM factors.g_factor) AS g_equal
            """
            )
            == {"p_equal": True, "g_equal": True}
        )
    finally:
        db.close_sync()


def test_postgresql_staging_governance_views_and_friday_constraint():
    database_url = os.environ.get("ALPHAHOME_FACTOR_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("ALPHAHOME_FACTOR_TEST_DATABASE_URL is not configured")
    database_name = urlparse(database_url).path.lstrip("/").lower()
    if "test" not in database_name:
        pytest.skip("factor integration database name must contain 'test'")

    connection = psycopg2.connect(database_url)
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT to_regnamespace('factors'), to_regnamespace('pgs_factors'), "
            "to_regclass('public.task_status')"
        )
        factors_schema, legacy_schema, original_task_status = cursor.fetchone()
    if factors_schema is not None or legacy_schema is not None:
        connection.close()
        pytest.skip("disposable database already has factor schemas")
    connection.autocommit = False
    db = _DB(connection)
    try:
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA factors")
            cursor.execute("CREATE SCHEMA pgs_factors")
            cursor.execute(
                """
                CREATE TABLE factors.p_factor (
                    ts_code text NOT NULL, calc_date date NOT NULL,
                    ann_date date NOT NULL, end_date date, data_source varchar(6),
                    p_score numeric, p_rank integer, gpa numeric,
                    roe_excl numeric, roa_excl numeric, net_margin_ttm numeric,
                    operating_margin_ttm numeric, roi_ttm numeric,
                    asset_turnover_ttm numeric, equity_multiplier numeric,
                    debt_to_asset_ratio numeric, equity_ratio numeric,
                    revenue_yoy_growth numeric, n_income_yoy_growth numeric,
                    operate_profit_yoy_growth numeric, data_quality text,
                    calculation_status text, created_at timestamp DEFAULT now(),
                    updated_at timestamp DEFAULT now(),
                    PRIMARY KEY (ts_code, calc_date)
                );
                CREATE TABLE factors.g_factor (
                    ts_code text NOT NULL, calc_date date NOT NULL,
                    ann_date date NOT NULL, data_source text NOT NULL,
                    g_efficiency_surprise numeric, g_efficiency_momentum numeric,
                    g_revenue_momentum numeric, g_profit_momentum numeric,
                    rank_es numeric, rank_em numeric, rank_rm numeric,
                    rank_pm numeric, g_score numeric,
                    data_timeliness_weight numeric, calculation_status text,
                    created_at timestamp DEFAULT now(), updated_at timestamp DEFAULT now(),
                    PRIMARY KEY (ts_code, calc_date)
                );
                CREATE VIEW pgs_factors.p_factor AS SELECT * FROM factors.p_factor;
                CREATE VIEW pgs_factors.g_factor AS SELECT * FROM factors.g_factor;
                """
            )
        connection.commit()

        governance = FactorGovernanceStore(db)
        governance.ensure_schema()
        run_id = governance.start_run(
            ["factor_p"],
            "manual",
            date(2026, 9, 11),
            formula_versions={"factor_p": "v2.0"},
        )
        writer = FactorSnapshotWriter(db)
        writer.write(
            _p_frame(score=50.0),
            "p",
            "2026-09-11",
            run_id=run_id,
            task_name="factor_p",
            expected_codes=["000001.SZ"],
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT p_score FROM pgs_factors.p_factor")
            assert float(cursor.fetchone()[0]) == 50.0
            cursor.execute(
                "SELECT status, output_count, is_current "
                "FROM factors.factor_run_date WHERE run_id = %s",
                (str(run_id),),
            )
            assert cursor.fetchone() == ("success", 1, True)

        other = psycopg2.connect(database_url)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                    ("factor_p", "2026-09-11"),
                )
            with other.cursor() as cursor:
                cursor.execute("SET lock_timeout = '100ms'")
                with pytest.raises(psycopg2.errors.LockNotAvailable):
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                        ("factor_p", "2026-09-11"),
                    )
            other.rollback()
            connection.rollback()
        finally:
            other.close()

        with pytest.raises(psycopg2.errors.StringDataRightTruncation):
            writer.write(
                _p_frame(data_source="forecast", score=99.0), "p", "2026-09-11"
            )
        with connection.cursor() as cursor:
            cursor.execute("SELECT p_score FROM factors.p_factor")
            assert float(cursor.fetchone()[0]) == 50.0

        FactorRepairService(db)._install_weekday_constraints(validate=True)
        with pytest.raises(psycopg2.errors.CheckViolation):
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO factors.p_factor (
                        ts_code, calc_date, ann_date, data_source,
                        p_score, p_rank, data_quality, calculation_status
                    ) VALUES ('BAD', '2026-09-10', '2026-09-01', 'report',
                              1, 1, 'high', 'success')
                    """
                )
            connection.commit()
        connection.rollback()
    finally:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS pgs_factors CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS factors CASCADE")
            if original_task_status is None:
                cursor.execute("DROP TABLE IF EXISTS public.task_status")
        connection.close()
