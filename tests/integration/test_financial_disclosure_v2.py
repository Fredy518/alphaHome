"""Execute production SQL against a guarded, explicit isolated test database."""

from datetime import date
import logging
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

from alphahome.pit.disclosure import DISCLOSURE_COLUMNS, FINANCIAL_PIT_CONTRACT
from alphahome.pit.pit_income_quarterly_manager import PITIncomeQuarterlyManager
from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager
from alphahome.pit.pit_cashflow_quarterly_manager import PITCashflowQuarterlyManager
from alphahome.pit.pit_financial_indicators_manager import PITFinancialIndicatorsManager
from alphahome.pit.calculators.financial_indicators_calculator import (
    FinancialIndicatorsCalculator,
)
from alphahome.features.recipes.mv.stock.stock_income_quarterly import (
    StockIncomeQuarterlyMV,
)
from alphahome.features.recipes.mv.stock.stock_balance_quarterly import (
    StockBalanceQuarterlyMV,
)
from alphahome.features.recipes.mv.stock.stock_cashflow_quarterly import (
    StockCashflowQuarterlyMV,
)
from alphahome.features.storage.definition_drift import (
    definition_drift,
    definition_seal,
    seal_comment_sql,
)
from alphahome.factors.core.data_repository import (
    PFactorDataRepository,
    GFactorDataRepository,
    PHistoryUnavailable,
)
from alphahome.factors.persistence import (
    P_FACTOR_COLUMNS,
    G_FACTOR_COLUMNS,
    FactorSnapshotWriter,
    factor_frame_checksum,
)

pytestmark = [pytest.mark.integration, pytest.mark.requires_db]

MIGRATION = (
    Path(__file__).parents[2]
    / "alphahome/pit/database/migrations/20261001_financial_disclosure_v2.sql"
)


class SQLContext:
    def __init__(self, connection):
        self.connection = connection
        self.db_manager = self

    def fetch_sync(self, sql, params=None):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    def query_dataframe(self, sql, params=None):
        return pd.DataFrame(self.fetch_sync(sql, params))

    def execute_sync(self, sql, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount

    def _get_sync_connection(self):
        return self.connection

    def insert(self, table, **values):
        # Identifiers are fixed in the fixture/tests, values are always bound.
        self.execute_sync(
            f"INSERT INTO {table} ({','.join(values)}) VALUES ({','.join(['%s'] * len(values))})",
            tuple(values.values()),
        )


@pytest.fixture
def financial_db(isolated_database_url):
    connection = psycopg2.connect(isolated_database_url)
    connection.autocommit = True
    ctx = SQLContext(connection)
    assert ctx.fetch_sync("SELECT current_database() AS name")[0]["name"].startswith(
        "alphahome_test_"
    )
    created = []
    created_schemas = []
    for schema in ("tushare", "rawdata", "pit", "factors", "features"):
        if ctx.fetch_sync("SELECT to_regnamespace(%s) AS name", (schema,))[0]["name"] is None:
            created_schemas.append(schema)
        ctx.execute_sync(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def table(name, columns, key=None):
        assert (
            ctx.fetch_sync("SELECT to_regclass(%s) AS relation", (name,))[0]["relation"]
            is None
        ), "Fixture refuses to overwrite an existing table"
        definition = ", ".join(f"{column} {kind}" for column, kind in columns.items())
        if key:
            definition += ", UNIQUE (" + ",".join(key) + ")"
        ctx.execute_sync(f"CREATE TABLE {name} ({definition})")
        created.append(("TABLE", name))

    base = dict(
        ts_code="text",
        end_date="date",
        ann_date="date",
        f_ann_date="date",
        report_type="integer",
        update_time="timestamp",
    )
    income_fields = (
        "revenue",
        "oper_cost",
        "n_income_attr_p",
        "operate_profit",
        "total_profit",
        "n_income",
        "basic_eps",
        "diluted_eps",
    )
    balance_fields = (
        "total_assets",
        "total_liab",
        "total_hldr_eqy_exc_min_int",
        "total_hldr_eqy_inc_min_int",
        "total_cur_assets",
        "total_cur_liab",
        "inventories",
        "minority_int",
    )
    cash_manager = PITCashflowQuarterlyManager()
    for source, fields in (
        ("fina_income", income_fields),
        ("fina_balancesheet", balance_fields),
        ("fina_cashflow", cash_manager.data_fields),
    ):
        table("tushare." + source, base | {field: "numeric" for field in fields})
    table(
        "tushare.fina_express",
        {k: v for k, v in base.items() if k not in ("f_ann_date", "report_type")}
        | {f: "numeric" for f in set(income_fields + balance_fields)},
    )
    table(
        "tushare.fina_forecast",
        dict(
            ts_code="text",
            end_date="date",
            ann_date="date",
            update_time="timestamp",
            net_profit_min="numeric",
            net_profit_max="numeric",
        ),
    )
    table(
        "tushare.stock_basic",
        dict(ts_code="text", list_date="date", delist_date="date"),
    )
    for source in (
        "fina_income",
        "fina_balancesheet",
        "fina_cashflow",
        "fina_express",
        "fina_forecast",
    ):
        ctx.execute_sync(
            f"CREATE VIEW rawdata.{source} AS SELECT * FROM tushare.{source}"
        )
        created.append(("VIEW", "rawdata." + source))

    key = ("ts_code", "end_date", "ann_date", "data_source")
    pit_base = dict(
        ts_code="text",
        end_date="date",
        ann_date="date",
        data_source="text",
        updated_at="timestamp DEFAULT CURRENT_TIMESTAMP",
    )
    table(
        "pit.pit_income_quarterly",
        pit_base
        | {f: "numeric" for f in income_fields if f not in ("basic_eps", "diluted_eps")}
        | dict(
            conversion_status="text",
            n_income_attr_p_ytd="numeric",
            basic_eps_ytd="numeric",
            diluted_eps_ytd="numeric",
            report_source_update_time="timestamp",
            report_source_row_count="integer",
            report_source_value_conflict="boolean",
            report_source_selection_basis="text",
        ),
        key,
    )
    table(
        "pit.pit_balance_quarterly",
        pit_base | dict(tot_assets="numeric", tot_liab="numeric", tot_equity="numeric"),
        key,
    )
    table(
        "pit.pit_cashflow_quarterly",
        pit_base | {f: "numeric" for f in cash_manager.data_fields},
        key,
    )
    indicators = (
        "gpa_ttm",
        "roe_excl_ttm",
        "roa_excl_ttm",
        "net_margin_ttm",
        "operating_margin_ttm",
        "roi_ttm",
        "asset_turnover_ttm",
        "equity_multiplier",
        "debt_to_asset_ratio",
        "equity_ratio",
        "revenue_yoy_growth",
        "n_income_yoy_growth",
        "operate_profit_yoy_growth",
    )
    table(
        "pit.pit_financial_indicators",
        pit_base
        | {f: "numeric" for f in indicators}
        | dict(
            data_quality="text",
            calculation_status="text",
            data_completeness="text",
            balance_sheet_lag="integer",
        ),
        key,
    )
    provenance = {"source_available_date", "availability_basis", "pit_contract_version"}
    table(
        "factors.factor_run_date",
        dict(task_name="text", calc_date="date", status="text", is_current="boolean"),
        ("task_name", "calc_date"),
    )
    for name, fields in (
        ("p_factor", P_FACTOR_COLUMNS),
        ("g_factor", G_FACTOR_COLUMNS),
    ):
        types = {
            field: (
                "date"
                if field in ("calc_date", "ann_date", "end_date")
                else (
                    "text"
                    if field
                    in ("ts_code", "data_source", "data_quality", "calculation_status")
                    else "integer" if field == "p_rank" else "numeric"
                )
            )
            for field in fields
            if field not in provenance
        }
        table("factors." + name, types, ("ts_code", "calc_date"))
    ctx.execute_sync(MIGRATION.read_text(encoding="utf-8"))
    try:
        yield ctx, created
    finally:
        for kind, name in reversed(created):
            ctx.execute_sync(f"DROP {kind} IF EXISTS {name}")
        for schema in reversed(created_schemas):
            ctx.execute_sync(f"DROP SCHEMA {schema}")
        connection.close()


def manager(cls, ctx):
    obj = cls()
    obj.context = ctx
    obj.logger = logging.getLogger("isolated-financial-test")
    return obj


def put_report(ctx, table, actual, update, **metrics):
    ctx.insert(
        "tushare." + table,
        ts_code="000928.SZ",
        end_date="2026-03-31",
        ann_date="2026-04-28",
        f_ann_date=actual,
        update_time=update,
        report_type=1,
        **metrics,
    )


@pytest.mark.parametrize("cls", [PITIncomeQuarterlyManager, PITBalanceQuarterlyManager])
def test_mixed_report_express_and_forecast_keep_their_public_events(financial_db, cls):
    ctx, _ = financial_db
    put_report(
        ctx,
        "fina_income",
        "2026-04-28",
        "2026-04-28",
        revenue=100.0,
        oper_cost=60.0,
        n_income_attr_p=20.0,
        operate_profit=30.0,
    )
    put_report(
        ctx,
        "fina_balancesheet",
        "2026-04-28",
        "2026-04-28",
        total_assets=200.0,
        total_liab=80.0,
        total_hldr_eqy_exc_min_int=120.0,
    )
    ctx.insert(
        "tushare.fina_express",
        ts_code="000928.SZ",
        end_date="2026-03-31",
        ann_date="2026-04-15",
        update_time="2026-04-16",
        revenue=90.0,
        n_income_attr_p=18.0,
        n_income=18.0,
        total_assets=180.0,
        total_hldr_eqy_exc_min_int=110.0,
    )
    ctx.insert(
        "tushare.fina_forecast",
        ts_code="000928.SZ",
        end_date="2026-03-31",
        ann_date="2026-04-05",
        update_time="2026-04-06",
        net_profit_min=10.0,
        net_profit_max=20.0,
    )
    obj = manager(cls, ctx)
    data = obj._preprocess_data(obj._fetch_tushare_data("2026-04-01", "2026-05-01"))
    expected = (
        {"report", "express", "forecast"}
        if cls is PITIncomeQuarterlyManager
        else {"report", "express"}
    )
    assert set(data.data_source) == expected
    assert obj._batch_upsert_to_pit(data, 100)["errors"] == 0
    rows = ctx.fetch_sync(f"SELECT * FROM pit.{obj.table_name} ORDER BY ann_date")
    assert {row["data_source"] for row in rows} == expected
    assert all(row["source_ann_date"] == row["ann_date"] for row in rows)
    assert all(row["source_update_time"] is not None for row in rows)


@pytest.mark.parametrize("source", ["express_income", "express_balance", "forecast"])
def test_newer_same_event_update_wins_and_stale_replay_is_not_counted(
    financial_db, source
):
    ctx, _ = financial_db
    is_forecast = source == "forecast"
    obj = manager(
        (
            PITBalanceQuarterlyManager
            if source == "express_balance"
            else PITIncomeQuarterlyManager
        ),
        ctx,
    )
    table = "tushare.fina_forecast" if is_forecast else "tushare.fina_express"
    values = (
        dict(net_profit_min=10.0, net_profit_max=20.0)
        if is_forecast
        else dict(
            revenue=100.0,
            n_income_attr_p=20.0,
            n_income=20.0,
            total_assets=180.0,
            total_hldr_eqy_exc_min_int=110.0,
        )
    )
    ctx.insert(
        table,
        ts_code="000928.SZ",
        end_date="2026-03-31",
        ann_date="2026-04-15",
        update_time="2026-04-16",
        **values,
    )
    fetch = lambda: obj._preprocess_data(
        obj._fetch_tushare_data("2026-04-01", "2026-05-01")
    )
    old = fetch()
    assert obj._batch_upsert_to_pit(old, 100)["inserted"] == 1
    assignment = (
        "net_profit_max=22.0" if is_forecast else "revenue=101.0,total_assets=181.0"
    )
    ctx.execute_sync(f"UPDATE {table} SET {assignment},update_time='2026-04-20'")
    new = fetch()
    assert new.iloc[0].source_update_time == pd.Timestamp("2026-04-20")
    assert obj._batch_upsert_to_pit(new, 100)["updated"] == 1
    metric = (
        "tot_assets"
        if source == "express_balance"
        else "n_income_attr_p" if is_forecast else "revenue"
    )
    expected = (
        181.0 if source == "express_balance" else 160000.0 if is_forecast else 101.0
    )
    row = ctx.fetch_sync(
        f"SELECT {metric},source_update_time FROM pit.{obj.table_name}"
    )[0]
    assert float(row[metric]) == expected
    assert row["source_update_time"] == pd.Timestamp("2026-04-20")
    stale = obj._batch_upsert_to_pit(old, 100)
    assert stale == {"inserted": 0, "updated": 0, "errors": 0}
    assert (
        float(ctx.fetch_sync(f"SELECT {metric} FROM pit.{obj.table_name}")[0][metric])
        == expected
    )


def test_actual_leak_is_fixed_through_executed_source_sql_and_pit_upsert(financial_db):
    ctx, _ = financial_db
    for actual, update, profit in [
        ("2026-04-28", "2026-06-30", 199100742.14),
        ("2026-05-09", "2026-05-19", 184563912.14),
    ]:
        put_report(
            ctx,
            "fina_income",
            actual,
            update,
            n_income_attr_p=profit,
            revenue=2879400012.36,
            operate_profit=278207579.81,
            oper_cost=2000000000.0,
            basic_eps=0.2,
            diluted_eps=0.2,
        )
    income = manager(PITIncomeQuarterlyManager, ctx)
    original = income._fetch_income_report("2026-04-28", "2026-05-08")
    assert (
        len(original) == 1 and float(original.iloc[0].n_income_attr_p) == 199100742.14
    )
    all_events = income._preprocess_data(
        income._fetch_income_report("2026-04-28", "2026-05-10")
    )
    hashes = ctx.fetch_sync(
        "SELECT GREATEST(ann_date,COALESCE(f_ann_date,ann_date)) AS public_date, "
        "md5(row_to_json(source)::text) AS fingerprint FROM tushare.fina_income source"
    )
    assert dict(zip(all_events.ann_date, all_events.source_version_hash)) == {
        row["public_date"]: row["fingerprint"] for row in hashes
    }
    result = income._batch_upsert_to_pit(all_events.iloc[::-1], 100)
    assert result["errors"] == 0 and result["inserted"] == 2
    rows = ctx.fetch_sync(
        "SELECT ann_date, source_ann_date, n_income_attr_p FROM pit.pit_income_quarterly ORDER BY ann_date"
    )
    assert [row["ann_date"] for row in rows] == [date(2026, 4, 28), date(2026, 5, 9)]
    assert float(rows[0]["n_income_attr_p"]) == 199100742.14
    assert float(rows[1]["n_income_attr_p"]) == 184563912.14
    assert len(income._fetch_income_report("2026-05-09", "2026-05-09")) == 1


def test_same_event_upserts_do_not_depend_on_batch_arrival_order(financial_db):
    ctx, _ = financial_db
    for reverse in (False, True):
        ctx.execute_sync(
            "TRUNCATE tushare.fina_balancesheet, pit.pit_balance_quarterly"
        )
        balance = manager(PITBalanceQuarterlyManager, ctx)
        rows = [("2026-04-28", 200.0), ("2026-04-29", 220.0)]
        for update, assets in reversed(rows) if reverse else rows:
            put_report(
                ctx,
                "fina_balancesheet",
                "2026-04-28",
                update,
                total_assets=assets,
                total_liab=100.0,
                total_hldr_eqy_exc_min_int=100.0,
            )
            raw = balance._fetch_tushare_data("2026-04-28", "2026-04-28")
            processed = balance._preprocess_data(raw)
            assert balance._batch_upsert_to_pit(processed, 100)["errors"] == 0
        assert (
            float(
                ctx.fetch_sync("SELECT tot_assets FROM pit.pit_balance_quarterly")[0][
                    "tot_assets"
                ]
            )
            == 220.0
        )


def test_balance_only_revision_recalculates_without_overwriting_prior_indicator(
    financial_db,
):
    ctx, _ = financial_db
    put_report(
        ctx,
        "fina_income",
        "2026-04-28",
        "2026-04-28",
        revenue=100.0,
        oper_cost=50.0,
        n_income_attr_p=20.0,
        operate_profit=25.0,
    )
    put_report(
        ctx,
        "fina_balancesheet",
        "2026-04-28",
        "2026-04-28",
        total_assets=200.0,
        total_liab=80.0,
        total_hldr_eqy_exc_min_int=100.0,
    )
    put_report(
        ctx,
        "fina_balancesheet",
        "2026-05-09",
        "2026-05-10",
        total_assets=220.0,
        total_liab=100.0,
        total_hldr_eqy_exc_min_int=90.0,
    )
    for cls in (PITIncomeQuarterlyManager, PITBalanceQuarterlyManager):
        obj = manager(cls, ctx)
        raw = obj._fetch_tushare_data("2026-04-28", "2026-05-10")
        assert obj._batch_upsert_to_pit(obj._preprocess_data(raw), 100)["errors"] == 0
    # Same-day express has lower priority even if its payload/update is later.
    ctx.insert(
        "pit.pit_balance_quarterly",
        ts_code="000928.SZ",
        end_date="2026-03-31",
        ann_date="2026-05-09",
        data_source="express",
        tot_assets=999.0,
        tot_equity=999.0,
        source_ann_date="2026-05-09",
        source_version_hash="express",
        availability_basis="public_disclosure_reconstructed",
        pit_contract_version=FINANCIAL_PIT_CONTRACT,
    )
    indicators = manager(PITFinancialIndicatorsManager, ctx)
    events = indicators._disclosure_events("2026-04-28", "2026-05-10")
    assert set(events.ann_date) == {date(2026, 4, 28), date(2026, 5, 9)}
    calculator = FinancialIndicatorsCalculator(ctx)
    for event in sorted(events.ann_date.unique()):
        result = calculator.calculate_indicators_for_date(
            event.isoformat(), ["000928.SZ"], use_parallel=False
        )
        assert result["success_count"] == 1 and result["failed_count"] == 0
    rows = ctx.fetch_sync(
        "SELECT * FROM pit.pit_financial_indicators ORDER BY ann_date"
    )
    assert len(rows) == 2
    assert rows[1]["ann_date"] == date(2026, 5, 9)
    assert rows[1]["income_ann_date"] == date(2026, 4, 28)
    assert rows[1]["balance_ann_date"] == date(2026, 5, 9)
    assert float(rows[1]["roe_excl_ttm"]) != float(rows[0]["roe_excl_ttm"])
    assert (
        float(
            calculator._fetch_pit_data_batch("2026-05-09", ["000928.SZ"])
            .iloc[0]
            .tot_equity
        )
        == 90.0
    )
    ctx.insert("tushare.stock_basic", ts_code="000928.SZ", list_date="2000-01-01")
    source = PFactorDataRepository(
        ctx, logging.getLogger("test-P")
    ).eligible_indicators("2026-05-08", ["000928.SZ"])
    assert source.iloc[0].ann_date == date(2026, 4, 28)
    source = PFactorDataRepository(
        ctx, logging.getLogger("test-P")
    ).eligible_indicators("2026-05-09", ["000928.SZ"])
    assert source.iloc[0].source_available_date == date(2026, 5, 9)


@pytest.mark.parametrize(
    "recipe_cls,source,metric",
    [
        (StockIncomeQuarterlyMV, "fina_income", "n_income_attr_p"),
        (StockBalanceQuarterlyMV, "fina_balancesheet", "total_assets"),
        (StockCashflowQuarterlyMV, "fina_cashflow", "net_profit"),
    ],
)
def test_features_retain_history_and_have_one_nonnegative_window_per_period(
    financial_db, recipe_cls, source, metric
):
    ctx, created = financial_db
    put_report(ctx, source, "2026-04-28", "2026-06-30", **{metric: 100.0})
    put_report(ctx, source, "2026-05-09", "2026-05-19", **{metric: 80.0})
    # Same-day annual and Q1 must BOTH be queryable, on independent period keys.
    ctx.insert(
        "tushare." + source,
        ts_code="000928.SZ",
        end_date="2025-12-31",
        ann_date="2026-04-28",
        f_ann_date="2026-04-28",
        update_time="2026-04-29",
        report_type=1,
        **{metric: 400.0},
    )
    if source != "fina_cashflow":
        ctx.insert(
            "tushare.fina_express",
            ts_code="000928.SZ",
            end_date="2026-03-31",
            ann_date="2026-04-28",
            update_time="2026-07-01",
            **(
                {metric: 999.0}
                if source == "fina_balancesheet"
                else {"n_income": 999.0}
            ),
        )
    recipe = recipe_cls()
    ctx.execute_sync(recipe.get_create_sql())
    created.append(("MATERIALIZED VIEW", recipe.full_name))
    rows = ctx.fetch_sync(
        f"SELECT * FROM {recipe.full_name} ORDER BY report_period, ann_date"
    )
    assert len(rows) == 3
    assert all(row["query_start_date"] <= row["query_end_date"] for row in rows)
    assert set(row["data_source"] for row in rows) == {"report"}
    for asof, expected in [
        ("2026-04-27", 0),
        ("2026-04-28", 2),
        ("2026-05-08", 2),
        ("2026-05-09", 2),
        ("2026-05-10", 2),
    ]:
        matches = ctx.fetch_sync(
            f"SELECT report_period FROM {recipe.full_name} WHERE %s::date BETWEEN query_start_date AND query_end_date",
            (asof,),
        )
        assert len(matches) == expected
        assert len({row["report_period"] for row in matches}) == expected
    definition = ctx.fetch_sync(
        "SELECT pg_get_viewdef(%s::regclass, true) AS definition", (recipe.full_name,)
    )[0]["definition"]
    assert definition_drift(recipe, definition, None)
    ctx.execute_sync(seal_comment_sql(recipe, definition))
    comment = ctx.fetch_sync(
        "SELECT obj_description(%s::regclass, 'pg_class') AS comment",
        (recipe.full_name,),
    )[0]["comment"]
    assert definition_drift(recipe, definition, comment) is None
    # A manual edit retaining the embedded code marker and old comment is caught.
    assert definition_drift(recipe, definition + " WHERE net_profit > 0", comment)


def test_migration_does_not_certify_legacy_rows_and_can_be_repeated(financial_db):
    ctx, _ = financial_db
    ctx.insert(
        "factors.p_factor",
        ts_code="000928.SZ",
        calc_date="2026-05-08",
        ann_date="2026-04-28",
        p_score=50.0,
        data_source="report",
    )
    ctx.execute_sync(MIGRATION.read_text(encoding="utf-8"))
    assert (
        ctx.fetch_sync("SELECT pit_contract_version FROM factors.p_factor")[0][
            "pit_contract_version"
        ]
        is None
    )
    with pytest.raises(RuntimeError):
        GFactorDataRepository(ctx, logging.getLogger("test-G")).p_history(
            "2026-05-08", ["000928.SZ"]
        )


def test_database_constraint_rejects_backdated_revision(financial_db):
    ctx, _ = financial_db
    with pytest.raises(psycopg2.errors.CheckViolation):
        ctx.insert(
            "pit.pit_balance_quarterly",
            ts_code="000928.SZ",
            end_date="2026-03-31",
            ann_date="2026-04-28",
            data_source="report",
            source_ann_date="2026-04-28",
            source_f_ann_date="2026-05-09",
            source_version_hash="sample",
            availability_basis="public_disclosure_reconstructed",
            pit_contract_version=FINANCIAL_PIT_CONTRACT,
        )


@pytest.mark.parametrize(
    "gap_date,status,blocked",
    [
        ("2026-06-26", "quarantined_source_gap", True),
        ("2024-05-03", "quarantined_source_gap", False),
        ("2026-06-26", "expected_no_data", False),
    ],
)
def test_g_history_distinguishes_failed_market_dates_from_evidenced_empty_dates(
    financial_db, gap_date, status, blocked
):
    ctx, _ = financial_db
    ctx.insert(
        "factors.factor_run_date",
        task_name="factor_p",
        calc_date=gap_date,
        status=status,
        is_current=True,
    )
    ctx.insert(
        "factors.p_factor",
        ts_code="000928.SZ",
        calc_date="2026-07-10",
        ann_date="2026-05-09",
        source_available_date="2026-05-09",
        pit_contract_version=FINANCIAL_PIT_CONTRACT,
        availability_basis="public_disclosure_reconstructed",
        p_score=50.0,
        data_source="report",
    )
    repo = GFactorDataRepository(ctx, logging.getLogger("test-G-history"))
    if blocked:
        with pytest.raises(PHistoryUnavailable, match="incomplete_P_history"):
            repo.p_history("2026-07-10", ["000928.SZ"])
    else:
        assert len(repo.p_history("2026-07-10", ["000928.SZ"])) == 1


def test_bulk_indicator_save_keeps_disclosure_provenance(financial_db):
    ctx, _ = financial_db
    calculator = FinancialIndicatorsCalculator(ctx)
    rows = [
        dict(
            ts_code=f"{index:06d}.SZ",
            end_date=date(2026, 3, 31),
            ann_date=date(2026, 5, 9),
            data_source="report",
            gpa_ttm=20.0,
            roe_excl_ttm=10.0,
            roa_excl_ttm=5.0,
            income_ann_date=date(2026, 4, 28),
            balance_ann_date=date(2026, 5, 9),
            source_available_date=date(2026, 5, 9),
            availability_basis="public_disclosure_reconstructed",
            pit_contract_version=FINANCIAL_PIT_CONTRACT,
            calculation_status="success",
            data_quality="high",
        )
        for index in range(50)
    ]
    calculator._save_indicators_batch(rows)
    saved = ctx.fetch_sync(
        "SELECT COUNT(*) AS n FROM pit.pit_financial_indicators WHERE source_available_date=ann_date AND pit_contract_version=%s",
        (FINANCIAL_PIT_CONTRACT,),
    )
    assert saved[0]["n"] == 50


def test_factor_snapshot_writer_persists_provenance_in_postgresql(financial_db):
    ctx, _ = financial_db
    row = {column: 1.0 for column in P_FACTOR_COLUMNS}
    row.update(
        ts_code="000928.SZ",
        calc_date="2026-05-08",
        ann_date="2026-04-28",
        end_date="2026-03-31",
        data_source="report",
        p_score=50.0,
        p_rank=1,
        data_quality="high",
        calculation_status="success",
        source_available_date="2026-04-28",
        availability_basis="public_disclosure_reconstructed",
        pit_contract_version=FINANCIAL_PIT_CONTRACT,
    )
    ctx.connection.autocommit = False
    count, _, _ = FactorSnapshotWriter(ctx).write(
        pd.DataFrame([row]), "p", "2026-05-08"
    )
    assert count == 1
    saved = GFactorDataRepository(ctx, logging.getLogger("test-G")).p_history(
        "2026-05-08", ["000928.SZ"]
    )
    assert saved.iloc[0].source_available_date == date(2026, 4, 28)
    assert saved.iloc[0].pit_contract_version == FINANCIAL_PIT_CONTRACT
    ctx.connection.commit()
    ctx.connection.autocommit = True


@pytest.mark.parametrize("factor", ["p", "g"])
def test_factor_checksum_matches_postgresql_numeric_rounding(financial_db, factor):
    ctx, _ = financial_db
    columns = P_FACTOR_COLUMNS if factor == "p" else G_FACTOR_COLUMNS
    numeric = "p_score" if factor == "p" else "rank_rm"
    ctx.execute_sync(
        f"ALTER TABLE factors.{factor}_factor ALTER COLUMN {numeric} TYPE numeric(15,6)"
    )
    if factor == "p":
        ctx.execute_sync(
            "ALTER TABLE factors.p_factor ALTER COLUMN gpa TYPE numeric(15,4)"
        )
    row = {column: 1.0 for column in columns}
    row.update(
        ts_code="000928.SZ",
        calc_date="2026-05-08",
        ann_date="2026-04-28",
        source_available_date="2026-04-28",
        data_source="report",
        availability_basis="public_disclosure_reconstructed",
        pit_contract_version=FINANCIAL_PIT_CONTRACT,
        calculation_status="success",
    )
    row[numeric] = 40.8203125
    if factor == "p":
        row.update(end_date="2026-03-31", p_rank=1, data_quality="high", gpa=-1.23445)
    else:
        row.update(g_efficiency_momentum=-0.0000001)
        ctx.execute_sync(
            "ALTER TABLE factors.g_factor ALTER COLUMN g_efficiency_momentum TYPE numeric(15,6)"
        )
    ctx.connection.autocommit = False
    _, checksum, _ = FactorSnapshotWriter(ctx).write(
        pd.DataFrame([row]), factor, "2026-05-08"
    )
    saved = ctx.query_dataframe(
        f"SELECT {','.join(columns)} FROM factors.{factor}_factor"
    )
    assert str(saved.iloc[0][numeric]) == "40.820313"
    assert factor_frame_checksum(saved, columns) == checksum
    ctx.connection.commit()
    ctx.connection.autocommit = True


def test_financial_feature_plan_blocks_stale_definition_without_replacing_it(
    financial_db, monkeypatch, isolated_database_url
):
    from alphahome.features import coordinator
    from alphahome.features.storage.database_init import (
        CREATE_MV_METADATA_TABLE_SQL,
        CREATE_MV_REFRESH_LOG_TABLE_SQL,
    )
    from alphahome.features.storage.recovery import CREATE_CHECKPOINT_SQL

    ctx, created = financial_db
    for name, ddl in (
        ("mv_metadata", CREATE_MV_METADATA_TABLE_SQL),
        ("mv_refresh_log", CREATE_MV_REFRESH_LOG_TABLE_SQL),
        ("refresh_checkpoint", CREATE_CHECKPOINT_SQL),
    ):
        full_name = "features." + name
        if (
            ctx.fetch_sync("SELECT to_regclass(%s) AS name", (full_name,))[0]["name"]
            is None
        ):
            ctx.execute_sync(ddl)
            created.append(("TABLE", full_name))
    recipe = StockCashflowQuarterlyMV()
    put_report(ctx, "fina_cashflow", "2026-04-28", "2026-05-01", net_profit=100.0)
    ctx.execute_sync(recipe.get_create_sql())
    created.append(("MATERIALIZED VIEW", recipe.full_name))
    monkeypatch.setattr(coordinator, "_recipes", lambda: {recipe.name: type(recipe)})
    plan = coordinator.build_feature_plan(
        isolated_database_url, [recipe.name], as_of_date="2026-05-11"
    )
    assert any("definition drift" in blocker for blocker in plan.blockers)
    assert len(ctx.fetch_sync(f"SELECT * FROM {recipe.full_name}")) == 1
    definition = ctx.fetch_sync(
        "SELECT pg_get_viewdef(%s::regclass,true) AS definition", (recipe.full_name,)
    )[0]["definition"]
    ctx.execute_sync(seal_comment_sql(recipe, definition))
    checked = coordinator.build_feature_plan(
        isolated_database_url, [recipe.name], as_of_date="2026-05-11"
    )
    assert not checked.blockers
