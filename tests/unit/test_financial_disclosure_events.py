"""Small historical witnesses and synthetic cases; no DB/app/network setup."""

from datetime import date
import logging
from unittest.mock import Mock

import pandas as pd
import pytest

from alphahome.pit.disclosure import (
    DISCLOSURE_COLUMNS,
    FINANCIAL_PIT_CONTRACT,
    PUBLIC_AVAILABILITY_BASIS,
    normalize_disclosure_events,
    validate_public_inputs,
)
from alphahome.pit.pit_income_quarterly_manager import PITIncomeQuarterlyManager
from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager
from alphahome.pit.pit_cashflow_quarterly_manager import PITCashflowQuarterlyManager
from alphahome.pit.pit_financial_indicators_manager import PITFinancialIndicatorsManager
from alphahome.pit.calculators.financial_indicators_calculator import (
    FinancialIndicatorsCalculator,
)
from alphahome.factors.core.data_repository import (
    PFactorDataRepository,
    GFactorDataRepository,
)
from alphahome.factors.core.g_factor_calculator import GFactorCalculator
from alphahome.factors.validation import validate_factor_frame
from alphahome.features.recipes.mv.stock.stock_cashflow_quarterly import (
    StockCashflowQuarterlyMV,
)
from alphahome.features.storage.definition_drift import (
    definition_drift,
    definition_seal,
)


def actual_income_sample():
    # 000928.SZ Q1 audit witness: original 4/28 and correction 5/9,
    # both carry the initial ann_date 4/28. Amounts copied from read-only audit.
    return pd.DataFrame(
        [
            dict(
                ts_code="000928.SZ",
                end_date="2026-03-31",
                ann_date="2026-04-28",
                f_ann_date="2026-04-28",
                update_time="2026-04-28 23:14:00",
                data_source="report",
                revenue=2879400012.36,
                oper_cost=2500000000.0,
                n_income_attr_p=199100742.14,
                operate_profit=278207579.81,
            ),
            dict(
                ts_code="000928.SZ",
                end_date="2026-03-31",
                ann_date="2026-04-28",
                f_ann_date="2026-05-09",
                update_time="2026-05-19 22:47:00",
                data_source="report",
                revenue=2879400012.36,
                oper_cost=2500000000.0,
                n_income_attr_p=184563912.14,
                operate_profit=263670749.81,
            ),
        ]
    )


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "asof,expected",
    [
        ("2026-04-27", None),
        ("2026-04-28", 199100742.14),
        ("2026-05-08", 199100742.14),
        ("2026-05-09", 184563912.14),
        ("2026-05-10", 184563912.14),
    ],
)
def test_actual_revision_witness_uses_public_event_not_initial_date(
    reverse, asof, expected
):
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    raw = actual_income_sample().iloc[::-1] if reverse else actual_income_sample()
    processed = manager._preprocess_data(raw)
    assert len(processed) == 2
    assert set(processed.ann_date) == {date(2026, 4, 28), date(2026, 5, 9)}
    available = processed[processed.ann_date <= date.fromisoformat(asof)]
    assert (None if available.empty else available.iloc[-1].n_income_attr_p) == expected


def test_late_collected_original_does_not_supersede_later_public_revision():
    raw = actual_income_sample()
    raw.loc[0, "update_time"] = "2026-06-30 10:00:00"
    events = normalize_disclosure_events(raw)
    assert events.iloc[-1].n_income_attr_p == 184563912.14
    assert events.iloc[0].n_income_attr_p == 199100742.14
    assert events.iloc[0].source_update_time.date() == date(2026, 6, 30)
    with pytest.raises(ValueError, match="first-receipt ledger"):
        normalize_disclosure_events(raw, mode="system_asof")


def test_mixed_sources_fill_missing_provenance_per_row():
    report = actual_income_sample().iloc[[0]].copy()
    report["source_ann_date"] = report["ann_date"]
    report["source_f_ann_date"] = report["f_ann_date"]
    report["source_update_time"] = report["update_time"]
    others = pd.DataFrame(
        [
            dict(
                ts_code="000928.SZ",
                end_date="2026-03-31",
                ann_date="2026-04-15",
                update_time="2026-04-16",
                data_source="express",
                revenue=90.0,
            ),
            dict(
                ts_code="000928.SZ",
                end_date="2026-03-31",
                ann_date="2026-04-05",
                update_time="2026-04-06",
                data_source="forecast",
                n_income_attr_p=15.0,
            ),
        ]
    )
    events = normalize_disclosure_events(pd.concat([report, others], ignore_index=True))
    assert set(events.data_source) == {"report", "express", "forecast"}
    by_source = events.set_index("data_source")
    assert by_source.loc["express", "ann_date"] == date(2026, 4, 15)
    assert by_source.loc["forecast", "ann_date"] == date(2026, 4, 5)
    assert by_source.loc["express", "source_update_time"] == pd.Timestamp("2026-04-16")


@pytest.mark.parametrize("same_update", [False, True])
def test_same_event_duplicate_selection_does_not_depend_on_input_order(same_update):
    raw = actual_income_sample().iloc[[0, 0]].copy().reset_index(drop=True)
    raw.loc[1, "n_income_attr_p"] = 7.0
    if not same_update:
        raw.loc[1, "update_time"] = "2026-04-29 10:00:00"
    forward = normalize_disclosure_events(raw)
    reverse = normalize_disclosure_events(raw.iloc[::-1])
    pd.testing.assert_frame_equal(forward, reverse)
    if not same_update:
        assert forward.iloc[0].n_income_attr_p == 7.0


@pytest.mark.parametrize(
    "fann,expected",
    [
        (None, date(2026, 4, 28)),
        ("2026-04-27", date(2026, 4, 28)),
        ("2026-05-09", date(2026, 5, 9)),
    ],
)
def test_actual_date_fallback_never_backdates(fann, expected):
    raw = actual_income_sample().iloc[[0]].copy()
    raw["f_ann_date"] = fann
    assert normalize_disclosure_events(raw).iloc[0].ann_date == expected


@pytest.mark.parametrize(
    "cls,fields",
    [
        (
            PITBalanceQuarterlyManager,
            {
                "total_assets": 200.0,
                "total_liab": 80.0,
                "total_hldr_eqy_exc_min_int": 100.0,
            },
        ),
        (PITCashflowQuarterlyManager, {"net_profit": 100.0, "n_cashflow_act": 90.0}),
    ],
)
def test_other_statements_preserve_all_public_versions(cls, fields):
    manager = cls()
    manager.logger = Mock()
    if cls is PITBalanceQuarterlyManager:
        manager._fill_express_missing_fields = lambda data: data
    raw = actual_income_sample().drop(
        columns=["revenue", "oper_cost", "operate_profit", "n_income_attr_p"]
    )
    for column, value in fields.items():
        raw[column] = value
    processed = manager._preprocess_data(raw)
    assert len(processed) == 2
    assert set(DISCLOSURE_COLUMNS) <= set(processed)


def test_quarter_difference_uses_baseline_public_at_each_event():
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    raw = pd.DataFrame(
        [
            dict(
                ts_code="000001.SZ",
                end_date="2026-03-31",
                ann_date="2026-04-28",
                data_source="report",
                revenue=100.0,
            ),
            dict(
                ts_code="000001.SZ",
                end_date="2026-03-31",
                ann_date="2026-09-01",
                data_source="report",
                revenue=120.0,
            ),
            dict(
                ts_code="000001.SZ",
                end_date="2026-06-30",
                ann_date="2026-08-08",
                data_source="report",
                revenue=250.0,
            ),
            dict(
                ts_code="000001.SZ",
                end_date="2026-06-30",
                ann_date="2026-09-02",
                data_source="express",
                revenue=260.0,
            ),
        ]
    )
    result = manager._quarterize_to_single(raw)
    assert result.loc[2, "revenue"] == 150.0  # excludes the 9/1 Q1 correction
    assert result.loc[3, "revenue"] == 140.0  # includes it after publication


def test_missing_previous_cumulative_is_not_labeled_single_quarter():
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    raw = pd.DataFrame(
        [
            dict(
                ts_code="000001.SZ",
                end_date="2026-06-30",
                ann_date="2026-08-08",
                data_source="report",
                revenue=250.0,
            )
        ]
    )
    assert pd.isna(manager._quarterize_to_single(raw).iloc[0].revenue)


@pytest.mark.parametrize("reverse", [False, True])
def test_forecast_yoy_lookup_keeps_public_boundaries_and_stock_period_keys(reverse):
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    manager._get_table_columns = lambda schema, table: (
        {"p_change_min", "p_change_max"}
        if table == "fina_forecast"
        else {"n_income_attr_p"}
    )
    forecasts = pd.DataFrame(
        [
            dict(
                ts_code="000001.SZ",
                end_date=date(2026, 12, 31),
                ann_date=date(2026, 4, 27),
                p_change_min=10.0,
                p_change_max=10.0,
            ),
            dict(
                ts_code="000001.SZ",
                end_date=date(2026, 12, 31),
                ann_date=date(2026, 4, 28),
                p_change_min=10.0,
                p_change_max=10.0,
            ),
            dict(
                ts_code="000001.SZ",
                end_date=date(2026, 12, 31),
                ann_date=date(2026, 5, 8),
                p_change_min=10.0,
                p_change_max=10.0,
            ),
            dict(
                ts_code="000001.SZ",
                end_date=date(2026, 12, 31),
                ann_date=date(2026, 5, 9),
                p_change_min=10.0,
                p_change_max=10.0,
            ),
            dict(
                ts_code="000002.SZ",
                end_date=date(2026, 6, 30),
                ann_date=date(2026, 4, 28),
                p_change_min=10.0,
                p_change_max=10.0,
            ),
        ]
    )
    bases = pd.DataFrame(
        [
            dict(
                ts_code="000001.SZ",
                end_date=date(2025, 12, 31),
                ann_date=date(2026, 4, 28),
                yoy_base=100.0,
                update_time="2026-04-28",
                source_version_hash="a",
            ),
            dict(
                ts_code="000001.SZ",
                end_date=date(2025, 12, 31),
                ann_date=date(2026, 4, 28),
                yoy_base=102.0,
                update_time="2026-06-30",
                source_version_hash="b",
            ),
            dict(
                ts_code="000001.SZ",
                end_date=date(2025, 12, 31),
                ann_date=date(2026, 4, 28),
                yoy_base=103.0,
                update_time="2026-06-30",
                source_version_hash="c",
            ),
            dict(
                ts_code="000001.SZ",
                end_date=date(2025, 12, 31),
                ann_date=date(2026, 5, 9),
                yoy_base=80.0,
                update_time="2026-05-19",
                source_version_hash="d",
            ),
            dict(
                ts_code="000002.SZ",
                end_date=date(2025, 6, 30),
                ann_date=date(2025, 7, 28),
                yoy_base=500.0,
                update_time="2025-07-28",
                source_version_hash="e",
            ),
            dict(
                ts_code="000001.SZ",
                end_date=date(2025, 9, 30),
                ann_date=date(2025, 10, 28),
                yoy_base=999.0,
                update_time="2025-10-28",
                source_version_hash="f",
            ),
        ]
    )
    if reverse:
        bases = bases.iloc[::-1]
    manager.context = Mock()
    manager.context.query_dataframe.side_effect = [forecasts, bases]
    result = manager._fetch_income_forecast("2026-04-27", "2026-05-09")
    values = result.set_index(["ts_code", "end_date", "ann_date"])["net_profit_mid"]
    assert pd.isna(values.loc[("000001.SZ", date(2026, 12, 31), date(2026, 4, 27))])
    assert values.loc[
        ("000001.SZ", date(2026, 12, 31), date(2026, 4, 28))
    ] == pytest.approx(113.3)
    assert values.loc[
        ("000001.SZ", date(2026, 12, 31), date(2026, 5, 8))
    ] == pytest.approx(113.3)
    assert values.loc[
        ("000001.SZ", date(2026, 12, 31), date(2026, 5, 9))
    ] == pytest.approx(88.0)
    assert values.loc[
        ("000002.SZ", date(2026, 6, 30), date(2026, 4, 28))
    ] == pytest.approx(550.0)


def test_balance_only_date_is_enumerated_by_indicator_manager():
    manager = PITFinancialIndicatorsManager()
    manager.context = Mock()
    manager._disclosure_events("2026-05-08", "2026-05-10", ts_code="000928.SZ")
    query, params = manager.context.query_dataframe.call_args.args
    assert "UNION" in query and "pit_balance_quarterly" in query
    assert params == ("2026-05-08", "2026-05-10", "000928.SZ") * 2


def public_frame(**extra):
    return pd.DataFrame(
        [
            dict(
                ts_code="000928.SZ",
                ann_date="2026-05-09",
                source_available_date="2026-05-09",
                pit_contract_version=FINANCIAL_PIT_CONTRACT,
                availability_basis=PUBLIC_AVAILABILITY_BASIS,
                **extra,
            )
        ]
    )


def test_indicator_observation_uses_balance_event_date_without_backdating():
    calculator = FinancialIndicatorsCalculator(Mock(db_manager=object()))
    data = public_frame(
        end_date=date(2026, 3, 31),
        data_source="report",
        balance_ann_date=date(2026, 5, 9),
        revenue=100.0,
        oper_cost=50.0,
        n_income_attr_p=20.0,
        operate_profit=25.0,
        tot_assets=200.0,
        tot_equity=100.0,
    )
    data["ann_date"] = date(2026, 4, 28)
    value = calculator._calculate_single_stock_indicators(
        "000928.SZ", "2026-05-09", data, current_record=data.iloc[0]
    )
    assert value["ann_date"] == date(2026, 5, 9)
    assert value["income_ann_date"] == date(2026, 4, 28)
    assert value["source_available_date"] == date(2026, 5, 9)


@pytest.mark.parametrize(
    "repository,method",
    [
        (PFactorDataRepository, "eligible_indicators"),
        (GFactorDataRepository, "p_history"),
    ],
)
@pytest.mark.parametrize("failure", ["future", "legacy", "missing"])
def test_factor_consumers_reject_unverified_or_future_sources(
    repository, method, failure
):
    frame = public_frame(calc_date="2026-05-08")
    if failure == "legacy":
        frame["pit_contract_version"] = None
    if failure == "missing":
        frame = frame.drop(columns="availability_basis")
    context = Mock()
    if repository is GFactorDataRepository:
        context.query_dataframe.side_effect = [pd.DataFrame(), frame]
    else:
        context.query_dataframe.return_value = frame
    repo = repository(context, logging.getLogger("test-disclosure"))
    with pytest.raises(RuntimeError):
        getattr(repo, method)("2026-05-08", ["000928.SZ"])


def test_source_guard_accepts_delayed_receipt_public_reconstruction():
    frame = public_frame()
    frame["source_update_time"] = "2026-06-30"
    validate_public_inputs(
        frame, "2026-05-09", available_column="source_available_date"
    )


def test_definition_drift_detects_code_change_and_manual_ddl_with_same_marker():
    recipe = StockCashflowQuarterlyMV()
    live = recipe.get_create_sql().split(" AS ", 1)[1]
    seal = definition_seal(recipe, live)
    assert definition_drift(recipe, live, seal) is None
    assert definition_drift(recipe, live, None)
    assert definition_drift(recipe, live + " WHERE net_profit > 0", seal)
    changed = StockCashflowQuarterlyMV()
    changed.get_create_sql = (
        lambda: recipe.get_create_sql()
        .replace("public_disclosure_v2", "public_disclosure_v3")
        .replace(
            __import__("re")
            .search(r"'([a-f0-9]{64})'::text", recipe.get_create_sql())
            .group(1),
            "a" * 64,
        )
    )
    assert definition_drift(changed, live, seal)


def test_reused_calculator_discards_source_cache_before_late_arrival_replay():
    calculator = FinancialIndicatorsCalculator(Mock(db_manager=object()))
    calculator._data_cache["old"] = object()
    calculator._calculate_serial = Mock(return_value={})
    calculator.calculate_indicators_for_date("2026-05-09", ["000928.SZ"])
    assert calculator._data_cache == {}


def test_source_guard_rejects_future_observation_even_with_earlier_source_date():
    frame = public_frame()
    frame["source_available_date"] = "2026-04-28"
    with pytest.raises(ValueError, match="observation"):
        validate_public_inputs(
            frame, "2026-05-08", available_column="source_available_date"
        )


def test_g_observation_covers_history_when_current_p_uses_older_financial_event():
    calculator = GFactorCalculator(context=Mock(db_manager=object()))
    history = pd.DataFrame(
        [
            dict(
                ts_code="000928.SZ",
                calc_date=calc_date,
                ann_date=ann_date,
                source_available_date=ann_date,
                data_source="report",
                p_score=score,
                revenue_yoy_growth=10.0,
                n_income_yoy_growth=20.0,
                availability_basis=PUBLIC_AVAILABILITY_BASIS,
                pit_contract_version=FINANCIAL_PIT_CONTRACT,
            )
            for calc_date, ann_date, score in [
                ("2025-07-11", "2025-04-28", 40.0),
                ("2026-07-03", "2026-06-30", 60.0),
                ("2026-07-10", "2026-05-09", 50.0),
            ]
        ]
    )
    result = calculator._calculate_g_factors_from_p_data_pit(history, "2026-07-10")
    assert len(result) == 1
    assert pd.Timestamp(result.iloc[0]["ann_date"]).date() == date(2026, 6, 30)
    assert result.iloc[0]["source_available_date"] == date(2026, 6, 30)
    assert result.iloc[0]["g_efficiency_momentum"] == pytest.approx(10.0)
    assert result.iloc[0]["g_revenue_momentum"] == pytest.approx(10.0)
    assert result.iloc[0]["g_profit_momentum"] == pytest.approx(20.0)
    validate_factor_frame(result, "g", "2026-07-10", ["000928.SZ"])
