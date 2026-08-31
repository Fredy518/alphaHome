from datetime import date, datetime
import logging
from unittest.mock import Mock

import pandas as pd
import time_machine

from alphahome.pit.base.pit_config import PITConfig
from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager
from alphahome.pit.pit_cashflow_quarterly_manager import PITCashflowQuarterlyManager
from alphahome.pit.calculators.financial_indicators_calculator import FinancialIndicatorsCalculator
from alphahome.pit.pit_financial_indicators_manager import PITFinancialIndicatorsManager
from alphahome.pit.pit_income_quarterly_manager import PITIncomeQuarterlyManager
from alphahome.pit.pit_industry_classification_manager import PITIndustryClassificationManager


def _patch_common_incremental_manager(monkeypatch, manager, upsert_result):
    manager.logger = logging.getLogger("test_pit_manager_regressions")
    raw = pd.DataFrame({"ts_code": ["002549.SZ"], "ann_date": ["2026-07-02"], "end_date": ["2026-06-30"]})
    monkeypatch.setattr(manager, "_ensure_table_exists", lambda: None)
    monkeypatch.setattr(manager, "_fetch_tushare_data", lambda start_date, end_date: raw)
    monkeypatch.setattr(manager, "_preprocess_data", lambda data: data)
    monkeypatch.setattr(manager, "_batch_upsert_to_pit", lambda data, batch_size: upsert_result)
    monkeypatch.setattr(manager, "ensure_indexes", lambda: None)


def test_income_incremental_counts_inserted_records(monkeypatch):
    manager = PITIncomeQuarterlyManager()
    _patch_common_incremental_manager(monkeypatch, manager, {"inserted": 19, "updated": 0, "errors": 0})
    monkeypatch.setattr(manager, "_ensure_income_unique_keys", lambda: None)
    captured = {}

    def resolve(days, source_specs):
        captured["days"] = days
        captured["source_specs"] = source_specs
        return "2026-07-01", "2026-08-26"

    monkeypatch.setattr(manager, "resolve_incremental_date_range", resolve)

    result = manager.incremental_update(days=7, batch_size=100)

    assert result["updated_records"] == 19
    assert result["inserted_records"] == 19
    assert result["updated_existing_records"] == 0
    assert captured["days"] == 7
    assert {spec[0] for spec in captured["source_specs"]} == {
        "tushare.fina_income",
        "tushare.fina_express",
        "tushare.fina_forecast",
    }


def test_income_forecast_horizon_metadata_marks_outliers():
    manager = PITIncomeQuarterlyManager()
    data = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
            "end_date": ["2026-06-30", "2026-12-31", "2027-12-31", "2025-12-31"],
            "ann_date": ["2026-03-31", "2026-03-31", "2026-03-31", "2026-04-30"],
            "data_source": ["forecast", "forecast", "forecast", "report"],
        }
    )

    result = manager._apply_forecast_horizon_metadata(data)

    forecast_rows = result[result["data_source"] == "forecast"].set_index("ts_code")
    assert forecast_rows.loc["000001.SZ", "forecast_horizon_bucket"] == "near_term"
    assert bool(forecast_rows.loc["000001.SZ", "is_usable_forecast"]) is True
    assert forecast_rows.loc["000002.SZ", "forecast_horizon_bucket"] == "long_horizon"
    assert bool(forecast_rows.loc["000002.SZ", "is_usable_forecast"]) is True
    assert forecast_rows.loc["000003.SZ", "forecast_horizon_status"] == "horizon_outlier"
    assert bool(forecast_rows.loc["000003.SZ", "is_usable_forecast"]) is False
    assert pd.isna(result.loc[result["ts_code"] == "000004.SZ", "forecast_horizon_bucket"]).all()


def test_income_quarterly_summary_logs_source_aligned_missing_ratio():
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    stats = {
        "report": {
            "total_records": 24074,
            "q2q4_records": 18133,
            "annual_only_records": 240,
            "missing_prev_cumulative": 6888,
            "affected_single_calculation": 0,
            "field_affected_breakdown": {},
        },
        "express": {
            "total_records": 1250,
            "q2q4_records": 1231,
            "annual_only_records": 0,
            "missing_prev_cumulative": 42,
            "affected_single_calculation": 1231,
            "field_affected_breakdown": {},
        },
        "forecast": {
            "total_records": 5315,
            "q2q4_records": 5137,
            "annual_only_records": 0,
            "missing_prev_cumulative": 47,
            "affected_single_calculation": 25685,
            "field_affected_breakdown": {},
        },
    }

    manager._print_quarterly_stats_summary(stats)

    messages = [call.args[0] for call in manager.logger.info.call_args_list]
    assert "  Express/Forecast累计值缺失比例: 1.40%" in messages
    assert ".2f" not in messages


def test_income_fetch_extends_only_report_dependencies(monkeypatch):
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    calls = []

    def fake_fetch(source):
        def _fetch(start_date, end_date, ts_code=None):
            calls.append((source, start_date, end_date, ts_code))
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "end_date": ["2026-06-30"],
                    "ann_date": ["2026-08-08"],
                    "data_source": [source],
                }
            )

        return _fetch

    monkeypatch.setattr(manager, "_fetch_income_report", fake_fetch("report"))
    monkeypatch.setattr(manager, "_fetch_income_express", fake_fetch("express"))
    monkeypatch.setattr(manager, "_fetch_income_forecast", fake_fetch("forecast"))

    manager._fetch_tushare_data("2026-08-07", "2026-08-14", ts_code="000001.SZ")

    assert calls == [
        ("report", "2025-08-07", "2026-08-14", "000001.SZ"),
        ("express", "2026-08-07", "2026-08-14", "000001.SZ"),
        ("forecast", "2026-08-07", "2026-08-14", "000001.SZ"),
    ]


def test_income_report_fetch_deterministically_resolves_duplicate_source_rows(
    monkeypatch,
):
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    captured = {}

    class _Context:
        def query_dataframe(self, query, params=None):
            captured["query"] = query
            captured["params"] = params
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "end_date": ["2024-12-31"],
                    "ann_date": ["2025-03-20"],
                    "revenue": [1.0],
                    "oper_cost": [1.0],
                    "n_income_attr_p": [1_150_000.0],
                    "operate_profit": [1.0],
                    "n_income_attr_p_ytd": [1_150_000.0],
                    "basic_eps_ytd": [1.15],
                    "diluted_eps_ytd": [1.14],
                    "report_source_update_time": ["2025-03-21"],
                    "report_source_row_count": [2],
                    "report_source_value_conflict": [True],
                    "report_source_selection_basis": [
                        manager.REPORT_SOURCE_SELECTION_BASIS
                    ],
                }
            )

    manager.context = _Context()
    monkeypatch.setattr(
        manager,
        "_get_table_columns",
        lambda schema, table: {
            "n_income_attr_p",
            "basic_eps",
            "diluted_eps",
        },
    )

    result = manager._fetch_income_report(
        "2025-03-01", "2025-03-31", ts_code="000001.SZ"
    )

    assert "ROW_NUMBER() OVER" in captured["query"]
    assert "md5(row_to_json(source)::text) DESC" in captured["query"]
    assert "report_source_value_conflict" in captured["query"]
    assert captured["params"] == (
        "2025-03-01",
        "2025-03-31",
        "000001.SZ",
    )
    assert result.iloc[0].report_source_row_count == 2
    assert bool(result.iloc[0].report_source_value_conflict) is True
    assert result.iloc[0].data_source == "report"


def test_income_quarterly_stats_exclude_extended_dependency_rows():
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    data = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "end_date": ["2026-03-31", "2026-06-30"],
            "ann_date": ["2026-04-30", "2026-08-08"],
            "data_source": ["report", "report"],
            "conversion_status": ["RPT_ORIG", "RPT_ORIG"],
            "revenue": [100.0, 250.0],
            "is_extended": [True, False],
        }
    )

    result = manager._quarterize_to_single(data)

    target = result.loc[result["is_extended"].eq(False)].iloc[0]
    assert target["revenue"] == 150.0
    assert target["conversion_status"] == "QTR_DIFF_RPT"
    messages = [call.args[0] for call in manager.logger.info.call_args_list]
    assert "  📋 总记录数: 1 条" in messages
    assert "  🎯 Q2-Q4记录数: 1 条" in messages


def test_income_quarterization_preserves_annual_actual_ytd_fields():
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    data = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "end_date": ["2024-09-30", "2024-12-31"],
            "ann_date": ["2024-10-30", "2025-03-20"],
            "data_source": ["report", "report"],
            "conversion_status": ["RPT_ORIG", "RPT_ORIG"],
            "n_income_attr_p": [800_000.0, 1_150_000.0],
            "n_income_attr_p_ytd": [800_000.0, 1_150_000.0],
            "basic_eps_ytd": [0.80, 1.15],
            "diluted_eps_ytd": [0.79, 1.14],
        }
    )

    result = manager._quarterize_to_single(data)
    annual = result.loc[
        pd.to_datetime(result["end_date"]).eq(pd.Timestamp("2024-12-31"))
    ].iloc[0]

    assert annual.n_income_attr_p == 350_000.0
    assert annual.n_income_attr_p_ytd == 1_150_000.0
    assert annual.basic_eps_ytd == 1.15
    assert annual.diluted_eps_ytd == 1.14


def test_balance_incremental_counts_inserted_records(monkeypatch):
    manager = PITBalanceQuarterlyManager()
    _patch_common_incremental_manager(monkeypatch, manager, {"inserted": 7, "updated": 0, "errors": 0})
    monkeypatch.setattr(manager, "_ensure_balance_unique_keys", lambda: None)
    captured = {}

    def resolve(days, source_specs):
        captured["source_specs"] = source_specs
        return "2026-07-01", "2026-08-26"

    monkeypatch.setattr(manager, "resolve_incremental_date_range", resolve)

    result = manager.incremental_update(days=7, batch_size=100, run_fix_after=False)

    assert result["updated_records"] == 7
    assert result["inserted_records"] == 7
    assert result["updated_existing_records"] == 0
    assert {spec[0] for spec in captured["source_specs"]} == {
        "tushare.fina_balancesheet",
        "tushare.fina_express",
    }


class _IncrementalWatermarkContext:
    def __init__(self, last_success, changed_dates):
        self.last_success = last_success
        self.changed_dates = list(changed_dates)
        self.calls = []

    def query_dataframe(self, query, params=None):
        self.calls.append((query, params))
        if "public.task_status" in query:
            return pd.DataFrame({"last_success_local": [self.last_success]})
        value = self.changed_dates.pop(0)
        return pd.DataFrame({"min_changed_date": [value]})


def test_incremental_watermark_expands_to_late_arrival(monkeypatch):
    manager = PITIncomeQuarterlyManager()
    manager.logger = Mock()
    manager.context = _IncrementalWatermarkContext(
        datetime(2026, 8, 14, 14, 16),
        [date(2026, 7, 16), date(2026, 8, 7), None],
    )
    monkeypatch.setattr(
        PITConfig,
        "get_incremental_date_range",
        staticmethod(lambda days=None: ("2026-08-19", "2026-08-26")),
    )

    start_date, end_date = manager.resolve_incremental_date_range(
        7,
        (
            ("tushare.fina_income", ("ann_date",), "update_time"),
            ("tushare.fina_express", ("ann_date",), "update_time"),
            ("tushare.fina_forecast", ("ann_date",), "update_time"),
        ),
    )

    assert (start_date, end_date) == ("2026-07-16", "2026-08-26")
    source_params = manager.context.calls[1][1]
    assert source_params == (datetime(2026, 8, 13, 14, 16), date(2026, 8, 26))
    manager.logger.info.assert_called_once()


def test_incremental_watermark_never_shortens_rolling_window(monkeypatch):
    manager = PITCashflowQuarterlyManager()
    manager.logger = Mock()
    manager.context = _IncrementalWatermarkContext(
        datetime(2026, 8, 25, 15, 8),
        [date(2026, 8, 24)],
    )
    monkeypatch.setattr(
        PITConfig,
        "get_incremental_date_range",
        staticmethod(lambda days=None: ("2026-08-19", "2026-08-26")),
    )

    result = manager.resolve_incremental_date_range(
        7,
        (("tushare.fina_cashflow", ("f_ann_date", "ann_date"), "update_time"),),
    )

    assert result == ("2026-08-19", "2026-08-26")
    assert 'COALESCE("f_ann_date", "ann_date")' in manager.context.calls[1][0]


def test_financial_indicators_tracks_income_and_balance_watermarks(monkeypatch):
    manager = PITFinancialIndicatorsManager()
    manager.logger = Mock()
    manager.context = Mock()
    manager.context.query_dataframe.return_value = pd.DataFrame()
    captured = {}

    def resolve(days, source_specs):
        captured["source_specs"] = source_specs
        return "2026-07-01", "2026-08-26"

    monkeypatch.setattr(manager, "resolve_incremental_date_range", resolve)
    monkeypatch.setattr(manager, "_ensure_table_exists", lambda: None)
    monkeypatch.setattr(manager, "ensure_table_exists", lambda: None)
    monkeypatch.setattr(manager, "_initialize_calculator", lambda: None)
    monkeypatch.setattr(manager, "_remove_forecast_indicator_rows", lambda: 0)

    result = manager.incremental_update(days=7, batch_size=100)

    assert result["updated_records"] == 0
    assert {spec[0] for spec in captured["source_specs"]} == {
        "pit.pit_income_quarterly",
        "pit.pit_balance_quarterly",
    }


def test_financial_indicators_incremental_preserves_original_error(monkeypatch):
    manager = PITFinancialIndicatorsManager()
    manager.logger = Mock()
    manager.context = Mock()
    manager.context.query_dataframe.side_effect = RuntimeError("indicator source failed")
    monkeypatch.setattr(
        manager,
        "resolve_incremental_date_range",
        lambda days, source_specs: ("2026-07-01", "2026-08-26"),
    )
    monkeypatch.setattr(manager, "_ensure_table_exists", lambda: None)
    monkeypatch.setattr(manager, "ensure_table_exists", lambda: None)
    monkeypatch.setattr(manager, "_initialize_calculator", lambda: None)
    monkeypatch.setattr(manager, "_remove_forecast_indicator_rows", lambda: 0)

    result = manager.incremental_update(days=7, batch_size=100)

    assert result["updated_records"] == 0
    assert result["error"] == "indicator source failed"


def test_balance_preprocess_validates_including_minority_and_keeps_sources(monkeypatch):
    manager = PITBalanceQuarterlyManager()
    manager.logger = Mock()
    monkeypatch.setattr(manager, "_fill_express_missing_fields", lambda data: data)
    raw = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "end_date": ["2026-06-30"] * 3,
            "ann_date": ["2026-08-08", "2026-08-08", "2026-08-09"],
            "data_source": ["report", "express", "report"],
            "total_assets": [100.0, 100.0, 100.0],
            "total_liab": [80.0, None, 80.0],
            "total_hldr_eqy_exc_min_int": [10.0, 10.0, 10.0],
            "total_hldr_eqy_inc_min_int": [20.0, None, 15.0],
        }
    )

    result = manager._preprocess_data(raw)

    assert len(result) == 3
    assert set(result["data_source"]) == {"report", "express"}
    assert "_tot_equity_including_minority" not in result.columns
    warning_messages = [call.args[0] for call in manager.logger.warning.call_args_list]
    assert warning_messages == ["发现 1 条资产负债不平衡的记录"]


@time_machine.travel("2026-07-03 13:30:00", tick=False)
def test_industry_affected_months_do_not_include_future_month_end():
    manager = PITIndustryClassificationManager()

    months = manager._get_affected_months("2026-04-03")

    assert months == [date(2026, 4, 1), date(2026, 5, 1), date(2026, 6, 1)]


class _FakeIndicatorDB:
    def __init__(self):
        self.calls = []

    def fetch_sync(self, query, params=None):
        self.calls.append((query, params))
        if "COUNT(*)" in query:
            return [{"total_rpt_orig_count": 0}]
        return []


class _FakeIndicatorContext:
    def __init__(self):
        self.db_manager = _FakeIndicatorDB()


def test_financial_indicator_fetch_defaults_to_standard_accounting_sources():
    context = _FakeIndicatorContext()
    calculator = FinancialIndicatorsCalculator(context)

    calculator._get_pit_data_for_calculation("2026-07-03", ["002549.SZ"])

    fetch_query, fetch_params = context.db_manager.calls[-1]
    assert "data_source = ANY(%s)" in fetch_query
    assert "i.data_source IN ('forecast'" not in fetch_query
    assert fetch_params[3] == ["report", "express"]


def test_financial_indicator_performance_log_is_gbk_safe_and_zero_safe():
    calculator = FinancialIndicatorsCalculator.__new__(FinancialIndicatorsCalculator)
    calculator.logger = Mock()
    calculator.enable_cache = False
    calculator.stats = {
        "start_time": 1.0,
        "end_time": 1.0,
        "successful_calculations": 0,
        "failed_calculations": 0,
        "cache_hits": 0,
        "cache_misses": 0,
    }

    calculator._log_performance_stats(detailed=True)

    messages = [call.args[0] for call in calculator.logger.info.call_args_list]
    assert messages
    for message in messages:
        message.encode("gbk")
