from datetime import date
import logging

import pandas as pd
import time_machine

from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager
from alphahome.pit.calculators.financial_indicators_calculator import FinancialIndicatorsCalculator
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

    result = manager.incremental_update(days=7, batch_size=100)

    assert result["updated_records"] == 19
    assert result["inserted_records"] == 19
    assert result["updated_existing_records"] == 0


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


def test_balance_incremental_counts_inserted_records(monkeypatch):
    manager = PITBalanceQuarterlyManager()
    _patch_common_incremental_manager(monkeypatch, manager, {"inserted": 7, "updated": 0, "errors": 0})
    monkeypatch.setattr(manager, "_ensure_balance_unique_keys", lambda: None)

    result = manager.incremental_update(days=7, batch_size=100, run_fix_after=False)

    assert result["updated_records"] == 7
    assert result["inserted_records"] == 7
    assert result["updated_existing_records"] == 0


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
