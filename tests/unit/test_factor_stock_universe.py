from datetime import date

import pandas as pd

from alphahome.common.schema_names import FACTOR_SCHEMA
from alphahome.factors.core.g_factor_calculator import GFactorCalculator
from alphahome.factors.core.p_factor_calculator import PFactorCalculator


class FakeFactorContext:
    db_manager = object()

    def __init__(self, responses):
        self.responses = responses
        self.queries = []

    def query_dataframe(self, query, params=None):
        self.queries.append((query, params))
        for pattern, frame in self.responses:
            if pattern in query:
                return frame.copy()
        return pd.DataFrame()


def test_p_factor_stock_universe_unions_optimized_and_stock_basic():
    context = FakeFactorContext(
        [
            ("get_trading_stocks_optimized", pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH"]})),
            ("tushare.stock_basic", pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH", "920000.BJ"]})),
        ]
    )
    calculator = PFactorCalculator(context=context)

    assert calculator._get_trading_stock_codes("2026-05-08") == [
        "000001.SZ",
        "600000.SH",
        "920000.BJ",
    ]


def test_g_factor_stock_universe_includes_same_day_p_factor_codes():
    context = FakeFactorContext(
        [
            ("get_trading_stocks_optimized", pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH", "688347.SH"]})),
            (f"{FACTOR_SCHEMA}.p_factor", pd.DataFrame({"ts_code": ["000001.SZ", "600000.SH", "920000.BJ"]})),
        ]
    )
    calculator = GFactorCalculator(context=context)

    assert calculator._get_trading_stock_codes("2026-05-08") == [
        "000001.SZ",
        "600000.SH",
        "920000.BJ",
    ]


def test_p_factor_filter_missing_dates_accepts_python_date_values(caplog):
    context = FakeFactorContext(
        [
            (f"{FACTOR_SCHEMA}.p_factor", pd.DataFrame({"calc_date": [date(2026, 6, 5)]})),
        ]
    )
    calculator = PFactorCalculator(context=context)

    assert calculator._filter_missing_dates(["2026-06-05", "2026-06-12"]) == [
        "2026-06-12"
    ]
    assert "过滤缺失日期失败" not in caplog.text


def test_g_factor_filter_missing_dates_accepts_python_date_values(caplog):
    context = FakeFactorContext(
        [
            (f"{FACTOR_SCHEMA}.g_factor", pd.DataFrame({"calc_date": [date(2026, 6, 5)]})),
        ]
    )
    calculator = GFactorCalculator(context=context)

    assert calculator._filter_missing_dates(["2026-06-05", "2026-06-12"]) == [
        "2026-06-12"
    ]
    assert "过滤缺失日期失败" not in caplog.text
