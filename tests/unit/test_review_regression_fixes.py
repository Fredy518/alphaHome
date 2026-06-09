import logging

import numpy as np
import pandas as pd
import pytest

from alphahome.common.config_manager import redact_sensitive_config, redact_url
from alphahome.common.db_manager import DBManager
from alphahome.factors.core.p_factor_calculator import PFactorCalculator
from alphahome.pit.calculators.financial_indicators_calculator import FinancialIndicatorsCalculator
from alphahome.providers._helpers import DataHelpers
from alphahome.providers._index_queries import IndexQueries
from alphahome.providers._stock_queries import StockQueries


class ExplodingDB:
    mode = "sync"

    def fetch_sync(self, *args, **kwargs):
        raise AssertionError("empty input should not query database")


def test_redacts_database_url_and_nested_secrets():
    url = "postgresql://alice:p%40ss@localhost:5432/alphadb"

    assert redact_url(url) == "postgresql://alice:***@localhost:5432/alphadb"
    assert redact_sensitive_config(
        {
            "database": {"url": url},
            "db_url": url,
            "api": {"tushare_token": "token-value"},
            "tinysoft": {"password": "pw", "session_password": "spw"},
        }
    ) == {
        "database": {"url": "postgresql://alice:***@localhost:5432/alphadb"},
        "db_url": "postgresql://alice:***@localhost:5432/alphadb",
        "api": {"tushare_token": "***REDACTED***"},
        "tinysoft": {"password": "***REDACTED***", "session_password": "***REDACTED***"},
    }


def test_sync_db_url_parser_rejects_missing_database_and_decodes_credentials():
    manager = DBManager("postgresql://alice:p%40ss@localhost:5432/alphadb", mode="sync")

    assert manager._conn_params["user"] == "alice"
    assert manager._conn_params["password"] == "p@ss"
    assert manager._conn_params["database"] == "alphadb"

    with pytest.raises(ValueError):
        DBManager("postgresql://localhost", mode="sync")


def test_financial_indicator_cleaning_preserves_missing_values():
    calculator = FinancialIndicatorsCalculator.__new__(FinancialIndicatorsCalculator)
    calculator.logger = logging.getLogger("test_financial_indicator_cleaning")
    df = pd.DataFrame(
        {
            "gpa_ttm": [None, np.nan, np.inf, 1.23456],
            "roe_excl_ttm": [0.1, None, -np.inf, 2.34567],
        }
    )

    cleaned = calculator._clean_indicators_data(df, list(df.columns))

    assert cleaned["gpa_ttm"].isna().iloc[0]
    assert cleaned["gpa_ttm"].isna().iloc[1]
    assert cleaned["gpa_ttm"].isna().iloc[2]
    assert cleaned["gpa_ttm"].iloc[3] == 1.2346
    assert cleaned["roe_excl_ttm"].isna().iloc[1]
    assert cleaned["roe_excl_ttm"].isna().iloc[2]


def test_p_factor_special_industry_blanks_gpa_ttm_before_standardization():
    calculator = PFactorCalculator.__new__(PFactorCalculator)
    calculator.logger = logging.getLogger("test_p_factor_special_industry")
    calculator._get_industry_classification_pit = lambda stock_codes, as_of_date: pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "requires_special_gpa_handling": [True, False],
            "gpa_calculation_method": ["null", "standard"],
        }
    )
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "gpa_ttm": [9999.0, 10.0],
        }
    )

    result = calculator._apply_industry_special_handling(df, "2026-06-09")

    assert pd.isna(result.loc[result["ts_code"] == "000001.SZ", "gpa_ttm"]).all()
    assert result.loc[result["ts_code"] == "600000.SH", "gpa_ttm"].iloc[0] == 10.0


def test_provider_empty_symbol_lists_return_empty_frames_without_sql():
    assert StockQueries(ExplodingDB()).get_stock_data([], "2024-01-01", "2024-01-02").empty
    assert IndexQueries(ExplodingDB()).get_index_info([]).empty
    assert DataHelpers(ExplodingDB()).get_industry_data([]).empty
