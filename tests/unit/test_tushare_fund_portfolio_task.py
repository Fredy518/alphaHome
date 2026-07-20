import logging

import pandas as pd

from alphahome.fetchers.tasks.fund.tushare_fund_portfolio import (
    TushareFundPortfolioTask,
)


def _make_task():
    return TushareFundPortfolioTask(
        db_connection=object(),
        api_token="test-token",
        api=object(),
    )


def test_process_data_nulls_out_of_range_portfolio_ratios(caplog):
    task = _make_task()
    source = pd.DataFrame(
        {
            "ts_code": ["020709.OF", "020709.OF", "020709.OF"],
            "symbol": ["300737.SZ", "002741.SZ", "688141.SH"],
            "mkv": [5_826_933.0, 5_210_892.0, 8_363_601.0],
            "amount": [886_900.0, 145_800.0, 54_486.0],
            "stk_mkv_ratio": [4.94, -0.01, 100.01],
            "stk_float_ratio": [10_000_083.37, 0.03, 100.0],
        }
    )

    with caplog.at_level(logging.WARNING, logger=task.logger.name):
        actual = task.process_data(source)

    assert actual.loc[0, "stk_mkv_ratio"] == 4.94
    assert pd.isna(actual.loc[0, "stk_float_ratio"])
    assert pd.isna(actual.loc[1, "stk_mkv_ratio"])
    assert pd.isna(actual.loc[2, "stk_mkv_ratio"])
    assert actual.loc[1, "stk_float_ratio"] == 0.03
    assert actual.loc[2, "stk_float_ratio"] == 100.0
    assert actual.loc[0, "mkv"] == 5_826_933.0
    assert source.loc[0, "stk_float_ratio"] == 10_000_083.37
    assert "检测到 3 个超出 [0, 100] 的持仓比例值，已置为 NULL" in caplog.text
    assert "stk_mkv_ratio=2" in caplog.text
    assert "stk_float_ratio=1" in caplog.text


def test_process_data_preserves_valid_ratio_boundaries(caplog):
    task = _make_task()
    source = pd.DataFrame(
        {
            "stk_mkv_ratio": [0, 25.1234, 100],
            "stk_float_ratio": [0, 0.01, 100],
        }
    )

    with caplog.at_level(logging.WARNING, logger=task.logger.name):
        actual = task.process_data(source)

    pd.testing.assert_frame_equal(actual, source)
    assert "超出 [0, 100]" not in caplog.text


def test_process_data_ignores_missing_ratio_columns():
    task = _make_task()
    source = pd.DataFrame({"ts_code": ["020709.OF"], "mkv": [5_826_933.0]})

    actual = task.process_data(source)

    pd.testing.assert_frame_equal(actual, source)
