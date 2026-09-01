import numpy as np
import pandas as pd
import pytest

from alphahome.fetchers.tasks.finance.tushare_fina_income import (
    TushareFinaIncomeTask,
)
from alphahome.fetchers.tasks.fund.tushare_fund_basic import (
    TushareFundBasicTask,
)
from alphahome.fetchers.tasks.fund.tushare_fund_dividend import (
    TushareFundDividendTask,
)
from alphahome.fetchers.tasks.fund.tushare_fund_etf_basic import (
    TushareFundEtfBasicTask,
)
from alphahome.fetchers.tasks.fund.tushare_fund_etf_index import (
    TushareFundEtfIndexTask,
)
from alphahome.fetchers.tasks.fund.tushare_fund_nav import TushareFundNavTask
from alphahome.fetchers.tasks.future.tushare_future_basic import (
    TushareFutureBasicTask,
)
from alphahome.fetchers.tasks.future.tushare_future_holding import (
    TushareFutureHoldingTask,
)
from alphahome.fetchers.tasks.index.tushare_index_factor import (
    TushareIndexFactorProTask,
)
from alphahome.fetchers.tasks.index.tushare_index_weight import (
    TushareIndexWeightTask,
)
from alphahome.fetchers.tasks.macro.tushare_macro_sf import TushareMacroSFTTask
from alphahome.fetchers.tasks.stock.tushare_stock_basic import (
    TushareStockBasicTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_dcindex import (
    TushareStockDcIndexTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_dividend import (
    TushareStockDividendTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_holdernumber import (
    TushareStockHolderNumberTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_limitprice import (
    TushareStockLimitPriceTask,
)


def _validate(task_cls, data):
    task = task_cls(
        db_connection=object(),
        api_token="test-token",
        api=object(),
    )
    return task._validate_data(pd.DataFrame(data))


@pytest.mark.parametrize(
    ("task_cls", "data"),
    [
        (
            TushareFinaIncomeTask,
            {
                "ts_code": ["000001.SZ"],
                "ann_date": [pd.Timestamp("2026-08-01")],
                "end_date": [pd.Timestamp("2026-06-30")],
                "revenue": [100.0],
                "total_profit": [-1000.0],
            },
        ),
        (
            TushareFundBasicTask,
            {
                "ts_code": ["000001.OF"],
                "name": ["测试基金"],
                "fund_type": ["混合型"],
                "status": [None],
                "market": [None],
            },
        ),
        (
            TushareFundDividendTask,
            {
                "ts_code": ["025959.OF"],
                "ex_date": [pd.Timestamp("2026-08-27")],
                "div_cash": [0.0],
                "base_unit": [1.0],
                "ear_distr": [-1.0],
                "ear_amount": [np.nan],
                "div_proc": ["实施"],
                "pay_date": [pd.Timestamp("2026-08-28")],
            },
        ),
        (
            TushareFundEtfBasicTask,
            {
                "ts_code": ["159070.OF"],
                "name": ["测试ETF"],
                "status": ["P"],
                "market": ["SZ"],
                "m_fee": [0.5],
            },
        ),
        (
            TushareFundEtfIndexTask,
            {
                "ts_code": ["FISAULM.OTH"],
                "index_name": ["测试指数"],
                "bp": [1000.0],
                "pub_date": [pd.NaT],
                "base_date": [pd.NaT],
            },
        ),
        (
            TushareFundNavTask,
            {
                "ts_code": ["511010.SH"],
                "nav_date": [pd.Timestamp("2026-08-28")],
                "unit_nav": [141.0],
                "accum_nav": [1.46],
                "net_asset": [100.0],
            },
        ),
        (
            TushareFutureBasicTask,
            {
                "ts_code": ["IF2609.CFX"],
                "symbol": ["IF2609"],
                "name": ["沪深300期货"],
                "exchange": ["CFFEX"],
                "multiplier": [np.nan],
                "per_unit": [np.nan],
            },
        ),
        (
            TushareFutureHoldingTask,
            {
                "trade_date": [pd.Timestamp("2026-08-28")],
                "symbol": ["IF2609"],
                "broker": ["测试会员"],
                "exchange": ["CFFEX"],
                "volume": [np.nan],
                "long_hld": [np.nan],
                "short_hld": [np.nan],
            },
        ),
        (
            TushareIndexFactorProTask,
            {
                "ts_code": ["000300.SH"],
                "trade_date": [pd.Timestamp("2026-08-28")],
                "close": [1.0],
                "high": [np.nan],
                "low": [np.nan],
                "volume": [np.nan],
                "amount": [np.nan],
                "rsi_bfq_12": [np.nan],
                "kdj_k_bfq": [np.nan],
            },
        ),
        (
            TushareIndexWeightTask,
            {
                "index_code": ["000300.SH"],
                "con_code": ["000001.SZ"],
                "trade_date": [pd.Timestamp("2026-08-31")],
                "weight": [-1.0],
            },
        ),
        (
            TushareMacroSFTTask,
            {
                "month": ["200201"],
                "inc_month": [-472.0],
                "inc_cumval": [-472.0],
                "stk_endval": [1.0],
                "month_end_date": [pd.Timestamp("2002-01-31")],
            },
        ),
        (
            TushareStockBasicTask,
            {
                "ts_code": ["T600018.SH"],
                "symbol": ["600018"],
                "name": ["上港集箱(退)"],
            },
        ),
        (
            TushareStockDcIndexTask,
            {
                "ts_code": ["BK1675.DC"],
                "trade_date": [pd.Timestamp("2026-08-27")],
                "name": ["历史新高"],
                "pct_change": [1.0],
                "total_mv": [1.0],
                "turnover_rate": [1.0],
                "up_num": [np.nan],
                "down_num": [np.nan],
            },
        ),
        (
            TushareStockDividendTask,
            {
                "ts_code": ["600600.SH"],
                "ex_date": [pd.Timestamp("2026-06-01")],
                "stk_div": [np.nan],
                "cash_div": [1.8],
                "cash_div_tax": [0.0],
                "div_proc": ["实施"],
                "record_date": [pd.Timestamp("2026-05-31")],
                "pay_date": [pd.Timestamp("2026-06-02")],
            },
        ),
        (
            TushareStockLimitPriceTask,
            {
                "trade_date": [pd.Timestamp("2026-08-28")],
                "ts_code": ["920288.BJ"],
                "pre_close": [12.57],
                "up_limit": [99999.99],
                "down_limit": [0.0],
            },
        ),
    ],
)
def test_known_source_edge_cases_do_not_trigger_false_validation_warnings(
    task_cls,
    data,
):
    passed, _, details = _validate(task_cls, data)

    assert passed is True
    assert details["failed_validations"] == {}


def test_index_factor_real_kdj_outlier_remains_visible():
    passed, _, details = _validate(
        TushareIndexFactorProTask,
        {
            "ts_code": ["000300.SH"],
            "trade_date": [pd.Timestamp("2026-08-28")],
            "close": [1.0],
            "high": [1.0],
            "low": [1.0],
            "volume": [1.0],
            "amount": [1.0],
            "rsi_bfq_12": [50.0],
            "kdj_k_bfq": [2123.0],
        },
    )

    assert passed is False
    assert details["failed_validations"] == {"KDJ.K应在0-100或为空": "1行失败"}


def test_holdernumber_missing_value_remains_a_filtered_source_warning():
    passed, result, details = _validate(
        TushareStockHolderNumberTask,
        {
            "ts_code": ["000001.SZ"],
            "ann_date": [pd.Timestamp("2026-08-28")],
            "holder_num": [np.nan],
        },
    )

    assert passed is False
    assert result.empty
    assert details["failed_validations"]["股东户数不能为空"] == "1行失败"
