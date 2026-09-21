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
from alphahome.fetchers.tasks.fund.akshare_fund_cf_em import AkShareFundCfEmTask
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
from alphahome.fetchers.tasks.stock.tushare_stock_ahcomparison import (
    TushareStockAHComparisonTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_report_rc import (
    TushareStockReportRcTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_limitprice import (
    TushareStockLimitPriceTask,
)
from alphahome.fetchers.tasks.stock.tushare_stock_thsindex import (
    TushareStockThsIndexTask,
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


def test_index_factor_process_nulls_invalid_kdj_before_validation():
    task = TushareIndexFactorProTask(
        db_connection=object(), api_token="test-token", api=object()
    )
    processed = task.process_data(
        pd.DataFrame(
            {
                "ts_code": ["000300.SH", "000905.SH"],
                "trade_date": [pd.Timestamp("2026-09-14")] * 2,
                "close": [1.0, 1.0],
                "high": [1.0, 1.0],
                "low": [1.0, 1.0],
                "volume": [1.0, 1.0],
                "amount": [1.0, 1.0],
                "rsi_bfq_12": [50.0, 50.0],
                "kdj_k_bfq": [2123.0, 50.0],
            }
        )
    )

    assert pd.isna(processed.iloc[0]["kdj_k_bfq"])
    assert processed.iloc[1]["kdj_k_bfq"] == 50.0
    assert task._validate_data(processed)[0] is True


def test_ahcomparison_allows_one_market_close_to_be_missing():
    passed, _, details = _validate(
        TushareStockAHComparisonTask,
        {
            "trade_date": [pd.Timestamp("2026-09-14")],
            "ts_code": ["601238.SH"],
            "hk_code": ["02238.HK"],
            "close": [np.nan],
            "hk_close": [10.0],
            "ah_comparison": [1.2],
        },
    )

    assert passed is True
    assert details["failed_validations"] == {}


def test_thsindex_allows_unknown_count_but_rejects_negative_count():
    common = {
        "ts_code": ["883400.TI"],
        "name": ["测试指数"],
        "exchange": ["A"],
        "type": ["S"],
    }

    passed, _, details = _validate(
        TushareStockThsIndexTask,
        {**common, "count": [np.nan]},
    )
    assert passed is True
    assert details["failed_validations"] == {}

    passed, _, details = _validate(
        TushareStockThsIndexTask,
        {**common, "count": [-1]},
    )
    assert passed is False
    assert details["failed_validations"] == {"成分个数有值时不能为负数": "1行失败"}


@pytest.mark.asyncio
async def test_thsindex_publishes_one_atomic_full_snapshot():
    class ReplaceDB:
        def __init__(self):
            self.calls = []

        async def replace_from_dataframe(self, **kwargs):
            self.calls.append(kwargs)
            return len(kwargs["df"])

    db = ReplaceDB()
    task = TushareStockThsIndexTask(
        db_connection=db,
        api_token="test-token",
        api=object(),
        task_config={"stream_batches": True},
    )
    data = pd.DataFrame({"ts_code": ["883400.TI"], "name": ["测试指数"]})

    assert task._should_stream_batches({"stream_batches": True}) is False
    assert await task._save_to_database(data) == 1
    assert len(db.calls) == 1
    assert db.calls[0]["target"] is task
    assert db.calls[0]["timestamp_column"] == "update_time"


def test_holdernumber_process_drops_unusable_source_rows():
    task = TushareStockHolderNumberTask(
        db_connection=object(), api_token="test-token", api=object()
    )
    processed = task.process_data(
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "ann_date": [pd.Timestamp("2026-09-14")] * 2,
                "holder_num": [np.nan, 12345],
            }
        )
    )

    assert processed["ts_code"].tolist() == ["000002.SZ"]
    assert task._validate_data(processed)[0] is True


def test_report_rc_process_drops_incomplete_primary_keys():
    task = TushareStockReportRcTask(
        db_connection=object(), api_token="test-token", api=object()
    )
    processed = task.process_data(
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "report_date": [pd.Timestamp("2026-09-14")] * 2,
                "org_name": ["测试机构", "测试机构"],
                "author_name": [None, "分析师"],
                "quarter": ["2026Q3", "2026Q3"],
                "max_price": [10.0, 10.0],
                "min_price": [9.0, 9.0],
                "roe": [10.0, 10.0],
            }
        )
    )

    assert processed["ts_code"].tolist() == ["000002.SZ"]
    assert task._validate_data(processed)[0] is True


def test_fund_split_process_drops_missing_ratio():
    task = AkShareFundCfEmTask(db_connection=object(), api=object())
    task._fund_code_to_ts_code_cache = {"000001": "000001.OF"}
    processed = task.process_data(
        pd.DataFrame(
            {
                "fund_code": ["000001", "000002"],
                "fund_name": ["有效基金", "异常基金"],
                "split_date": [pd.Timestamp("2026-09-01")] * 2,
                "split_type": ["拆分", "拆分"],
                "split_ratio": [1.5, np.nan],
            }
        )
    )

    assert processed["fund_code"].tolist() == ["000001"]
    assert task._validate_data(processed)[0] is True
