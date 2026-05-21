import pandas as pd
import pytest

from alphahome.fetchers.tasks.option.tushare_option_basic import (
    FINANCIAL_OPTION_EXCHANGES,
    TushareOptionBasicTask,
)


class _DummyApi:
    pass


def _make_task():
    return TushareOptionBasicTask(
        db_connection=object(),
        api_token="test-token",
        api=_DummyApi(),
    )


@pytest.mark.asyncio
async def test_tushare_option_basic_batches_only_financial_exchanges():
    task = _make_task()

    batches = await task.get_batch_list()

    assert [batch["exchange"] for batch in batches] == list(FINANCIAL_OPTION_EXCHANGES)


def test_tushare_option_basic_filters_commodity_options_before_save():
    task = _make_task()
    raw = pd.DataFrame(
        {
            "ts_code": [
                "IO2608-C-4600.CFX",
                "10000001.SH",
                "AU2608-C-460.SHF",
                "M2608-P-3000.DCE",
            ],
            "exchange": ["CFFEX", "SSE", "SHFE", "DCE"],
            "name": ["沪深300指数期权", "ETF期权", "黄金期权", "豆粕期权"],
            "call_put": ["C", "P", "C", "P"],
            "exercise_price": [4600, 2.5, 460, 3000],
            "maturity_date": ["20260821", "20260821", "20260821", "20260821"],
        }
    )

    processed = task.process_data(raw)

    assert processed["ts_code"].tolist() == ["IO2608-C-4600.CFX", "10000001.SH"]
