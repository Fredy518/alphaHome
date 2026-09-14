from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from alphahome.features.recipes.python.stock_sma_daily import StockSmaDailyFeature


@pytest.mark.parametrize("frame", [
    None, pd.DataFrame(),
    pd.DataFrame({"ts_code": ["A"], "trade_date": [None]}),
    pd.DataFrame({"ts_code": ["A"], "trade_date": ["2026-09-12"]}),
    pd.DataFrame({"ts_code": [None], "trade_date": ["2026-09-11"]}),
    pd.DataFrame({"ts_code": ["A", "A"], "trade_date": ["2026-09-11"] * 2}),
])
def test_invalid_compute_result_cannot_replace_a_window(frame):
    with pytest.raises(ValueError):
        StockSmaDailyFeature()._validate_frame(frame, date(2026, 9, 1), date(2026, 9, 11))


@pytest.mark.asyncio
async def test_unknown_strategy_is_rejected_before_connection():
    feature = StockSmaDailyFeature(SimpleNamespace(connection_string="not-used"))
    with pytest.raises(ValueError, match="strategy"):
        await feature.refresh("typo")
