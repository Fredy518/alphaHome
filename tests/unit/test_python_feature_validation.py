from datetime import date, timedelta
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


@pytest.mark.asyncio
async def test_stock_sma_compute_keeps_security_windows_independent():
    first_day = date(2026, 7, 1)
    rows = []
    for offset in range(21):
        trade_date = first_day + timedelta(days=offset)
        rows.append({"ts_code": "A", "trade_date": trade_date, "close": offset + 1})
        rows.append({"ts_code": "B", "trade_date": trade_date, "close": 101 + offset})

    class FakeDB:
        async def fetch(self, sql):
            return rows

    result = await StockSmaDailyFeature(FakeDB()).compute("20260701", "20260721")

    assert len(result) == 4
    assert result.groupby("ts_code").size().to_dict() == {"A": 2, "B": 2}
    latest = result.sort_values("trade_date").groupby("ts_code").tail(1).set_index("ts_code")
    assert latest.loc["A", "sma20"] == pytest.approx(11.5)
    assert latest.loc["B", "sma20"] == pytest.approx(111.5)
