from __future__ import annotations

import pandas as pd
import pytest

from tinydata.errors import TinyDataQueryError
from tinydata.infotable import InfoTableOptions, query_infotable


class FakeClient:
    def __init__(self):
        self.queries = []

    def exec(self, tsl_code, *, as_dataframe=True, timeout_ms=None):
        self.queries.append(tsl_code)
        if "infotable 999" in tsl_code:
            raise TinyDataQueryError("select 查询的股票为空")
        if "array(" in tsl_code:
            return pd.DataFrame({"value": [1]})
        if "'SZ000001'" in tsl_code:
            return pd.DataFrame({"StockID": ["SZ000001"], "value": [10]})
        if "'SH600000'" in tsl_code:
            return pd.DataFrame({"StockID": ["SH600000"], "value": [20]})
        return pd.DataFrame({"StockID": ["SZ000001"], "value": [1]})


def test_query_infotable_batch_falls_back_when_identifier_missing():
    client = FakeClient()
    df = query_infotable(
        client,
        10,
        codes=["000001.SZ", "600000.SH"],
        options=InfoTableOptions(code_batch_size=100, retries=1),
    )
    assert list(df["StockID"]) == ["SZ000001", "SH600000"]
    assert any("array(" in q for q in client.queries)
    assert any("'SZ000001'" in q for q in client.queries)


def test_query_infotable_full_table_stock_empty_message():
    client = FakeClient()
    with pytest.raises(TinyDataQueryError, match="requires codes"):
        query_infotable(client, 999, allow_full_table=True, options=InfoTableOptions(retries=1))
