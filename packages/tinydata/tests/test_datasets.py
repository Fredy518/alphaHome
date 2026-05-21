from __future__ import annotations

import pandas as pd

from tinydata.datasets.fund import FUND_FOF_HOLDING_DETAIL
from tinydata.datasets.specs import process_dataset_frame


def test_process_dataset_frame_maps_fields_and_types():
    raw = pd.DataFrame(
        {
            "StockID": ["OF012345"],
            "截止日": ["20231231"],
            "代码": ["OF000001"],
            "数量": ["100.5"],
            "市值排名": ["1"],
            "未映射中文列": ["drop-me"],
        }
    )

    out = process_dataset_frame(raw, FUND_FOF_HOLDING_DETAIL)
    assert out.loc[0, "ts_code"] == "012345.OF"
    assert out.loc[0, "report_date"].isoformat() == "2023-12-31"
    assert out.loc[0, "holding_code_raw"] == "OF000001"
    assert float(out.loc[0, "quantity"]) == 100.5
    assert int(out.loc[0, "rank_no"]) == 1
    assert out.loc[0, "source_table_id"] == 349
    assert "未映射中文列" not in out.columns
