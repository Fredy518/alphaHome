"""Fetch FOF holding details for a report period."""

import tinydata as td


td.configure(timeout_ms=60000)

df = td.fund_fof_holding_detail(
    codes=["012345.OF"],
    report_period="20231231",
)
print(df.head())
