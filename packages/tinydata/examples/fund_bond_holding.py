"""Fetch fund bond holding details."""

import tinydata as td


td.configure(timeout_ms=60000)

df = td.fund_bond_holding(
    codes=["000001.OF"],
    start_date="20230101",
    end_date="20231231",
)
print(df.head())
