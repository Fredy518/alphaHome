"""Fetch option daily basic extension fields."""

import tinydata as td


td.configure(timeout_ms=60000)

df = td.option_basic_daily_ext(
    codes=["10000001.SH"],
    trade_date="20240531",
)
print(df.head())
