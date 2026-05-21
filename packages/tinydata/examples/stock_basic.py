"""Fetch stock basic extension fields."""

import tinydata as td


td.configure(timeout_ms=60000)

df = td.stock_basic_ext(codes=["000001.SZ", "600000.SH"])
print(df.head())
