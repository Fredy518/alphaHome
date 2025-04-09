import tushare as ts
pro = ts.pro_api()
df = pro.index_daily(ts_code='000906.SH', start_date='20230101', end_date='20231031')
df = (
    df[['trade_date', 'open', 'high', 'low', 'close', 'vol']]
    .sort_values(by='trade_date', ascending=False)
    .reset_index(drop=True)
)

print(df)

