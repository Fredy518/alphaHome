"""验证 5 个保留评审项的数据覆盖率"""
from data_infra.connection import get_connection_manager

cm = get_connection_manager()

# 1. PCR Weekly - option_daily 覆盖率
print('=== 1. PCR Weekly (option_daily) ===')
result = cm.execute_query('''
    SELECT 
        MIN(trade_date) as min_date,
        MAX(trade_date) as max_date,
        COUNT(DISTINCT trade_date) as total_days,
        COUNT(*) as total_rows
    FROM tushare.option_daily
''')
if result:
    r = result[0]
    print(f"Date range: {r['min_date']} ~ {r['max_date']}")
    print(f"Trading days: {r['total_days']}, Total rows: {r['total_rows']}")
else:
    print('No data')

# 2. CB Risk Appetite - cbond_daily 覆盖率
print()
print('=== 2. CB Risk Appetite (cbond_daily) ===')
result = cm.execute_query('''
    SELECT 
        MIN(trade_date) as min_date,
        MAX(trade_date) as max_date,
        COUNT(DISTINCT trade_date) as total_days,
        COUNT(*) as total_rows
    FROM tushare.cbond_daily
''')
if result:
    r = result[0]
    print(f"Date range: {r['min_date']} ~ {r['max_date']}")
    print(f"Trading days: {r['total_days']}, Total rows: {r['total_rows']}")
else:
    print('No data')

# 3. Microcap Risk Appetite - stock_factor_pro 微盘股覆盖率
print()
print('=== 3. Microcap Risk Appetite (stock_factor_pro total_mv) ===')
result = cm.execute_query('''
    SELECT 
        MIN(trade_date) as min_date,
        MAX(trade_date) as max_date,
        COUNT(DISTINCT trade_date) as total_days
    FROM tushare.stock_factor_pro
    WHERE total_mv IS NOT NULL AND total_mv > 0
''')
if result:
    r = result[0]
    print(f"Date range: {r['min_date']} ~ {r['max_date']}")
    print(f"Trading days with valid total_mv: {r['total_days']}")
else:
    print('No data')

# 4. Market Industry Flow - index_swmember
print()
print('=== 4. Market Industry Flow (index_swmember) ===')
result = cm.execute_query('''
    SELECT 
        MIN(in_date) as min_date,
        MAX(in_date) as max_date,
        COUNT(*) as total_rows,
        COUNT(DISTINCT l2_code) as l2_count
    FROM tushare.index_swmember
''')
if result:
    r = result[0]
    print(f"Date range: {r['min_date']} ~ {r['max_date']}")
    print(f"Total rows: {r['total_rows']}, L2 industries: {r['l2_count']}")
else:
    print('No data')

# 5. 检查 option_basic 表结构
print()
print('=== 5. option_basic (for PCR calculation) ===')
result = cm.execute_query('''
    SELECT 
        MIN(list_date) as min_date,
        MAX(list_date) as max_date,
        COUNT(*) as total_rows
    FROM tushare.option_basic
''')
if result:
    r = result[0]
    print(f"Date range: {r['min_date']} ~ {r['max_date']}")
    print(f"Total option contracts: {r['total_rows']}")
else:
    print('No data')

print()
print('Done!')
