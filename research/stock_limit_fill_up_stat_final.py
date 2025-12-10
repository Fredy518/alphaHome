"""
补齐 tushare.stock_limitlist 表的 up_stat 字段 - 最终版

up_stat 字段含义：涨停统计（N/T）
- N: 涨停次数
- T: 交易日跨度（从序列第一次涨停到当前）

规则（基于数据分析得出，准确率 97.79%）：
- 如果两次涨停间隔 <= 4 个交易日，则属于同一序列
- 如果间隔 > 4 个交易日，则重置为新序列，从 1/1 开始
"""

import asyncio
from collections import defaultdict, Counter
from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import DBManager

TABLE_NAME = "tushare.stock_limitlist"

# 序列中断阈值：如果两次涨停间隔超过此交易日数，则认为是新序列
SEQUENCE_BREAK_THRESHOLD = 4


async def fill_up_stat(db, dry_run=True):
    """
    补齐缺失的 up_stat 值
    
    规则：
    - 如果两次涨停间隔 <= 4 个交易日，属于同一序列
    - T = 从序列第一次涨停到当前的交易日数
    - N = 序列内涨停次数
    """
    print("=" * 70)
    print("补齐 up_stat 字段 - 最终版")
    print("=" * 70)
    print(f"  序列中断阈值: {SEQUENCE_BREAK_THRESHOLD} 个交易日")
    
    # 获取所有交易日
    trading_dates_query = """
    SELECT DISTINCT trade_date FROM tushare.stock_limitlist ORDER BY trade_date;
    """
    trading_dates = await db.fetch(trading_dates_query)
    trading_dates_list = [r['trade_date'] for r in trading_dates]
    date_to_idx = {d: i for i, d in enumerate(trading_dates_list)}
    
    print(f"  交易日数量: {len(trading_dates_list)}")
    
    # 获取所有涨停记录（按股票和日期排序）
    all_up_query = f"""
    SELECT ts_code, trade_date, up_stat
    FROM {TABLE_NAME}
    WHERE "limit" = 'U'
    ORDER BY ts_code, trade_date;
    """
    all_up_records = await db.fetch(all_up_query)
    print(f"  总涨停记录数: {len(all_up_records):,}")
    
    # 按股票分组处理
    stock_records = defaultdict(list)
    for r in all_up_records:
        stock_records[r['ts_code']].append(r)
    
    print(f"  涉及股票数: {len(stock_records):,}")
    
    results = []
    processed = 0
    
    for ts_code, records in stock_records.items():
        sequence_start_idx = None
        up_count_in_sequence = 0
        last_up_idx = None
        
        for r in records:
            trade_date = r['trade_date']
            
            if trade_date not in date_to_idx:
                continue
            
            current_idx = date_to_idx[trade_date]
            
            # 判断是否开始新序列
            if last_up_idx is None or (current_idx - last_up_idx) > SEQUENCE_BREAK_THRESHOLD:
                sequence_start_idx = current_idx
                up_count_in_sequence = 1
            else:
                up_count_in_sequence += 1
            
            # 如果已有 up_stat，跳过（但仍需更新序列状态）
            if r['up_stat'] and r['up_stat'].strip():
                last_up_idx = current_idx
                continue
            
            T = current_idx - sequence_start_idx + 1
            N = up_count_in_sequence
            up_stat = f"{N}/{T}"
            
            results.append({
                'ts_code': ts_code,
                'trade_date': trade_date,
                'up_stat': up_stat
            })
            
            last_up_idx = current_idx
        
        processed += 1
        if processed % 1000 == 0:
            print(f"  已处理股票: {processed:,} / {len(stock_records):,}")
    
    print(f"\n  计算完成，共 {len(results):,} 条记录需要更新")
    
    # 预览结果
    print("\n" + "=" * 70)
    print("预览计算结果（前 30 条）")
    print("=" * 70)
    
    print(f"  {'股票':12} | {'日期':12} | {'计算的up_stat':12}")
    print("  " + "-" * 45)
    
    for r in results[:30]:
        print(f"  {r['ts_code']:12} | {r['trade_date']} | {r['up_stat']:12}")
    
    # 统计分布
    print("\n" + "=" * 70)
    print("计算结果分布")
    print("=" * 70)
    
    up_stat_counts = Counter(r['up_stat'] for r in results)
    print("  up_stat 值分布（前25）:")
    for up_stat, count in up_stat_counts.most_common(25):
        print(f"    {up_stat:10} : {count:,}")
    
    # 按年份统计
    print("\n  按年份统计:")
    year_counts = Counter(r['trade_date'].year for r in results)
    for year in sorted(year_counts.keys()):
        print(f"    {year}: {year_counts[year]:,}")
    
    # 更新数据库
    if dry_run:
        print("\n" + "=" * 70)
        print("DRY RUN 模式 - 不会实际更新数据库")
        print("=" * 70)
        print(f"  将更新 {len(results):,} 条记录")
        print("\n  如需实际更新，请调用: fill_up_stat(db, dry_run=False)")
    else:
        print("\n" + "=" * 70)
        print("开始更新数据库")
        print("=" * 70)
        
        batch_size = 500
        updated = 0
        
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            
            for r in batch:
                update_query = f"""
                UPDATE {TABLE_NAME}
                SET up_stat = $1, update_time = NOW()
                WHERE ts_code = $2 AND trade_date = $3 AND "limit" = 'U';
                """
                await db.execute(update_query, r['up_stat'], r['ts_code'], r['trade_date'])
            
            updated += len(batch)
            if updated % 5000 == 0 or updated == len(results):
                print(f"  已更新: {updated:,} / {len(results):,}")
        
        print(f"\n  ✓ 更新完成！共更新 {updated:,} 条记录")
    
    return results


async def verify_results(db):
    """验证更新后的结果"""
    print("\n" + "=" * 70)
    print("验证更新结果")
    print("=" * 70)
    
    # 检查缺失情况
    query = f"""
    SELECT 
        CASE WHEN up_stat IS NULL OR up_stat = '' THEN '缺失' ELSE '有值' END as status,
        COUNT(*) as count
    FROM {TABLE_NAME}
    WHERE "limit" = 'U'
    GROUP BY 1;
    """
    result = await db.fetch(query)
    
    for r in result:
        print(f"  {r['status']:10} : {r['count']:,}")
    
    # 按年份检查
    yearly_query = f"""
    SELECT 
        EXTRACT(YEAR FROM trade_date) as year,
        COUNT(*) as total,
        SUM(CASE WHEN up_stat IS NULL OR up_stat = '' THEN 1 ELSE 0 END) as missing
    FROM {TABLE_NAME}
    WHERE "limit" = 'U'
    GROUP BY 1
    ORDER BY 1;
    """
    yearly = await db.fetch(yearly_query)
    
    print("\n  按年份统计:")
    print(f"    {'年份':8} | {'总数':>10} | {'缺失':>10}")
    print("    " + "-" * 35)
    for r in yearly:
        print(f"    {int(r['year']):8} | {r['total']:>10,} | {r['missing']:>10,}")


async def main():
    import sys
    
    # 检查命令行参数
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1] == '--execute':
        dry_run = False
        print("⚠️  执行模式：将实际更新数据库！")
    
    db_url = get_database_url()
    if not db_url:
        print("错误：无法获取数据库连接字符串")
        return
    
    db = DBManager(db_url, mode='async')
    await db.connect()
    
    try:
        # 补齐 up_stat
        await fill_up_stat(db, dry_run=dry_run)
        
        # 如果实际执行了更新，验证结果
        if not dry_run:
            await verify_results(db)
        
        if dry_run:
            print("\n" + "=" * 70)
            print("使用说明")
            print("=" * 70)
            print("""
  当前为 DRY RUN 模式，不会修改数据库。
  
  如需实际更新数据库，请运行：
    python research/stock_limit_fill_up_stat_final.py --execute
""")
        
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
