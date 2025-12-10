"""
补齐 tushare.stock_limitlist 表的 up_stat 字段 - V2 改进版

up_stat 字段含义：涨停统计（N/T）
- N: 涨停次数
- T: 交易日跨度（从序列第一次涨停到当前）

改进：
1. 支持非连续涨停序列（中间有间隔的情况）
2. 使用窗口期来判断是否属于同一波行情
"""

import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import DBManager

TABLE_NAME = "tushare.stock_limitlist"

# 序列中断阈值：如果两次涨停间隔超过此交易日数，则认为是新序列
SEQUENCE_BREAK_THRESHOLD = 10


async def analyze_existing_patterns(db):
    """深入分析现有 up_stat 数据，理解 T 的计算规则"""
    print("=" * 70)
    print("深入分析现有 up_stat 数据模式")
    print("=" * 70)
    
    # 分析 N != T 的情况（非连续涨停）
    query = """
    WITH parsed AS (
        SELECT 
            ts_code, trade_date, up_stat,
            SPLIT_PART(up_stat, '/', 1)::int as N,
            SPLIT_PART(up_stat, '/', 2)::int as T
        FROM tushare.stock_limitlist
        WHERE "limit" = 'U' 
          AND up_stat IS NOT NULL 
          AND up_stat != ''
          AND up_stat LIKE '%/%'
    )
    SELECT 
        CASE 
            WHEN N = T THEN '连续涨停(N=T)'
            WHEN N < T THEN '非连续涨停(N<T)'
            ELSE '异常(N>T)'
        END as pattern,
        COUNT(*) as count
    FROM parsed
    GROUP BY 1
    ORDER BY count DESC;
    """
    patterns = await db.fetch(query)
    
    print("\n涨停模式分布:")
    for p in patterns:
        print(f"  {p['pattern']:20} : {p['count']:,}")
    
    # 分析非连续涨停的具体案例
    print("\n非连续涨停案例（N < T）:")
    case_query = """
    WITH parsed AS (
        SELECT 
            ts_code, trade_date, up_stat,
            SPLIT_PART(up_stat, '/', 1)::int as N,
            SPLIT_PART(up_stat, '/', 2)::int as T
        FROM tushare.stock_limitlist
        WHERE "limit" = 'U' 
          AND up_stat IS NOT NULL 
          AND up_stat != ''
          AND up_stat LIKE '%/%'
    )
    SELECT ts_code, trade_date, up_stat, N, T, T - N as gap
    FROM parsed
    WHERE N < T
    ORDER BY gap DESC, trade_date DESC
    LIMIT 20;
    """
    cases = await db.fetch(case_query)
    
    print(f"  {'股票':12} | {'日期':12} | {'up_stat':10} | {'N':>3} | {'T':>3} | {'间隔':>4}")
    print("  " + "-" * 55)
    for c in cases:
        print(f"  {c['ts_code']:12} | {c['trade_date']} | {c['up_stat']:10} | {c['n']:>3} | {c['t']:>3} | {c['gap']:>4}")
    
    return patterns


async def get_stock_limit_history(db, ts_code):
    """获取单只股票的完整涨停历史"""
    query = f"""
    SELECT trade_date, "limit", up_stat
    FROM {TABLE_NAME}
    WHERE ts_code = $1
    ORDER BY trade_date;
    """
    return await db.fetch(query, ts_code)


async def verify_calculation_logic(db):
    """验证计算逻辑是否正确"""
    print("\n" + "=" * 70)
    print("验证计算逻辑")
    print("=" * 70)
    
    # 获取交易日列表
    trading_dates_query = """
    SELECT DISTINCT trade_date FROM tushare.stock_limitlist ORDER BY trade_date;
    """
    trading_dates = await db.fetch(trading_dates_query)
    trading_dates_list = [r['trade_date'] for r in trading_dates]
    date_to_idx = {d: i for i, d in enumerate(trading_dates_list)}
    
    # 选择一个有复杂涨停模式的股票进行验证
    test_stock = '000004.SZ'
    history = await get_stock_limit_history(db, test_stock)
    
    print(f"\n验证股票: {test_stock}")
    print(f"  {'日期':12} | {'limit':6} | {'实际up_stat':12} | {'计算up_stat':12} | {'匹配':4}")
    print("  " + "-" * 60)
    
    # 构建涨停日集合
    up_dates = set()
    for r in history:
        if r['limit'] == 'U':
            up_dates.add(r['trade_date'])
    
    # 计算每个涨停日的 up_stat
    sequence_start_idx = None
    up_count_in_sequence = 0
    last_up_idx = None
    
    for r in history:
        if r['limit'] != 'U':
            continue
        
        trade_date = r['trade_date']
        actual_up_stat = r['up_stat'] or ''
        
        if trade_date not in date_to_idx:
            continue
        
        current_idx = date_to_idx[trade_date]
        
        # 判断是否开始新序列
        if last_up_idx is None or (current_idx - last_up_idx) > SEQUENCE_BREAK_THRESHOLD:
            # 新序列
            sequence_start_idx = current_idx
            up_count_in_sequence = 1
        else:
            # 继续当前序列
            up_count_in_sequence += 1
        
        T = current_idx - sequence_start_idx + 1
        N = up_count_in_sequence
        calculated_up_stat = f"{N}/{T}"
        
        match = '✓' if calculated_up_stat == actual_up_stat else '✗'
        
        if actual_up_stat:  # 只显示有实际值的记录
            print(f"  {trade_date} | {r['limit']:6} | {actual_up_stat:12} | {calculated_up_stat:12} | {match:4}")
        
        last_up_idx = current_idx


async def fill_up_stat_v2(db):
    """
    V2版本：支持非连续涨停序列
    
    规则：
    - 如果两次涨停间隔 <= SEQUENCE_BREAK_THRESHOLD 个交易日，属于同一序列
    - T = 从序列第一次涨停到当前的交易日数
    - N = 序列内涨停次数
    """
    print("\n" + "=" * 70)
    print("开始计算缺失的 up_stat 值 (V2)")
    print("=" * 70)
    
    # 获取所有交易日
    trading_dates_query = """
    SELECT DISTINCT trade_date FROM tushare.stock_limitlist ORDER BY trade_date;
    """
    trading_dates = await db.fetch(trading_dates_query)
    trading_dates_list = [r['trade_date'] for r in trading_dates]
    date_to_idx = {d: i for i, d in enumerate(trading_dates_list)}
    
    print(f"  交易日数量: {len(trading_dates_list)}")
    print(f"  序列中断阈值: {SEQUENCE_BREAK_THRESHOLD} 个交易日")
    
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
            
            # 如果已有 up_stat，跳过
            if r['up_stat'] and r['up_stat'].strip():
                # 但仍需更新序列状态
                if trade_date in date_to_idx:
                    current_idx = date_to_idx[trade_date]
                    if last_up_idx is None or (current_idx - last_up_idx) > SEQUENCE_BREAK_THRESHOLD:
                        sequence_start_idx = current_idx
                        up_count_in_sequence = 1
                    else:
                        up_count_in_sequence += 1
                    last_up_idx = current_idx
                continue
            
            if trade_date not in date_to_idx:
                continue
            
            current_idx = date_to_idx[trade_date]
            
            # 判断是否开始新序列
            if last_up_idx is None or (current_idx - last_up_idx) > SEQUENCE_BREAK_THRESHOLD:
                sequence_start_idx = current_idx
                up_count_in_sequence = 1
            else:
                up_count_in_sequence += 1
            
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
    
    return results


async def compare_versions(db):
    """比较 V1 和 V2 版本的差异"""
    print("\n" + "=" * 70)
    print("比较简化版(V1)和改进版(V2)的差异")
    print("=" * 70)
    
    # 获取交易日
    trading_dates_query = """
    SELECT DISTINCT trade_date FROM tushare.stock_limitlist ORDER BY trade_date;
    """
    trading_dates = await db.fetch(trading_dates_query)
    trading_dates_list = [r['trade_date'] for r in trading_dates]
    date_to_idx = {d: i for i, d in enumerate(trading_dates_list)}
    
    # 获取缺失记录
    missing_query = f"""
    SELECT ts_code, trade_date
    FROM {TABLE_NAME}
    WHERE "limit" = 'U' AND (up_stat IS NULL OR up_stat = '')
    ORDER BY ts_code, trade_date;
    """
    missing_records = await db.fetch(missing_query)
    
    # 获取所有涨停记录
    all_up_query = f"""
    SELECT ts_code, trade_date
    FROM {TABLE_NAME}
    WHERE "limit" = 'U'
    ORDER BY ts_code, trade_date;
    """
    all_up_records = await db.fetch(all_up_query)
    
    # 构建股票涨停日集合
    stock_up_dates = defaultdict(set)
    for r in all_up_records:
        stock_up_dates[r['ts_code']].add(r['trade_date'])
    
    # V1: 简化版（只计算连续涨停）
    v1_results = {}
    for record in missing_records:
        ts_code = record['ts_code']
        trade_date = record['trade_date']
        
        if trade_date not in date_to_idx:
            continue
        
        current_idx = date_to_idx[trade_date]
        up_dates = stock_up_dates[ts_code]
        
        N = 1
        T = 1
        check_idx = current_idx - 1
        while check_idx >= 0:
            check_date = trading_dates_list[check_idx]
            if check_date in up_dates:
                N += 1
                T += 1
                check_idx -= 1
            else:
                break
        
        v1_results[(ts_code, trade_date)] = f"{N}/{T}"
    
    # V2: 改进版（支持非连续）
    stock_records = defaultdict(list)
    for r in missing_records:
        stock_records[r['ts_code']].append(r)
    
    v2_results = {}
    for ts_code, records in stock_records.items():
        up_dates = stock_up_dates[ts_code]
        sorted_up_dates = sorted([d for d in up_dates if d in date_to_idx], key=lambda x: date_to_idx[x])
        
        sequence_start_idx = None
        up_count_in_sequence = 0
        last_up_idx = None
        
        for up_date in sorted_up_dates:
            current_idx = date_to_idx[up_date]
            
            if last_up_idx is None or (current_idx - last_up_idx) > SEQUENCE_BREAK_THRESHOLD:
                sequence_start_idx = current_idx
                up_count_in_sequence = 1
            else:
                up_count_in_sequence += 1
            
            T = current_idx - sequence_start_idx + 1
            N = up_count_in_sequence
            
            # 只记录缺失的
            if any(r['trade_date'] == up_date for r in records):
                v2_results[(ts_code, up_date)] = f"{N}/{T}"
            
            last_up_idx = current_idx
    
    # 比较差异
    diff_count = 0
    same_count = 0
    
    for key in v1_results:
        if key in v2_results:
            if v1_results[key] != v2_results[key]:
                diff_count += 1
            else:
                same_count += 1
    
    print(f"  相同结果: {same_count:,}")
    print(f"  不同结果: {diff_count:,}")
    print(f"  差异比例: {100 * diff_count / (same_count + diff_count):.2f}%")
    
    # 显示一些差异案例
    if diff_count > 0:
        print("\n  差异案例（前10个）:")
        print(f"    {'股票':12} | {'日期':12} | {'V1':10} | {'V2':10}")
        print("    " + "-" * 50)
        
        shown = 0
        for key in v1_results:
            if key in v2_results and v1_results[key] != v2_results[key]:
                print(f"    {key[0]:12} | {key[1]} | {v1_results[key]:10} | {v2_results[key]:10}")
                shown += 1
                if shown >= 10:
                    break


async def preview_and_update(db, results, dry_run=True):
    """预览并更新数据库"""
    # 预览
    print("\n" + "=" * 70)
    print(f"预览计算结果（前 20 条）")
    print("=" * 70)
    
    print(f"  {'股票':12} | {'日期':12} | {'计算的up_stat':12}")
    print("  " + "-" * 45)
    
    for r in results[:20]:
        print(f"  {r['ts_code']:12} | {r['trade_date']} | {r['up_stat']:12}")
    
    # 统计分布
    print("\n" + "=" * 70)
    print("计算结果分布")
    print("=" * 70)
    
    from collections import Counter
    up_stat_counts = Counter(r['up_stat'] for r in results)
    print("  up_stat 值分布（前20）:")
    for up_stat, count in up_stat_counts.most_common(20):
        print(f"    {up_stat:10} : {count:,}")
    
    # 更新
    if dry_run:
        print("\n" + "=" * 70)
        print("DRY RUN 模式 - 不会实际更新数据库")
        print("=" * 70)
        print(f"  将更新 {len(results):,} 条记录")
    else:
        print("\n" + "=" * 70)
        print("开始更新数据库")
        print("=" * 70)
        
        batch_size = 1000
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
            print(f"  已更新: {updated:,} / {len(results):,}")
        
        print(f"\n  更新完成！共更新 {updated:,} 条记录")


async def main():
    db_url = get_database_url()
    if not db_url:
        print("错误：无法获取数据库连接字符串")
        return
    
    db = DBManager(db_url, mode='async')
    await db.connect()
    
    try:
        # 1. 分析现有数据模式
        await analyze_existing_patterns(db)
        
        # 2. 验证计算逻辑
        await verify_calculation_logic(db)
        
        # 3. 比较两个版本
        await compare_versions(db)
        
        # 4. 使用 V2 计算
        results = await fill_up_stat_v2(db)
        
        # 5. 预览和更新
        await preview_and_update(db, results, dry_run=True)
        
        print("\n" + "=" * 70)
        print("如需实际更新数据库，请修改 dry_run=False")
        print("=" * 70)
        
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
