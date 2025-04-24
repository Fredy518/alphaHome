#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查tushare_fund_daily表中是否有交易日缺失
"""

import asyncio
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import dotenv
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('check_fund_daily')

# 设置路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 导入
from data_module.task_factory import TaskFactory
from data_module.tools.calendar import get_trade_cal


async def check_missing_dates():
    """检查是否有交易日缺失"""
    print("开始检查tushare_fund_daily表交易日缺失情况...")

    # 初始化TaskFactory
    await TaskFactory.initialize()
    db_manager = TaskFactory.get_db_manager()  # 使用get_db_manager方法

    try:
        # 步骤1: 获取交易日历中的所有交易日
        start_date = "20100101"  # 设置一个相对较早的起始日期
        end_date = datetime.now().strftime("%Y%m%d")
        
        print(f"获取交易日历数据 ({start_date} 到 {end_date})...")
        calendar_df = await get_trade_cal(start_date=start_date, end_date=end_date)
        
        # 过滤出交易日（is_open=1）
        trade_dates = calendar_df[calendar_df['is_open'] == 1]['cal_date'].tolist()
        print(f"交易日历中共有 {len(trade_dates)} 个交易日")

        # 步骤2: a. 获取所有基金代码以确定范围
        query_codes = """
        SELECT DISTINCT ts_code FROM tushare_fund_daily
        ORDER BY ts_code
        """
        fund_codes = await db_manager.fetch(query_codes)
        fund_codes = [code[0] for code in fund_codes]
        print(f"tushare_fund_daily表中共有 {len(fund_codes)} 个基金代码")
        
        if len(fund_codes) == 0:
            print("表中没有数据，请先更新数据")
            return
        
        # 随机选择30个基金代码进行详细检查
        import random
        selected_codes = random.sample(fund_codes, min(30, len(fund_codes)))
        print(f"随机选择 {len(selected_codes)} 个基金代码进行详细检查...")
        
        # 步骤2: b. 查询基金交易日数据
        results = []
        
        for i, code in enumerate(selected_codes):
            print(f"检查基金 {i+1}/{len(selected_codes)}: {code}", end="\r")
            query_dates = f"""
            SELECT DISTINCT trade_date 
            FROM tushare_fund_daily 
            WHERE ts_code = '{code}'
            ORDER BY trade_date
            """
            fund_dates = await db_manager.fetch(query_dates)
            fund_dates = [date[0].strftime("%Y%m%d") if hasattr(date[0], 'strftime') else date[0] for date in fund_dates]
            
            # 获取该基金第一个和最后一个交易日
            if fund_dates:
                first_date = fund_dates[0]
                last_date = fund_dates[-1]
                
                # 筛选交易日历中该基金存在期间的交易日
                expected_dates = [d for d in trade_dates if first_date <= d <= last_date]
                
                # 步骤3: 找出缺失的日期
                missing_dates = set(expected_dates) - set(fund_dates)
                
                # 计算缺失率
                missing_ratio = len(missing_dates) / len(expected_dates) if expected_dates else 0
                
                results.append({
                    "ts_code": code,
                    "first_date": first_date,
                    "last_date": last_date,
                    "expected_dates": len(expected_dates),
                    "actual_dates": len(fund_dates),
                    "missing_dates": len(missing_dates),
                    "missing_ratio": missing_ratio,
                    "missing_dates_list": sorted(list(missing_dates))[:10]  # 只显示前10个缺失日期
                })
        
        print("\n\n基金交易日检查结果:")
        print("-" * 80)
        
        # 先按缺失率排序
        results.sort(key=lambda x: x['missing_ratio'], reverse=True)
        
        for result in results:
            print(f"基金代码: {result['ts_code']}")
            print(f"数据范围: {result['first_date']} 到 {result['last_date']}")
            print(f"应有交易日: {result['expected_dates']}, 实际交易日: {result['actual_dates']}")
            missing_count = result['missing_dates']
            if missing_count > 0:
                print(f"缺失交易日数量: {missing_count} ({result['missing_ratio']:.2%})")
                if result['missing_dates_list']:
                    missing_dates_str = ", ".join(result['missing_dates_list'])
                    if missing_count > 10:
                        missing_dates_str += " ..."
                    print(f"部分缺失日期: {missing_dates_str}")
            else:
                print("无缺失交易日")
            print("-" * 80)
        
        # 统计整体情况
        complete_count = sum(1 for r in results if r['missing_dates'] == 0)
        missing_count = len(results) - complete_count
        high_missing_count = sum(1 for r in results if r['missing_ratio'] > 0.5)
        
        print("\n统计信息:")
        print(f"检查的基金数量: {len(results)}")
        print(f"数据完整的基金数量: {complete_count} ({complete_count/len(results):.2%})")
        print(f"存在缺失的基金数量: {missing_count} ({missing_count/len(results):.2%})")
        print(f"缺失率超过50%的基金数量: {high_missing_count} ({high_missing_count/len(results):.2%})")
        
        # 计算平均缺失率
        avg_missing_ratio = sum(r['missing_ratio'] for r in results) / len(results)
        print(f"平均缺失率: {avg_missing_ratio:.2%}")

    except Exception as e:
        print(f"检查过程中发生错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        # 关闭连接
        await TaskFactory.shutdown()
        print("检查完成")


if __name__ == "__main__":
    asyncio.run(check_missing_dates()) 