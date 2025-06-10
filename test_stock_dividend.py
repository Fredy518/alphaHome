#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票分红送股数据任务测试脚本
"""

import sys
import os
import asyncio
from datetime import datetime, date

# 确保能找到 alphahome 模块
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

async def test_stock_dividend_task():
    """测试股票分红送股数据任务"""
    try:
        from alphahome.fetchers.tasks.stock.tushare_stock_dividend import TushareStockDividendTask
        
        print("开始测试股票分红送股数据任务...")
        
        # 创建数据库连接（这里用Mock，实际使用时需要真实连接）
        class MockDB:
            async def fetch_all(self, query):
                # 返回一些测试股票代码
                return [
                    {'ts_code': '000001.SZ'},
                    {'ts_code': '000002.SZ'},
                    {'ts_code': '600000.SH'},
                    {'ts_code': '600036.SH'},
                    {'ts_code': '600519.SH'}
                ]
        
        # 创建任务实例
        task = TushareStockDividendTask(db_connection=MockDB())
        task.db = MockDB()  # 设置数据库连接
        
        print(f"任务名称: {task.name}")
        print(f"API名称: {task.api_name}")
        print(f"表名: {task.table_name}")
        print(f"日期列: {task.date_column}")
        
        # 测试1: 全量模式批处理策略
        print("\n=== 测试1: 全量模式（按股票代码分批） ===")
        batch_list_full = await task.get_batch_list(
            force_full=True,
            start_date='20230101',
            end_date='20231231'
        )
        print(f"全量模式生成批次数: {len(batch_list_full)}")
        if batch_list_full:
            print(f"第一个批次示例: {batch_list_full[0]}")
        
        # 测试2: 增量模式批处理策略
        print("\n=== 测试2: 增量模式（按日期分批） ===")
        batch_list_inc = await task.get_batch_list(
            start_date='20231201',
            end_date='20231210'
        )
        print(f"增量模式生成批次数: {len(batch_list_inc)}")
        if batch_list_inc:
            print(f"第一个批次示例: {batch_list_inc[0]}")
        
        # 测试3: 参数准备
        print("\n=== 测试3: API参数准备 ===")
        if batch_list_full:
            prepared_params = task.prepare_params(batch_list_full[0])
            print(f"准备的API参数: {prepared_params}")
        
        # 测试4: 测试通用批处理工具
        print("\n=== 测试4: 通用股票代码批处理工具 ===")
        from alphahome.fetchers.tools.batch_utils import generate_stock_code_batches
        try:
            batch_list_codes = await generate_stock_code_batches(
                db_connection=MockDB(),
                logger=task.logger
            )
            print(f"通过工具函数生成批次数: {len(batch_list_codes)}")
            if batch_list_codes:
                print(f"第一个批次示例: {batch_list_codes[0]}")
        except Exception as e:
            print(f"通用工具测试出错: {e}")
        
        print("\n✅ 股票分红送股任务测试完成！")
        return True
        
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_stock_dividend_task())
    if not success:
        sys.exit(1) 