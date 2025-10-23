#!/usr/bin/env python3
"""
测试脚本：执行pytdx_stock_daily任务，获取并保存2025/1/1以来的全市场日线数据

此脚本专门用于测试pytdx_stock_daily.py的功能，包括：
1. 初始化数据库连接
2. 创建pytdx_stock_daily任务实例
3. 执行全市场日线数据获取任务
4. 验证数据保存结果
"""

import asyncio
import logging
import sys
import os
from datetime import datetime

# 添加项目路径
sys.path.insert(0, 'alphahome')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def test_pytdx_stock_daily_full():
    """执行完整的pytdx_stock_daily任务测试"""

    print("=" * 60)
    print("开始测试: pytdx_stock_daily 全市场日线数据获取")
    print("=" * 60)

    try:
        # 1. 初始化UnifiedTaskFactory
        print("步骤1: 初始化UnifiedTaskFactory...")
        from alphahome.common.task_system import UnifiedTaskFactory

        await UnifiedTaskFactory.initialize()
        if not UnifiedTaskFactory._initialized:
            raise RuntimeError("UnifiedTaskFactory初始化失败")

        print("[OK] UnifiedTaskFactory初始化成功")

        # 2. 创建pytdx_stock_daily任务实例
        print("\n步骤2: 创建pytdx_stock_daily任务实例...")
        task_name = "pytdx_stock_daily"

        # 设置任务参数 - 只测试几只股票用于调试
        task_params = {
            "update_type": "manual",  # 手动指定日期范围
            "start_date": "20250101",  # 2025年1月1日
            "end_date": datetime.now().strftime("%Y%m%d"),  # 今天
            # 添加调试参数：只处理前3只股票
            "debug_limit": 3
        }

        task_instance = await UnifiedTaskFactory.create_task_instance(
            task_name,
            **task_params
        )

        print(f"[OK] 任务实例创建成功: {task_instance.name}")
        print(f"   数据源: {task_instance.data_source}")
        print(f"   表名: {task_instance.table_name}")
        print(f"   日期范围: {task_params['start_date']} 到 {task_params['end_date']}")

        # 3. 执行任务
        print(f"\n步骤3: 执行任务 (获取2025/1/1以来的全市场日线数据)...")
        print("[WARNING] 注意: 这将连接通达信服务器并下载大量数据，可能需要几分钟时间")

        start_time = datetime.now()
        result = await task_instance.execute()
        end_time = datetime.now()

        execution_time = (end_time - start_time).total_seconds()

        print("[OK] 任务执行完成")
        print(".2f")
        print(f"   状态: {result.get('status', 'unknown')}")
        print(f"   影响行数: {result.get('rows', 0)}")

        # 4. 验证结果
        print("\n步骤4: 验证数据保存结果...")

        if result.get('status') == 'success' and result.get('rows', 0) > 0:
            # 查询保存的数据
            try:
                db = task_instance.db

                # 查询总记录数
                total_count_query = f"SELECT COUNT(*) as total FROM {task_instance.get_full_table_name()}"
                total_result = await db.fetch_one(total_count_query)
                total_count = total_result['total'] if total_result else 0

                # 查询日期范围
                date_range_query = f"""
                    SELECT
                        MIN(trade_date) as min_date,
                        MAX(trade_date) as max_date,
                        COUNT(DISTINCT ts_code) as stock_count
                    FROM {task_instance.get_full_table_name()}
                    WHERE trade_date >= '2025-01-01'
                """
                date_result = await db.fetch_one(date_range_query)

                print("[OK] 数据验证成功:")
                print(f"   总记录数: {total_count}")
                if date_result:
                    print(f"   股票数量: {date_result['stock_count']}")
                    print(f"   日期范围: {date_result['min_date']} 到 {date_result['max_date']}")
                else:
                    print("   未找到2025年以来的数据")

                # 显示最近几条记录作为样例
                sample_query = f"""
                    SELECT ts_code, trade_date, open, high, low, close, volume, amount
                    FROM {task_instance.get_full_table_name()}
                    WHERE trade_date >= '2025-01-01'
                    ORDER BY trade_date DESC, ts_code
                    LIMIT 5
                """
                sample_data = await db.fetch(sample_query)

                if sample_data:
                    print("\n   最新数据样例:")
                    for row in sample_data:
                        print(f"     {row['ts_code']} {row['trade_date']} O:{row['open']} H:{row['high']} L:{row['low']} C:{row['close']} V:{row['volume']}")

            except Exception as e:
                print(f"[ERROR] 数据验证失败: {e}")
        else:
            print("[ERROR] 任务执行失败或未保存数据")

        print("\n" + "=" * 60)
        print("测试完成总结:")
        print(f"- 执行时间: {execution_time:.2f}秒")
        print(f"- 处理状态: {result.get('status', 'unknown')}")
        print(f"- 数据行数: {result.get('rows', 0)}")
        print("=" * 60)

    except Exception as e:
        print(f"[ERROR] 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()

        print("\n" + "=" * 60)
        print("测试失败")
        print("=" * 60)

async def test_pytdx_stock_daily_simulation():
    """模拟执行pytdx_stock_daily任务（不实际下载数据）"""

    print("=" * 60)
    print("模拟测试: pytdx_stock_daily 全市场日线数据获取")
    print("=" * 60)

    try:
        # 1. 初始化UnifiedTaskFactory
        print("步骤1: 初始化UnifiedTaskFactory...")
        from alphahome.common.task_system import UnifiedTaskFactory

        await UnifiedTaskFactory.initialize()
        if not UnifiedTaskFactory._initialized:
            raise RuntimeError("UnifiedTaskFactory初始化失败")

        print("[OK] UnifiedTaskFactory初始化成功")

        # 2. 创建pytdx_stock_daily任务实例
        print("\n步骤2: 创建pytdx_stock_daily任务实例...")
        task_name = "pytdx_stock_daily"

        task_params = {
            "update_type": "manual",
            "start_date": "20250101",
            "end_date": "20250105",  # 只测试几天的数据
        }

        task_instance = await UnifiedTaskFactory.create_task_instance(
            task_name,
            **task_params
        )

        print(f"[OK] 任务实例创建成功: {task_instance.name}")
        print(f"   数据源: {task_instance.data_source}")
        print(f"   表名: {task_instance.table_name}")

        # 3. 测试批次生成（不实际执行数据获取）
        print("\n步骤3: 测试批次生成...")
        batches = await task_instance.get_batch_list(**task_params)

        print(f"[OK] 批次生成成功: {len(batches)} 个批次")

        if batches:
            print(f"   示例批次: 股票代码 {batches[0].get('code')}, 市场 {batches[0].get('market')}")

        # 4. 显示任务配置信息
        print("\n步骤4: 任务配置信息")
        print(f"   数据源: {task_instance.data_source}")
        print(f"   表名: {task_instance.table_name}")
        print(f"   主键: {task_instance.primary_keys}")
        print(f"   日期列: {task_instance.date_column}")
        print(f"   默认起始日期: {task_instance.default_start_date}")

        print("\n" + "=" * 60)
        print("模拟测试完成 - 任务配置正确")
        print("如需实际执行数据下载，请使用完整测试模式")
        print("=" * 60)

    except Exception as e:
        print(f"[ERROR] 模拟测试过程中出错: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """主函数"""
    import sys

    print("pytdx_stock_daily 全市场日线数据获取测试")
    print()

    # 检查命令行参数
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        print("选择测试模式:")
        print("1. 模拟测试 - 只验证任务配置和批次生成（推荐）")
        print("2. 完整测试 - 实际连接通达信服务器下载数据（耗时较长）")
        print()

        choice = input("请选择 (1/2): ").strip()

    if choice == "1":
        await test_pytdx_stock_daily_simulation()
    elif choice == "2":
        print("完整测试将连接通达信服务器下载2025年以来的全市场日线数据")
        if len(sys.argv) > 2 and sys.argv[2] == "y":
            response = "y"
        else:
            response = input("[WARNING] 此操作将下载大量数据，可能需要几分钟时间。是否继续？(y/N): ")
        if response.lower() in ['y', 'yes']:
            await test_pytdx_stock_daily_full()
        else:
            print("测试取消")
    else:
        print("无效选择，退出测试")
        print("使用方法: python test_pytdx_stock_daily_full.py [1|2] [y]")

if __name__ == "__main__":
    asyncio.run(main())
