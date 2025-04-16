import asyncio
import logging
import sys
import os
import argparse
from datetime import datetime, timedelta
import dotenv
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from data_module.task_factory import TaskFactory

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 使用脚本名称作为logger名称
logger = logging.getLogger('tushare_stock_daily_updater')

async def main():
    # ---- 恢复命令行参数解析 ----
    parser = argparse.ArgumentParser(description='Tushare 股票日线数据更新工具 (支持全量和增量)')
    parser.add_argument('--days', type=int, help='增量更新: 更新最近 N 个交易日的数据')
    parser.add_argument('--start-date', help='增量或范围更新: 更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='增量或范围更新: 更新结束日期 (格式: YYYYMMDD, 默认为当天)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式: 从 19901219 (Tushare最早日期) 开始更新')
    parser.add_argument('--auto', action='store_true', help='自动增量模式: 从数据库最新日期的下一个交易日开始更新 (默认模式)')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')
    args = parser.parse_args()
    # ---- 结束参数解析 ----

    start_time = datetime.now()
    logger.info(f"开始执行 tushare_stock_daily 数据更新...")

    # 初始化 TaskFactory (连接数据库等)
    await TaskFactory.initialize()
    logger.info("TaskFactory 初始化成功")

    task = None # 初始化 task 变量以备 finally 使用
    try:
        task_name = "tushare_stock_daily"
        task = await TaskFactory.get_task(task_name)
        logger.info(f"任务 '{task_name}' 获取成功")

        # 定义要传递给任务的通用参数 (不含 concurrent_limit)
        common_kwargs = {
            'show_progress': args.show_progress
        }

        update_mode_desc = "默认自动增量" # Default description
        # 将 run_args 初始化为默认自动模式，除非被覆盖
        run_args = {'update_mode': 'trade_day', 'end_date': args.end_date} 

        # ---- 恢复更新模式逻辑 ----
        if args.full_update:
            update_mode_desc = "全量"
            # Tushare 日线数据最早日期为 19901219
            run_args = {'start_date': '19901219', 'end_date': args.end_date or datetime.now().strftime('%Y%m%d')}
        elif args.start_date:
            update_mode_desc = "指定日期范围"
            run_args = {'start_date': args.start_date, 'end_date': args.end_date or datetime.now().strftime('%Y%m%d')}
        elif args.days:
            update_mode_desc = f"最近 {args.days} 交易日增量"
            # 对于按天数回溯，需要传递 trade_days_lookback
            run_args = {'update_mode': 'trade_day', 'trade_days_lookback': args.days, 'end_date': args.end_date}
        elif args.auto:
            # 明确指定自动模式（虽然是默认，但处理一下）
            update_mode_desc = "自动增量"
            run_args = {'update_mode': 'trade_day', 'end_date': args.end_date}
        # 如果没有指定任何模式，默认使用 run_args 中初始化的自动模式
        # ---- 结束更新模式逻辑 ----

        logger.info(f"模式: {update_mode_desc} 更新")
        
        result = await task.execute(**run_args, **common_kwargs) # 调用 task 实例的 execute
        logger.info(f"{update_mode_desc} 更新结果: {result}")

    except Exception as e:
        task_name_log = task.name if task else task_name # 记录任务名
        logger.error(f"任务 '{task_name_log}' 执行过程中发生错误: {str(e)}", exc_info=True)
    finally:
        # 关闭 TaskFactory (断开数据库连接等)
        await TaskFactory.shutdown()
        logger.info("TaskFactory 已关闭")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"tushare_stock_daily 数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    asyncio.run(main()) 