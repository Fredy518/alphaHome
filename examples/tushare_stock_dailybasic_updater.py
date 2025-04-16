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

# Load environment variables
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from data_module.task_factory import TaskFactory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Use the new script name as the logger name
logger = logging.getLogger('tushare_stock_dailybasic_updater')

async def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description='Tushare 股票每日基本指标更新工具')
    parser.add_argument('--days', type=int, help='更新最近 N 个交易日的数据')
    parser.add_argument('--start-date', help='更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='更新结束日期 (格式: YYYYMMDD, 默认为当天)')
    parser.add_argument('--full-update', action='store_true', help='全量更新模式')
    parser.add_argument('--auto', action='store_true', help='自动增量模式 (默认)')
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')

    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"开始执行 tushare_stock_dailybasic 数据更新...")

    await TaskFactory.initialize()
    logger.info("TaskFactory 初始化成功")

    task = None
    try:
        task_name = "tushare_stock_dailybasic"
        task = await TaskFactory.get_task(task_name)
        logger.info(f"任务 '{task_name}' 获取成功")

        common_kwargs = {
            'show_progress': args.show_progress
        }

        update_mode_desc = "默认自动增量"
        run_args = {'update_mode': 'trade_day', 'end_date': args.end_date}

        if args.full_update:
            update_mode_desc = "全量"
            run_args = {'start_date': '19910101', 'end_date': args.end_date or datetime.now().strftime('%Y%m%d')}
        elif args.start_date:
            update_mode_desc = "指定日期范围"
            run_args = {'start_date': args.start_date, 'end_date': args.end_date or datetime.now().strftime('%Y%m%d')}
        elif args.days:
            update_mode_desc = f"最近 {args.days} 交易日增量"
            run_args = {'update_mode': 'trade_day', 'trade_days_lookback': args.days, 'end_date': args.end_date}
        elif args.auto:
            update_mode_desc = "自动增量"
            run_args = {'update_mode': 'trade_day', 'end_date': args.end_date}

        logger.info(f"模式: {update_mode_desc} 更新")
        result = await task.execute(**run_args, **common_kwargs)
        logger.info(f"{update_mode_desc} 更新结果: {result}")

    except Exception as e:
        task_name_log = task.name if task else task_name
        logger.error(f"任务 '{task_name_log}' 执行过程中发生错误: {str(e)}", exc_info=True)
    finally:
        await TaskFactory.shutdown()
        logger.info("TaskFactory 已关闭")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"tushare_stock_dailybasic 数据更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    # Example usage:
    # python examples/tushare_stock_dailybasic_updater.py --auto
    # python examples/tushare_stock_dailybasic_updater.py --days 5
    # python examples/tushare_stock_dailybasic_updater.py --start-date 20230101 --end-date 20230131
    # python examples/tushare_stock_dailybasic_updater.py --full-update
    asyncio.run(main()) 