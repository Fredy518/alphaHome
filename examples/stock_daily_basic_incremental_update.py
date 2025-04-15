import asyncio
import logging
import sys
import os
import argparse
from datetime import datetime, timedelta
import dotenv

# 添加项目根目录到系统路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 加载环境变量
dotenv.load_dotenv()

from data_module.task_factory import TaskFactory
from config import get_concurrent_limit

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('stock_daily_basic_incremental_update') # Changed logger name

async def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='股票每日基本面指标增量更新工具') # Changed description
    parser.add_argument('--days', type=int, help='更新最近几个交易日的数据')
    parser.add_argument('--start-date', help='更新起始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='更新结束日期，格式：YYYYMMDD，默认为当天')
    parser.add_argument('--full-update', action='store_true', help='全表更新标志，从19910101开始更新')
    parser.add_argument('--auto', action='store_true', help='自动模式，从数据库最新日期的下一个交易日开始更新')
    
    # 添加传递给任务的其他参数，例如 show_progress
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')

    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info("开始执行 stock_daily_basic 增量更新...") # Changed message
    
    # 初始化任务工厂（自动连接数据库）
    await TaskFactory.initialize()
    
    # 获取任务实例（自动使用配置中的API令牌）
    task = await TaskFactory.get_task('stock_daily_basic') # Changed task name
    logger.info("StockDailyBasicTask 初始化成功") # Changed message
    
    # 从配置获取并发限制
    concurrent_limit = get_concurrent_limit()
    
    # 确定更新结束日期
    end_date = args.end_date or datetime.now().strftime('%Y%m%d')
    
    # 准备传递给任务的通用参数
    common_kwargs = {
        'concurrent_limit': concurrent_limit,
        'show_progress': args.show_progress
    }

    try:
        if args.full_update:
            # 全量更新模式
            start_date = '19910101'  # 中国股市开始时间
            logger.info(f"使用全量更新模式，更新 stock_daily_basic 范围: {start_date} 到 {end_date}") # Changed message
            result = await task.execute(start_date=start_date, end_date=end_date, **common_kwargs)
            logger.info(f"全量更新结果: {result}")

        elif args.start_date:
            # 用户指定的起始日期
            start_date = args.start_date
            logger.info(f"使用用户指定的日期范围更新 stock_daily_basic: {start_date} 到 {end_date}") # Changed message
            result = await task.execute(start_date=start_date, end_date=end_date, **common_kwargs)
            logger.info(f"指定日期更新结果: {result}")

        elif args.days:
            # 按指定交易日回溯天数更新
            logger.info(f"模式: 更新 stock_daily_basic 最近 {args.days} 个交易日的数据") # Changed message
            result = await task.update_by_trade_day(trade_days_lookback=args.days, end_date=end_date, **common_kwargs)
            logger.info(f"按天数回溯更新结果: {result}")

        elif args.auto:
            # 自动模式，从数据库最新日期的下一个交易日开始更新
            logger.info("模式: 自动增量更新 stock_daily_basic") # Changed message
            result = await task.update_by_trade_day(end_date=end_date, **common_kwargs) # 不传 trade_days_lookback
            logger.info(f"自动增量更新结果: {result}")
            
        else:
             # 默认行为：自动模式
            logger.info("模式: 默认自动增量更新 stock_daily_basic") # Changed message
            result = await task.update_by_trade_day(end_date=end_date, **common_kwargs) # 不传 trade_days_lookback
            logger.info(f"默认自动增量更新结果: {result}")

    except Exception as e:
        logger.error(f"stock_daily_basic 任务执行过程中发生错误: {str(e)}", exc_info=True) # Changed message
    finally:
        # 关闭数据库连接
        await TaskFactory.shutdown()
        logger.info("数据库连接已关闭")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"stock_daily_basic 增量更新执行完毕。总耗时: {duration}") # Changed message

if __name__ == "__main__":
    asyncio.run(main()) 