import asyncio
import logging
import sys
import os
import argparse
from datetime import datetime, timedelta
import dotenv

# 添加项目根目录到系统路径
# Use os.path.join for better cross-platform compatibility
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# 加载环境变量
dotenv_path = os.path.join(project_root, '.env')
dotenv.load_dotenv(dotenv_path=dotenv_path)

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

# 使用新的脚本名称作为logger名称
logger = logging.getLogger('tushare_stock_dailybasic_updater')

async def main():
    # 解析命令行参数
    # 更新脚本描述
    parser = argparse.ArgumentParser(description='Tushare 股票每日基本面指标更新工具 (支持全量和增量)') 
    parser.add_argument('--days', type=int, help='增量更新: 更新最近 N 个交易日的数据')
    parser.add_argument('--start-date', help='增量或范围更新: 更新起始日期 (格式: YYYYMMDD)')
    parser.add_argument('--end-date', help='增量或范围更新: 更新结束日期 (格式: YYYYMMDD, 默认为当天)')
    # Tushare daily_basic 最早日期可能不同，需要确认，暂时用 19910101 作为示例
    parser.add_argument('--full-update', action='store_true', help='全量更新模式: 从 Tushare 支持的最早日期开始更新 (示例: 19910101)') 
    parser.add_argument('--auto', action='store_true', help='自动增量模式: 从数据库最新日期的下一个交易日开始更新 (默认模式)')
    
    # 添加传递给任务的其他参数
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help='是否显示进度条')

    args = parser.parse_args()
    
    start_time = datetime.now()
    # 更新日志信息
    logger.info("开始执行 tushare_stock_dailybasic 数据更新...") 
    
    # 初始化任务工厂（自动连接数据库）
    await TaskFactory.initialize()
    logger.info("TaskFactory 初始化成功")

    task = None # Initialize task to None for finally block
    try:
        # 获取任务实例（使用新的任务名称）
        task_name = "tushare_stock_dailybasic"
        task = await TaskFactory.get_task(task_name)
        logger.info(f"任务 '{task_name}' (TushareStockDailyBasicTask) 初始化成功") 
        
        # 从配置获取并发限制
        concurrent_limit = get_concurrent_limit()
        
        # 确定更新结束日期
        end_date = args.end_date or datetime.now().strftime('%Y%m%d')
        
        # 准备传递给任务的通用参数
        common_kwargs = {
            'concurrent_limit': concurrent_limit,
            'show_progress': args.show_progress
        }

        result = None
        update_mode = "默认自动增量" # Default mode

        if args.full_update:
            # 全量更新模式
            update_mode = "全量"
            # TODO: 确认 Tushare daily_basic 的实际最早支持日期
            start_date = '19910101'  # 假设的最早日期
            logger.info(f"模式: {update_mode} 更新, 范围: {start_date} 到 {end_date}")
            result = await task.execute(start_date=start_date, end_date=end_date, **common_kwargs)
           

        elif args.start_date:
            # 用户指定的起始日期
            update_mode = "指定日期范围"
            start_date = args.start_date
            logger.info(f"模式: {update_mode} 更新, 范围: {start_date} 到 {end_date}")
            result = await task.execute(start_date=start_date, end_date=end_date, **common_kwargs)
           

        elif args.days:
            # 按指定交易日回溯天数更新
            update_mode = f"最近 {args.days} 交易日增量"
            logger.info(f"模式: {update_mode} 更新, 结束日期: {end_date}")
            result = await task.update_by_trade_day(trade_days_lookback=args.days, end_date=end_date, **common_kwargs)
           

        elif args.auto:
            # 自动模式
            update_mode = "自动增量 (从数据库最新日期+1交易日开始)"
            logger.info(f"模式: {update_mode} 更新, 结束日期: {end_date}")
            result = await task.update_by_trade_day(end_date=end_date, **common_kwargs) # 不传 trade_days_lookback
            
            
        else:
             # 默认行为：自动模式
            logger.info(f"模式: {update_mode} 更新 (从数据库最新日期+1交易日开始), 结束日期: {end_date}")
            result = await task.update_by_trade_day(end_date=end_date, **common_kwargs) # 不传 trade_days_lookback
            

        if result:
            logger.info(f"{update_mode} 更新结果: {result}")
        else:
             logger.warning(f"{update_mode} 更新未返回结果或未执行。")


    except Exception as e:
        logger.error(f"任务 '{task.name if task else '未知'}' 执行过程中发生错误: {str(e)}", exc_info=True) 
        # Consider re-raising or exiting with an error code
        # raise
    finally:
        # 关闭数据库连接
        await TaskFactory.shutdown()
        logger.info("数据库连接已关闭")

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