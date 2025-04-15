import asyncio
import logging
import sys
import os
import argparse
from datetime import datetime, timedelta
import dotenv

# 添加项目根目录到系统路径
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# 加载环境变量
dotenv.load_dotenv()

from data_module.task_factory import TaskFactory
from data_module.tools.calendar import get_last_trade_day, get_trade_days_between
from config import get_concurrent_limit

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('stock_daily_incremental_update')

async def get_latest_data_date(task):
    """获取数据库中最新的数据日期
    
    Args:
        task: 任务实例，用于执行数据库查询
        
    Returns:
        str: 最新数据日期，格式为YYYYMMDD；如果没有数据，返回None
    """
    try:
        # 获取数据库中最新的交易日期
        db = TaskFactory.get_db_manager()
        table_name = task.table_name
        date_column = task.date_column
        
        query = f"SELECT MAX({date_column}) FROM {table_name}"
        result = await db.fetch_one(query)
        
        if result and result[0]:
            # 将日期格式化为YYYYMMDD
            latest_date = result[0]
            if isinstance(latest_date, str):
                # 如果是字符串，确保格式正确
                return latest_date.replace('-', '')
            else:
                # 如果是日期对象，格式化为字符串
                return latest_date.strftime('%Y%m%d')
        return None
    except Exception as e:
        logger.error(f"获取最新数据日期失败: {str(e)}")
        return None

async def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='股票日线数据增量更新工具')
    parser.add_argument('--days', type=int, help='更新最近几个交易日的数据（默认为3）')
    parser.add_argument('--start-date', help='更新起始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='更新结束日期，格式：YYYYMMDD，默认为当前日期')
    parser.add_argument('--full-update', action='store_true', help='全表更新标志，如果设置则从最早有数据的日期开始更新')
    parser.add_argument('--auto', action='store_true', help='自动模式，从数据库最新日期的下一个交易日开始更新')
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info(f"开始执行 stock_daily_incremental_update...")
    
    # 初始化任务工厂（自动连接数据库）
    await TaskFactory.initialize()
    
    # 获取任务实例（自动使用配置中的API令牌）
    stock_daily_task = await TaskFactory.get_task('stock_daily')
    logger.info("StockDailyTask 初始化成功")
    
    # 从配置获取并发限制
    concurrent_limit = get_concurrent_limit()
    
    # 确定更新日期范围
    end_date = args.end_date or datetime.now().strftime('%Y%m%d')
    
    # 确定起始日期
    start_date = None
    
    if args.full_update:
        # 全量更新模式
        start_date = '19910101'  # 中国股市开始时间
        logger.info("使用全量更新模式")
    elif args.start_date:
        # 用户指定的起始日期
        start_date = args.start_date
        logger.info(f"使用用户指定的起始日期: {start_date}")
    elif args.days:
        # 更新最近N个交易日
        days = args.days
        today = datetime.now().strftime('%Y%m%d')
        # 获取N天前的交易日
        start_date = await get_last_trade_day(date=today, n=days)
        logger.info(f"更新最近{days}个交易日的数据，起始日期: {start_date}")
    elif args.auto:
        # 自动模式，从数据库最新日期的下一个交易日开始更新
        latest_date = await get_latest_data_date(stock_daily_task)
        if latest_date:
            # 获取数据库中记录的下一个交易日
            trade_days = await get_trade_days_between(latest_date, end_date)
            if len(trade_days) > 1:
                # 跳过最后一天，因为可能已经有记录
                start_date = trade_days[1]  
                logger.info(f"数据库最新日期为 {latest_date}，从下一个交易日 {start_date} 开始更新")
            else:
                logger.info(f"数据库最新日期为 {latest_date}，没有需要更新的数据")
                await TaskFactory.shutdown()
                return
        else:
            # 如果数据库中没有记录，默认更新最近30个交易日
            start_date = await get_last_trade_day(date=end_date, n=30)
            logger.info(f"数据库无记录，默认更新最近30个交易日，起始日期: {start_date}")
    else:
        # 默认更新最近3个交易日
        start_date = await get_last_trade_day(date=end_date, n=3)
        logger.info(f"默认更新最近3个交易日，起始日期: {start_date}")
    
    # 检查是否有需要更新的日期范围
    if start_date and end_date:
        # 获取实际的交易日列表
        trade_days = await get_trade_days_between(start_date, end_date)
        if not trade_days:
            logger.info(f"在 {start_date} 到 {end_date} 之间没有发现交易日，无需更新")
            await TaskFactory.shutdown()
            return
            
        logger.info(f"将更新从 {start_date} 到 {end_date} 之间的数据，共 {len(trade_days)} 个交易日")
        
        # 执行增量更新
        try:
            logger.info(f"开始增量更新，并发限制: {concurrent_limit}")
            
            # 直接调用execute方法，将所有参数作为关键字参数传递
            result = await stock_daily_task.execute(
                start_date=start_date,
                end_date=end_date,
                show_progress=True
            )
            logger.info(f"增量更新结果: {result}")
        except Exception as e:
            logger.error(f"增量更新失败: {str(e)}")
            raise
        finally:
            # 关闭数据库连接
            await TaskFactory.shutdown()
    else:
        logger.error("无法确定更新日期范围")
        await TaskFactory.shutdown()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"stock_daily_incremental_update 执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    asyncio.run(main()) 