import asyncio
import logging
import os
from datetime import datetime, timedelta
import sys

# 添加项目根目录到系统路径
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from data_module.factory import TaskFactory, get_task
from config import get_concurrent_limit

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 设置其他模块的日志级别为WARNING，减少输出
logging.getLogger('data_module').setLevel(logging.WARNING)
logging.getLogger('task').setLevel(logging.WARNING)

# 只保留主脚本的INFO级别日志
logger = logging.getLogger('stock_daily_incremental_update')

async def main():
    start_time = datetime.now()
    logger.info(f"开始执行 stock_daily_incremental_update (增量更新)...")

    # 初始化任务工厂（自动连接数据库）
    await TaskFactory.initialize()
    
    # 获取任务实例（自动使用配置中的API令牌）
    stock_daily_task = await get_task('stock_daily')
    logger.info("StockDailyTask 初始化成功")

    # 执行增量更新
    try:
        logger.info(f"使用默认配置 - 并发限制: {stock_daily_task.default_concurrent_limit}, 页面大小: {stock_daily_task.default_page_size}")

        # 使用增量更新方法，自动确定日期范围
        result = await stock_daily_task.incremental_update(
            safety_days=1,  # 回溯1天以确保数据连续性
            show_progress=True,
            progress_desc='增量更新股票日线数据'
        )

        # 验证数据是否成功保存到数据库
        current_date = datetime.now()
        today_date_str = current_date.strftime('%Y-%m-%d')
        
        if result['status'] == 'up_to_date':
            logger.info(f"数据库已是最新状态，无需更新")
        elif result['status'] == 'success' and result['rows'] > 0:
            # 检查今日数据是否存在
            today_query = f"""
            SELECT COUNT(*) FROM stock_daily WHERE trade_date = '{today_date_str}'
            """
            count = await TaskFactory.get_db_manager().fetch_val(today_query)
            
            if count > 0:
                logger.info(f"成功验证：今日数据已保存 ({count} 条记录)")
            else:
                # 检查今日是否为交易日
                is_trading_day = await check_if_trading_day(today_date_str)
                
                if is_trading_day:
                    logger.warning(f"警告：今日 ({today_date_str}) 是交易日但数据未保存")
                    # 尝试专门获取今日数据
                    await fetch_today_data(stock_daily_task, today_date_str)
                else:
                    logger.info(f"今日 ({today_date_str}) 不是交易日，无需获取数据")
        else:
            logger.warning(f"增量更新完成，但有警告: {result['status']}")
    except Exception as e:
        logger.error(f"增量更新失败: {str(e)}")
        raise
    finally:
        # 关闭数据库连接
        await TaskFactory.shutdown()

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"stock_daily_incremental_update 执行完毕。总耗时: {duration}")

async def check_if_trading_day(date_str):
    # 获取数据库管理器
    db_manager = TaskFactory._db_manager
    """检查指定日期是否为交易日"""
    # 先检查是否为周末
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    if date_obj.weekday() >= 5:  # 5和6表示周六和周日
        logger.info(f"{date_str} 是周末，不是交易日")
        return False
        
    # 检查是否为节假日或其他非交易日
    try:
        trade_cal_query = f"""
        SELECT 1 FROM stock_trade_calendar 
        WHERE cal_date = '{date_str}' AND is_open = 1
        LIMIT 1
        """
        is_trade_day = await db_manager.fetch_val(trade_cal_query)
        if is_trade_day:
            return True
        else:
            logger.info(f"{date_str} 是法定节假日或休市日")
            return False
    except Exception as e:
        # 如果表不存在或查询出错，使用简单判断
        logger.warning(f"无法查询交易日历: {str(e)}")
        # 工作日默认为交易日
        return True
        
async def fetch_today_data(stock_daily_task, today_date_str):
    """专门获取今日数据"""
    try:
        logger.info("尝试专门获取今日数据...")
        today_date_num = today_date_str.replace('-', '')
        today_result = await stock_daily_task.execute(
            start_date=today_date_num,
            end_date=today_date_num,
            show_progress=False
        )
        if today_result['status'] == 'success' and today_result['rows'] > 0:
            logger.info(f"成功获取并保存今日数据: {today_result['rows']} 条记录")
        else:
            logger.info(f"今日无数据可获取，可能是数据源尚未更新: {today_result}")
    except Exception as e:
        logger.warning(f"尝试获取今日数据时出错: {str(e)}")

if __name__ == '__main__':
    # 添加命令行参数支持
    # 使用方式：python stock_daily_incremental_update.py --quiet
    is_quiet = '--quiet' in sys.argv or '-q' in sys.argv

    if is_quiet:
        # 安静模式：禁用所有进度条和非关键日志
        logging.getLogger().setLevel(logging.WARNING)
        logger.setLevel(logging.WARNING)

    asyncio.run(main())
