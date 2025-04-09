import asyncio
import logging
import sys
import os
from datetime import datetime
import dotenv

# 添加项目根目录到系统路径
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# 加载环境变量
dotenv.load_dotenv()

from data_module.factory import TaskFactory
from config import get_concurrent_limit

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('stock_daily_full_update')

async def main():
    start_time = datetime.now()
    logger.info(f"开始执行 stock_daily_full_update (1991年至今全量更新)...")
    
    # 初始化任务工厂（自动连接数据库）
    await TaskFactory.initialize()
    
    # 获取任务实例（自动使用配置中的API令牌）
    stock_daily_task = await TaskFactory.get_task('stock_daily')
    logger.info("StockDailyTask 初始化成功")
    
    # 从配置获取并发限制
    concurrent_limit = get_concurrent_limit()
    
    # 执行全量更新（从1991年至今）
    try:
        logger.info(f"开始全量更新，并发限制: {concurrent_limit}")
        
        # 设置日期范围参数（从1991年开始到现在）
        start_date = '19910101'  # 中国股市开始时间
        end_date = datetime.now().strftime('%Y%m%d')  # 当前日期
        
        # 直接调用execute方法，将所有参数作为关键字参数传递
        result = await stock_daily_task.execute(
            start_date=start_date,
            end_date=end_date,
            show_progress=True
        )
        logger.info(f"全量更新结果: {result}")
    except Exception as e:
        logger.error(f"全量更新失败: {str(e)}")
        raise
    finally:
        # 关闭数据库连接
        await db_manager.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"stock_daily_full_update 执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    asyncio.run(main())
