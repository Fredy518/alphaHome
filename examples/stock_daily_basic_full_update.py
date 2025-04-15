import asyncio
import logging
import sys
import os
from datetime import datetime
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

logger = logging.getLogger('stock_daily_basic_full_update')

async def main():
    start_time = datetime.now()
    logger.info("开始执行 stock_daily_basic 全量更新...")

    # 初始化任务工厂（自动连接数据库）
    await TaskFactory.initialize()

    try:
        # 获取任务实例（自动使用配置中的API令牌）
        task = await TaskFactory.get_task('stock_daily_basic')
        logger.info("StockDailyBasicTask 初始化成功")

        # 从配置获取并发限制
        concurrent_limit = get_concurrent_limit()

        # 定义全量更新的日期范围
        start_date = '20250101' # 或者 Tushare 支持的最早日期
        end_date = datetime.now().strftime('%Y%m%d')

        logger.info(f"执行全量更新，范围: {start_date} 到 {end_date}")

        # 准备通用参数
        common_kwargs = {
            'concurrent_limit': concurrent_limit,
            'show_progress': True
        }

        # 执行全量更新 (execute 会处理分批)
        result = await task.execute(
            start_date=start_date,
            end_date=end_date,
            **common_kwargs
        )
        logger.info(f"全量更新结果: {result}")

    except Exception as e:
        logger.error(f"全量更新过程中发生错误: {str(e)}", exc_info=True)
    finally:
        # 关闭数据库连接
        await TaskFactory.shutdown()
        logger.info("数据库连接已关闭")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"stock_daily_basic 全量更新执行完毕。总耗时: {duration}")

if __name__ == "__main__":
    asyncio.run(main()) 