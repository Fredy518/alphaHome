import asyncio
import logging
import sys
import os

# 将项目根目录添加到 Python 路径中，以便正确导入模块
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from alphahome.common.config_manager import ConfigManager
from alphahome.common.db_manager import DBManager
from alphahome.fetchers.tasks.stock.ifind_stock_basic import iFindStockBasicTask

# --- 测试配置 ---
# 配置日志记录，以便在控制台看到详细的执行信息
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)

# 用于测试的证券代码列表
TEST_CODES = ["000001.SZ", "600519.SH"] 
# 希望获取的指标 (用分号分隔)
TEST_INDICATORS = "ths_stock_short_name_stock;ths_listed_market_stock;ths_list_date_stock;ths_pe_ttm_stock"
# 可选的报告期 (YYYY-MM-DD) - 在新API中已不再需要
# TEST_REPORT_DATE = "2023-12-31"


async def main():
    """主函数，用于运行 iFind 上市公司基本资料获取测试。"""
    logger = logging.getLogger("test_ifind_fetch")
    logger.info("--- 开始测试 iFind 上市公司基本资料获取流程 ---")

    db_manager = None
    config_manager = None
    try:
        # 1. 初始化 ConfigManager
        logger.info("正在初始化配置管理器...")
        config_manager = ConfigManager()
        config = config_manager.load_config() # 预加载以检查
        if not config.get("database", {}).get("url"):
            logger.error("错误: 未能在 config.json 中找到 'database.url'。请检查您的配置。")
            return
        if not config.get("api", {}).get("ifind"):
            logger.error("错误: 未能在 config.json 中找到 'api.ifind' 部分。请检查您的配置。")
            return

        # 2. 初始化 DBManager
        logger.info("正在初始化数据库管理器...")
        db_url = config.get("database", {}).get("url")
        if not db_url:
            logger.error("错误: 未能在 config.json 中找到 'database.url'。请检查您的配置。")
            return
        
        db_manager = DBManager(connection_string=db_url)
        await db_manager.connect()
        logger.info("数据库连接成功。")
        
        # 3. 实例化 iFindStockBasicTask
        logger.info("正在实例化 iFindStockBasicTask...")
        ifind_task = iFindStockBasicTask(db_manager=db_manager, config_manager=config_manager)
        
        # 4. 执行任务
        logger.info(f"即将执行任务: '{ifind_task.name}'...")
        logger.info("注意: data_pool 端点将获取所有上市公司基本资料，无需指定股票代码")
        
        # data_pool 端点不需要传递 codes 参数
        result = await ifind_task.execute()

        # 5. 输出执行结果
        logger.info("--- 任务执行完成 ---")
        logger.info(f"最终结果: {result}")

    except Exception as e:
        logger.error(f"执行过程中发生错误: {e}", exc_info=True)

    finally:
        # 6. 清理资源
        if db_manager:
            await db_manager.close()
            logger.info("数据库连接已关闭。")


if __name__ == "__main__":
    asyncio.run(main()) 