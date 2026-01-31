#!/usr/bin/env python
"""
Features 模块初始化脚本

用于初始化 features schema 及元数据表，并可选创建物化视图。

使用方法:
    python scripts/features_init.py                    # 仅初始化 schema 和元数据表
    python scripts/features_init.py --create-views     # 同时创建物化视图
    python scripts/features_init.py --check            # 仅检查初始化状态
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import get_database_url
from alphahome.features.storage.database_init import FeaturesDatabaseInit
from alphahome.features.storage.base_view import BaseFeatureView

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ==============================================================================
# 物化视图定义导入
# ==============================================================================

def get_all_view_classes() -> list:
    """
    获取所有已注册的物化视图类。

    Returns:
        list: BaseFeatureView 子类列表
    
    可用的物化视图:
    - StockIndustryMonthlySnapshotMV: 股票行业分类（月度快照，sw+ci）
    - StockFinaIndicatorMV: 股票财务指标（含 PIT 时间窗口）(rawdata.fina_indicator)
    - StockIncomeQuarterlyMV: 股票利润表（含 PIT 时间窗口）(rawdata.fina_income)
    - StockBalanceQuarterlyMV: 股票资产负债表（含 PIT 时间窗口）(rawdata.fina_balancesheet)
    - StockDailyEnrichedMV: 每日行情增强 (rawdata.stock_daily + rawdata.stock_dailybasic)
    - MarketStatsMV: 市场横截面统计 (rawdata.stock_dailybasic)
    
    说明:
    - PIT 是时间语义原则，应贯穿所有特征；不再以 PIT 作为 recipe/MV 分类或命名前缀。
    """
    from alphahome.features import FeatureRegistry

    # discover() 会动态扫描并触发 @feature_register()，返回所有已注册 Recipe 类
    return FeatureRegistry.discover()



# ==============================================================================
# 初始化函数
# ==============================================================================

async def initialize_features_schema(db_manager: DBManager) -> bool:
    """
    初始化 features schema 和元数据表。

    Args:
        db_manager: 数据库管理器

    Returns:
        bool: 是否成功
    """
    try:
        initializer = FeaturesDatabaseInit(db_manager=db_manager, schema="features")
        success = await initializer.ensure_initialized()
        if success:
            logger.info("features schema 初始化成功")
        return success
    except Exception as e:
        logger.error(f"features schema 初始化失败: {e}")
        raise


async def check_initialization_status(db_manager: DBManager) -> dict:
    """
    检查 features schema 初始化状态。

    Args:
        db_manager: 数据库管理器

    Returns:
        dict: 状态信息
    """
    status = {
        "schema_exists": False,
        "mv_metadata_exists": False,
        "mv_refresh_log_exists": False,
        "view_count": 0,
        "views": [],
    }

    try:
        # 检查 schema
        schema_sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.schemata
            WHERE schema_name = 'features'
        ) AS exists;
        """
        result = await db_manager.fetch(schema_sql)
        status["schema_exists"] = result and result[0]["exists"]

        if not status["schema_exists"]:
            return status

        # 检查元数据表
        for table_name in ["mv_metadata", "mv_refresh_log"]:
            table_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'features' AND table_name = '{table_name}'
            ) AS exists;
            """
            result = await db_manager.fetch(table_sql)
            status[f"{table_name}_exists"] = result and result[0]["exists"]

        # 检查已创建的物化视图
        views_sql = """
        SELECT matviewname AS name
        FROM pg_matviews
        WHERE schemaname = 'features'
        ORDER BY matviewname;
        """
        result = await db_manager.fetch(views_sql)
        if result:
            status["views"] = [row["name"] for row in result]
            status["view_count"] = len(status["views"])

        return status

    except Exception as e:
        logger.error(f"检查初始化状态失败: {e}")
        raise


async def create_materialized_views(db_manager: DBManager, views: list = None) -> dict:
    """
    创建物化视图。

    Args:
        db_manager: 数据库管理器
        views: 视图类列表（默认为所有已注册的视图）

    Returns:
        dict: 创建结果 {"success": [...], "failed": [...]}
    """
    if views is None:
        views = get_all_view_classes()

    results = {"success": [], "failed": []}

    for view_class in views:
        try:
            view = view_class(db_manager=db_manager, schema="features")
            logger.info(f"创建物化视图: {view.full_name}")
            await view.create(if_not_exists=True)
            results["success"].append(view.name)
            logger.info(f"{view.name} 创建成功")
        except Exception as e:
            results["failed"].append({"name": view_class.name, "error": str(e)})
            logger.error(f"{view_class.name} 创建失败: {e}")

    return results


# ==============================================================================
# 主函数
# ==============================================================================

async def main(args: argparse.Namespace) -> int:
    """
    主函数。

    Args:
        args: 命令行参数

    Returns:
        int: 退出码
    """
    # 获取数据库连接 URL
    try:
        database_url = get_database_url()
        logger.info(f"数据库连接: {database_url[:50]}...")
    except Exception as e:
        logger.error(f"获取数据库配置失败: {e}")
        return 1

    # 创建数据库管理器
    db_manager = DBManager(database_url)

    try:
        await db_manager.connect()
        logger.info("数据库连接成功")

        # 检查模式
        if args.check:
            status = await check_initialization_status(db_manager)
            print("\n" + "=" * 60)
            print("Features Schema 初始化状态")
            print("=" * 60)
            print(f"Schema 存在:        {'OK' if status['schema_exists'] else 'FAIL'}")
            print(f"mv_metadata 表:     {'OK' if status['mv_metadata_exists'] else 'FAIL'}")
            print(f"mv_refresh_log 表:  {'OK' if status['mv_refresh_log_exists'] else 'FAIL'}")
            print(f"物化视图数量:       {status['view_count']}")
            if status["views"]:
                print(f"已创建的视图:       {', '.join(status['views'])}")
            print("=" * 60 + "\n")
            return 0

        # 初始化 schema 和元数据表
        logger.info("开始初始化 features schema...")
        await initialize_features_schema(db_manager)

        # 创建物化视图
        if args.create_views:
            logger.info("开始创建物化视图...")
            results = await create_materialized_views(db_manager)
            print("\n" + "=" * 60)
            print("物化视图创建结果")
            print("=" * 60)
            print(f"成功: {len(results['success'])} 个")
            for name in results["success"]:
                print(f"  OK {name}")
            if results["failed"]:
                print(f"失败: {len(results['failed'])} 个")
                for item in results["failed"]:
                    print(f"  FAIL {item['name']}: {item['error']}")
            print("=" * 60 + "\n")

            if results["failed"]:
                return 1

        logger.info("初始化完成")
        return 0

    except Exception as e:
        logger.error(f"执行失败: {e}")
        return 1

    finally:
        await db_manager.close()
        logger.info("数据库连接已关闭")


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="Features 模块初始化脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python scripts/features_init.py                    # 仅初始化 schema 和元数据表
    python scripts/features_init.py --create-views     # 同时创建物化视图
    python scripts/features_init.py --check            # 仅检查初始化状态
        """
    )

    parser.add_argument(
        "--create-views",
        action="store_true",
        help="创建物化视图"
    )

    parser.add_argument(
        "--check",
        action="store_true",
        help="仅检查初始化状态"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
