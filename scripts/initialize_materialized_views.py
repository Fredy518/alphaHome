#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
初始化 features 物化视图系统（兼容旧脚本名）

历史上本仓库使用 scripts/initialize_materialized_views.py 初始化物化视图子系统。
features 重构后，推荐使用新入口：
    python scripts/features_init.py --create-views

本脚本保留旧文件名以兼容既有文档/习惯用法，但实际初始化目标已切换为：
- schema: features
- 元数据表: features.mv_metadata / features.mv_refresh_log
- 视图定义: alphahome.features.recipes.mv 下的 BaseFeatureView 子类
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.config_manager import get_database_url
from alphahome.common.db_manager import DBManager
from alphahome.features import FeatureRegistry
from alphahome.features.storage.database_init import FeaturesDatabaseInit


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


async def initialize_features_views(create_views: bool = True) -> bool:
    logger = logging.getLogger(__name__)

    db_url = os.environ.get("DATABASE_URL") or get_database_url()
    db_manager = DBManager(db_url)

    try:
        await db_manager.connect()
        logger.info("数据库连接成功")

        initializer = FeaturesDatabaseInit(db_manager=db_manager, schema="features")
        await initializer.ensure_initialized()
        logger.info("features schema 与元数据表就绪")

        if not create_views:
            return True

        view_classes = FeatureRegistry.discover()
        results: dict[str, list] = {"success": [], "failed": []}

        for view_cls in view_classes:
            try:
                view = view_cls(db_manager=db_manager, schema="features")
                logger.info(f"创建物化视图: {view.full_name}")
                await view.create(if_not_exists=True)
                results["success"].append(view.name)
            except Exception as e:
                results["failed"].append({"name": view_cls.name, "error": str(e)})
                logger.error(f"{view_cls.name} 创建失败: {e}")

        logger.info(
            f"物化视图创建完成：成功 {len(results['success'])} 个，失败 {len(results['failed'])} 个"
        )
        return len(results["failed"]) == 0

    finally:
        await db_manager.close()
        logger.info("数据库连接已关闭")


def main() -> int:
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("features 物化视图系统初始化（兼容旧脚本名）")
    logger.info("推荐新入口：python scripts/features_init.py --create-views")
    logger.info("=" * 60)

    try:
        ok = asyncio.run(initialize_features_views(create_views=True))
        return 0 if ok else 1
    except Exception as e:
        logger.error(f"初始化失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

