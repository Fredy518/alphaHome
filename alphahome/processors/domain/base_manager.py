#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
域服务抽象基类（processors）
===========================

该基类定义了所有数据预处理域服务应实现的统一接口，
以支持“域服务 + 任务包装”的模式。所有具体域（含 PIT）
都应继承本类并实现四大接口：
- ensure_tables_exist: 建表/索引等结构治理
- full_rebuild: 全量重建
- incremental_update: 增量更新（幂等）
- validate: 数据质量校验

注意：
- 统一通过 DBManager 注入，保持双模（sync/async）一致性。
- 默认 schema 使用 "processors"，可在子类中重写 get_default_schema。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class BaseDomainManager:
    """数据预处理域服务抽象基类。

    子类应实现四大接口，并可复用本类提供的辅助方法。
    """

    def __init__(self, db_manager: "DBManager", batch_size: int = 500) -> None:
        # DB 管理器（双模：sync/async）
        self.db = db_manager
        # 处理批大小（默认 500，可由子类覆盖/传参）
        self.batch_size = int(batch_size)
        # 默认命名空间（schema）
        self.schema_name = self.get_default_schema()

    # ---------------------------------------------------------------------
    # 可重写配置
    # ---------------------------------------------------------------------
    def get_default_schema(self) -> str:
        """返回默认 schema 名称。

        子类可重写以适配不同命名空间。
        """
        return "processors"

    # ---------------------------------------------------------------------
    # 抽象接口（由子类实现）
    # ---------------------------------------------------------------------
    async def ensure_tables_exist(self) -> None:
        """确保所需表与索引存在（异步）。"""
        raise NotImplementedError

    def full_rebuild(
        self,
        stocks: Optional[List[str]] = None,
        date_range: Optional[Tuple[Optional[str], Optional[str]]] = None,
    ) -> Dict[str, Any]:
        """执行全量重建（同步）。"""
        raise NotImplementedError

    def incremental_update(
        self,
        since: Optional[str] = None,
        stocks: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """执行增量更新（同步，需幂等）。"""
        raise NotImplementedError

    def validate(self) -> Dict[str, Any]:
        """执行数据质量校验（同步）。"""
        raise NotImplementedError

    # ---------------------------------------------------------------------
    # 辅助方法（可复用）
    # ---------------------------------------------------------------------
    async def _ensure_namespace_async(self) -> None:
        """确保默认 schema 存在（异步）。"""
        # SchemaManagementMixin 提供 ensure_schema_exists( async )
        await self.db.ensure_schema_exists(self.schema_name)  # type: ignore[attr-defined]

    def _ensure_namespace_sync(self) -> None:
        """确保默认 schema 存在（同步）。

        说明：
        - 出于通用性，直接使用 execute_sync 发起 DDL，可避免依赖内部私有桥接。
        - 若在 async 模式下调用此方法，将由 DBManager 的实现自行抛错或转为兼容路径。
        """
        self.db.execute_sync(f'CREATE SCHEMA IF NOT EXISTS "{self.schema_name}"')  # type: ignore[attr-defined]


