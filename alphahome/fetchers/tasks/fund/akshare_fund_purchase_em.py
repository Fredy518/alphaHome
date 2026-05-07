#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 基金申购数据任务（fund_purchase_em）

目标地址: http://fund.eastmoney.com/data/fundpurchase.html
描述: 天天基金网-基金数据-申购状态及限购额度

接口说明:
- akshare.fund_purchase_em() 单次返回所有基金的最新申购状态（快照数据）

存储说明:
- schema: akshare（由 data_source 决定）
- table: fund_purchase_limit
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.akshare.akshare_task import AkShareTask
from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareFundPurchaseEmTask(AkShareTask):
    """
    获取基金申购状态及限购额度数据（AkShare fund_purchase_em）

    批处理策略:
    - 单批次：一次性获取所有基金的最新申购数据（快照）
    """

    # 1) 核心属性
    domain = "fund"
    name = "akshare_fund_purchase_em"
    description = "天天基金网-基金数据-申购状态及限购额度（AkShare fund_purchase_em）"
    table_name = "fund_purchase_limit"
    data_source = "akshare"

    primary_keys = ["fund_code", "snapshot_date"]
    date_column = "snapshot_date"
    # 快照接口不支持历史回补，这里保留一个占位起始日，
    # 仅用于兼容 FetcherTask 的 FULL/SMART 日期窗口计算。
    default_start_date = "20000101"

    # 2) AkShare 特有属性
    api_name = "fund_purchase_em"
    api_params: Optional[Dict[str, Any]] = None

    # 3) 列名映射（AkShare 返回中文表头）
    column_mapping = {
        "基金代码": "fund_code",
        "基金简称": "fund_name",
        "申购状态": "purchase_status",
        "日累计限定金额": "daily_limit_amount",
        "赎回状态": "redemption_status",
        "最新净值": "latest_nav",
        "最新净值/万份收益": "latest_nav",
    }

    # 4) 数据类型转换
    transformations = {
        "daily_limit_amount": float,
        "latest_nav": float,
    }

    # 5) 数据库表结构
    schema_def = {
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "fund_name": {"type": "VARCHAR(100)"},
        "purchase_status": {"type": "VARCHAR(20)"},
        "daily_limit_amount": {"type": "NUMERIC(18,2)"},
        "redemption_status": {"type": "VARCHAR(20)"},
        "latest_nav": {"type": "NUMERIC(10,4)"},
        "snapshot_date": {"type": "DATE", "constraints": "NOT NULL"},
        # update_time 会自动添加
    }

    indexes = [
        {"name": "idx_fund_purchase_em_fund_code", "columns": "fund_code"},
        {"name": "idx_fund_purchase_em_snapshot_date", "columns": "snapshot_date"},
        {"name": "idx_fund_purchase_em_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["fund_code"].notna(), "基金代码不能为空"),
        (lambda df: df["snapshot_date"].notna(), "快照日期不能为空"),
    ]

    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        生成单批次参数列表。

        fund_purchase_em 接口一次性返回所有基金的最新申购状态（快照）。
        """
        update_type = kwargs.get("update_type", UpdateTypes.SMART)
        if await self._should_skip_by_recent_update_time(update_type, max_age_days=1):
            return []

        self.logger.info(f"任务 {self.name}: 生成单批次参数（一次性获取最新申购状态数据）")
        return [{}]  # 返回空参数字典

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理数据，添加快照日期并规范化限购额度。

        规范化规则：
        - 空字符串、"无限制"、或 ≥1e9 的大数 → NULL（表示无限额）
        - 正常数值 → 保留原值

        Args:
            data: 原始数据
            **kwargs: 额外参数

        Returns:
            处理后的数据
        """
        import numpy as np

        # 先调用父类的 process_data 应用基础转换
        data = super().process_data(data, **kwargs)

        if data is None or data.empty:
            return data

        # 规范化 daily_limit_amount：将无限额情况转为 NULL
        if "daily_limit_amount" in data.columns:
            # 已经是数值类型（父类 transformations 已转换）
            # 将 ≥1e9 的大数视为无限额
            data.loc[data["daily_limit_amount"] >= 1e9, "daily_limit_amount"] = np.nan

        # 过滤列：只保留 schema_def 中定义的字段
        schema_columns = set(self.schema_def.keys())
        available_columns = set(data.columns)
        columns_to_keep = available_columns & schema_columns

        if columns_to_keep != available_columns:
            removed_columns = available_columns - columns_to_keep
            self.logger.debug(f"移除不需要的列: {removed_columns}")
            data = data[list(columns_to_keep)]

        # 添加快照日期（当前日期）
        if "snapshot_date" not in data.columns:
            data["snapshot_date"] = datetime.now().strftime("%Y-%m-%d")

        return data


__all__ = ["AkShareFundPurchaseEmTask"]
