#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 基金拆分数据任务（fund_cf_em）

目标地址: http://fund.eastmoney.com/data/fundchaifen.html
描述: 天天基金网-基金数据-分红送配-基金拆分

接口说明:
- akshare.fund_cf_em() 单次返回所有历史数据（不支持年份参数）

存储说明:
- schema: akshare（由 data_source 决定）
- table: fund_cf_em
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
class AkShareFundCfEmTask(AkShareTask):
    """
    获取基金拆分数据（AkShare fund_cf_em）

    批处理策略:
    - 单批次：一次性获取所有历史数据
    """

    # 1) 核心属性
    domain = "fund"
    name = "akshare_fund_cf_em"
    description = "天天基金网-基金数据-分红送配-基金拆分（AkShare fund_cf_em）"
    table_name = "fund_cf_em"
    data_source = "akshare"

    primary_keys = ["fund_code", "split_date", "split_type", "split_ratio"]
    date_column = "split_date"
    # 单批次任务的默认起始日期（用于兼容性）
    default_start_date = "20050101"

    # 缓存基金代码映射表（实例级别）
    _fund_code_to_ts_code_cache: Optional[Dict[str, str]] = None

    async def _pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """
        任务执行前的准备工作：预加载基金代码映射。
        """
        await super()._pre_execute(stop_event=stop_event, **kwargs)
        await self._load_fund_code_mapping()

    # 2) AkShare 特有属性
    api_name = "fund_cf_em"
    api_params: Optional[Dict[str, Any]] = None

    # 3) 列名映射（AkShare 返回中文表头）
    column_mapping = {
        "基金代码": "fund_code",
        "基金简称": "fund_name",
        "拆分折算日": "split_date",
        "拆分类型": "split_type",
        "拆分折算": "split_ratio",  # 单位：每份
    }

    # 4) 数据类型转换
    transformations = {
        "split_ratio": float,
    }

    # 5) 数据库表结构
    schema_def = {
        "fund_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)"},  # 从 tushare.fund_basic 获取的 TS 代码
        "fund_name": {"type": "VARCHAR(100)"},
        "split_date": {"type": "DATE", "constraints": "NOT NULL"},
        "split_type": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "split_ratio": {"type": "NUMERIC(18,6)", "constraints": "NOT NULL"},
        # update_time 会自动添加
    }

    indexes = [
        {"name": "idx_fund_cf_em_fund_code", "columns": "fund_code"},
        {"name": "idx_fund_cf_em_ts_code", "columns": "ts_code"},
        {"name": "idx_fund_cf_em_split_date", "columns": "split_date"},
        {"name": "idx_fund_cf_em_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["fund_code"].notna(), "基金代码不能为空"),
        (lambda df: df["split_date"].notna(), "拆分折算日不能为空"),
        (lambda df: df["split_type"].notna(), "拆分类型不能为空"),
        (lambda df: df["split_ratio"].notna(), "拆分折算不能为空"),
        (lambda df: df["split_ratio"] > 0, "拆分折算应为正数"),
    ]

    validation_mode = "report"


    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        生成单批次参数列表。

        fund_cf_em 接口一次性返回所有历史数据。
        """
        self.logger.info(f"任务 {self.name}: 生成单批次参数（一次性获取所有历史数据）")
        return [{}]  # 返回空参数字典

    async def _load_fund_code_mapping(self) -> Dict[str, str]:
        """
        从 tushare.fund_basic 表加载基金代码到 ts_code 的映射。

        Returns:
            Dict[str, str]: fund_code -> ts_code 的映射
            例如: {"000001": "000001.OF", "000011": "000011.OF"}
        """
        if self._fund_code_to_ts_code_cache is not None:
            return self._fund_code_to_ts_code_cache

        try:
            query = """
                SELECT ts_code
                FROM tushare.fund_basic
                ORDER BY ts_code
            """
            rows = await self.db.fetch(query)

            # 构建映射: fund_code(6位数字) -> ts_code
            mapping = {}
            for row in rows:
                ts_code = row["ts_code"]
                if ts_code:
                    # 如果ts_code包含"."，取前面的部分作为fund_code
                    if "." in ts_code:
                        fund_code = ts_code.split(".")[0]
                    else:
                        # 如果不包含"."，直接使用ts_code作为fund_code
                        fund_code = ts_code
                    mapping[fund_code] = ts_code

            self._fund_code_to_ts_code_cache = mapping
            self.logger.info(f"已加载 {len(mapping)} 条基金代码映射")
            return mapping

        except Exception as e:
            self.logger.error(f"加载基金代码映射失败: {e}")
            return {}

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理数据，为 fund_code 添加对应的 ts_code。

        Args:
            data: 原始数据
            **kwargs: 额外参数

        Returns:
            处理后的数据
        """
        # 先调用父类的 process_data 应用基础转换
        data = super().process_data(data, **kwargs)

        if data is None or data.empty:
            return data

        # 过滤列：只保留 schema_def 中定义的字段
        schema_columns = set(self.schema_def.keys())
        available_columns = set(data.columns)
        columns_to_keep = available_columns & schema_columns

        if columns_to_keep != available_columns:
            removed_columns = available_columns - columns_to_keep
            self.logger.debug(f"移除不需要的列: {removed_columns}")
            data = data[list(columns_to_keep)]

        # 获取基金代码映射（在 _pre_execute 中已加载）
        fund_code_mapping = self._fund_code_to_ts_code_cache or {}

        if not fund_code_mapping:
            self.logger.warning("基金代码映射为空，ts_code 将保持为空")
            data["ts_code"] = None
            return data

        # 为 fund_code 添加 ts_code
        if "fund_code" in data.columns:
            original_count = len(data)

            def get_ts_code(fund_code):
                if pd.isna(fund_code):
                    return None
                fund_code_str = str(fund_code).strip()
                return fund_code_mapping.get(fund_code_str)

            data["ts_code"] = data["fund_code"].apply(get_ts_code)

            # 统计映射成功的情况
            mapped_count = data["ts_code"].notna().sum()
            self.logger.info(
                f"ts_code 映射完成: {mapped_count}/{original_count} 条记录成功映射"
            )

        return data


__all__ = ["AkShareFundCfEmTask"]

