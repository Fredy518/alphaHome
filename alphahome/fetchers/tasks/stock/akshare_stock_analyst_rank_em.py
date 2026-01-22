#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 东方财富分析师指数（年度榜单）

接口:
- ak.stock_analyst_rank_em(year="2024")
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.akshare.akshare_task import AkShareTask
from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register


@task_register()
class AkShareStockAnalystRankEmTask(AkShareTask):
    domain = "stock"
    name = "akshare_stock_analyst_rank_em"
    description = "东方财富-研究报告-分析师指数年度榜单（AkShare stock_analyst_rank_em）"
    table_name = "stock_analyst_rank_em"
    data_source = "akshare"

    primary_keys = ["year", "analyst_id"]
    date_column = "as_of_date"
    default_start_date = "20180101"

    api_name = "stock_analyst_rank_em"

    schema_def = {
        "year": {"type": "VARCHAR(4)", "constraints": "NOT NULL"},
        "as_of_date": {"type": "DATE"},
        "seq": {"type": "INTEGER"},
        "analyst_id": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "analyst_name": {"type": "VARCHAR(50)"},
        "analyst_org": {"type": "VARCHAR(100)"},
        "industry_code": {"type": "VARCHAR(20)"},
        "industry": {"type": "VARCHAR(50)"},
        "annual_index": {"type": "NUMERIC(20,4)"},
        "return_year": {"type": "NUMERIC(20,6)"},
        "return_3m": {"type": "NUMERIC(20,6)"},
        "return_6m": {"type": "NUMERIC(20,6)"},
        "return_12m": {"type": "NUMERIC(20,6)"},
        "component_stock_count": {"type": "INTEGER"},
        "latest_rating_stock_name": {"type": "VARCHAR(50)"},
        "latest_rating_stock_code": {"type": "VARCHAR(10)"},
    }

    indexes = [
        {"name": "idx_stock_analyst_rank_em_year", "columns": "year"},
        {"name": "idx_stock_analyst_rank_em_industry_code", "columns": "industry_code"},
        {"name": "idx_stock_analyst_rank_em_update_time", "columns": "update_time"},
    ]

    validations = [
        (lambda df: df["year"].notna(), "年度不能为空"),
        (lambda df: df["analyst_id"].notna(), "分析师ID不能为空"),
    ]
    validation_mode = "report"

    def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        data = super().process_data(data, **kwargs)
        if data is None or data.empty:
            return data

        # 动态列名（包含年份）统一归一
        rename: Dict[str, str] = {
            "序号": "seq",
            "分析师名称": "analyst_name",
            "分析师单位": "analyst_org",
            "年度指数": "annual_index",
            "3个月收益率": "return_3m",
            "6个月收益率": "return_6m",
            "12个月收益率": "return_12m",
            "成分股个数": "component_stock_count",
            "分析师ID": "analyst_id",
            "行业代码": "industry_code",
            "行业": "industry",
            "更新日期": "as_of_date",
            "年度": "year",
        }

        year_return_col = None
        latest_name_col = None
        latest_code_col = None
        for col in data.columns:
            if re.fullmatch(r"\d{4}年收益率", str(col)):
                year_return_col = col
            if re.fullmatch(r"\d{4}最新个股评级-股票名称", str(col)):
                latest_name_col = col
            if re.fullmatch(r"\d{4}最新个股评级-股票代码", str(col)):
                latest_code_col = col

        if year_return_col:
            rename[year_return_col] = "return_year"
        if latest_name_col:
            rename[latest_name_col] = "latest_rating_stock_name"
        if latest_code_col:
            rename[latest_code_col] = "latest_rating_stock_code"

        data = data.rename(columns={k: v for k, v in rename.items() if k in data.columns})

        # year 强制为字符串 4 位
        if "year" in data.columns:
            data["year"] = data["year"].astype(str).str.extract(r"(\d{4})", expand=False)

        keep = [c for c in self.schema_def.keys() if c in data.columns]
        return data[keep]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        update_type = kwargs.get("update_type", UpdateTypes.SMART)
        if await self._should_skip_by_recent_update_time(update_type, max_age_days=30):
            return []
        now_year = datetime.now().year

        if update_type == UpdateTypes.MANUAL:
            year = kwargs.get("year")
            if not year:
                self.logger.error(f"{self.name}: 手动模式需要提供 year 参数")
                return []
            return [{"year": str(year)}]

        start_year = int(kwargs.get("start_year", 2018))
        end_year = int(kwargs.get("end_year", now_year))

        if update_type == UpdateTypes.FULL:
            years = list(range(start_year, end_year + 1))
        else:
            # 智能增量：默认取当年 + 上一年（覆盖跨年更新）
            years = sorted({now_year, now_year - 1})

        self.logger.info(f"{self.name}: 生成 {len(years)} 个年度批次: {years}")
        return [{"year": str(y)} for y in years]


__all__ = ["AkShareStockAnalystRankEmTask"]
