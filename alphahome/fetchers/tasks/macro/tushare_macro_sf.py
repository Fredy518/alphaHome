#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
月度社会融资 (sf_month) 数据任务

接口文档: https://tushare.pro/document/2?doc_id=310
说明:
- 数据为月度级别，整体量远低于 2000 条，支持一次性全量抓取。
- 采用全量更新策略：每次执行不传任何过滤参数，直接获取全部数据。
- 为便于日期查询，额外生成 month_end_date 字段（对应月份的月末日期）。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareMacroSFTTask(TushareTask):
    """获取月度社会融资数据（sf_month，单批全量）"""

    # 1. 核心属性
    domain = "macro"
    name = "tushare_macro_sf"
    description = "获取月度社会融资数据，包含当月增量、累计值及存量"
    table_name = "macro_sf_month"
    primary_keys = ["month"]
    date_column = None  # 全量任务，不依赖日期增量
    default_start_date = "19900101"  # 占位，满足基类要求
    data_source = "tushare"
    single_batch = True
    update_type = "full"

    # --- 代码级默认配置 --- #
    default_concurrent_limit = 1
    default_page_size = 2000  # 官方限额为2000

    # 2. Tushare 特有属性
    api_name = "sf_month"
    fields = [
        "month",
        "inc_month",
        "inc_cumval",
        "stk_endval",
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {
        "inc_month": float,
        "inc_cumval": float,
        "stk_endval": float,
    }

    # 5. 表结构
    schema_def = {
        "month": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "inc_month": {"type": "NUMERIC(20,2)"},
        "inc_cumval": {"type": "NUMERIC(20,2)"},
        "stk_endval": {"type": "NUMERIC(20,4)"},
    }

    # 6. 索引
    indexes = [
        {"name": "idx_macro_sf_month", "columns": "month"},
        {"name": "idx_macro_sf_month_end", "columns": "month_end_date"},
        {"name": "idx_macro_sf_update_time", "columns": "update_time"},
    ]

    # 7. 验证规则
    validations = [
        (lambda df: df["month"].notna(), "月份不能为空"),
        (
            lambda df: df["month"].astype(str).str.match(r"^\d{6}$"),
            "月份格式必须为YYYYMM",
        ),
        (lambda df: df["inc_month"].fillna(0) >= 0, "当月社融增量应为非负"),
        (lambda df: df["inc_cumval"].fillna(0) >= 0, "累计社融增量应为非负"),
        (lambda df: df["stk_endval"].fillna(0) >= 0, "社融存量应为非负"),
        (
            lambda df: df["month_end_date"].notna(),
            "month_end_date 生成失败",
        ),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """全量模式：返回单个空参数批次。"""
        self.logger.info(
            "任务 %s: 采用全量模式，单批次拉取所有 sf_month 数据", self.name
        )
        return [
            {
                "fields": ",".join(self.fields or []),
            }
        ]

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """生成 month_end_date 并执行基础清洗。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            self.logger.info("任务 %s: 无可处理数据", self.name)
            return df

        df = super().process_data(df, **kwargs)

        if "month" not in df.columns:
            self.logger.error("任务 %s: 缺少 month 列，无法生成 month_end_date", self.name)
            return pd.DataFrame()

        def _month_to_end_date(month_str: Optional[str]) -> Optional[pd.Timestamp]:
            if pd.isna(month_str):
                return None
            text = str(month_str).strip()
            if len(text) != 6 or not text.isdigit():
                return None
            year = int(text[:4])
            month = int(text[4:6])
            try:
                # 计算月份最后一天
                next_month = month % 12 + 1
                next_year = year + (month // 12)
                first_day_next = datetime(next_year, next_month, 1)
                end_day = first_day_next - pd.Timedelta(days=1)
                return end_day.date()
            except Exception:
                return None

        df["month_end_date"] = df["month"].apply(_month_to_end_date)

        invalid = df["month_end_date"].isna().sum()
        if invalid:
            self.logger.warning(
                "任务 %s: %d 行 month_end_date 转换失败，将被丢弃", self.name, invalid
            )
            df = df[df["month_end_date"].notna()].copy()

        # 确保存入数据库的数据按月份顺序排序
        df = df.sort_values("month_end_date").reset_index(drop=True)

        self.logger.info("任务 %s: 处理后共 %d 行数据", self.name, len(df))
        return df


