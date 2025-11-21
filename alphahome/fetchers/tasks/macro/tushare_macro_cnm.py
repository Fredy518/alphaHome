#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
货币供应量 (cn_m) 月度数据任务

接口文档: https://tushare.pro/document/2?doc_id=242
说明:
- 获取月度 M0/M1/M2 及其同比、环比数据。
- 总体数据量远小于单次 5000 条上限，采用全量单批更新，每次执行获取全部数据。
- 类似 `tushare_macro_sf.py`，在表中增加 month_end_date 字段并按其排序存储。
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareMacroCNMTask(TushareTask):
    """获取月度货币供应量数据（cn_m，全量单批）"""

    # 1. 核心属性
    domain = "macro"
    name = "tushare_macro_cnm"
    description = "获取月度货币供应量数据（M0/M1/M2 及同比、环比）"
    table_name = "macro_cn_m"
    primary_keys = ["month"]
    date_column = None
    default_start_date = "19900101"  # 占位，满足基类要求
    data_source = "tushare"
    single_batch = True
    update_type = "full"

    # --- 代码级默认配置 --- #
    default_concurrent_limit = 1
    default_page_size = 5000

    # 2. Tushare 特有属性
    api_name = "cn_m"
    fields = [
        "month",
        "m0",
        "m0_yoy",
        "m0_mom",
        "m1",
        "m1_yoy",
        "m1_mom",
        "m2",
        "m2_yoy",
        "m2_mom",
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {
        "m0": float,
        "m0_yoy": float,
        "m0_mom": float,
        "m1": float,
        "m1_yoy": float,
        "m1_mom": float,
        "m2": float,
        "m2_yoy": float,
        "m2_mom": float,
    }

    # 5. 表结构
    schema_def = {
        "month": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "m0": {"type": "NUMERIC(20,2)"},
        "m0_yoy": {"type": "NUMERIC(10,4)"},
        "m0_mom": {"type": "NUMERIC(10,4)"},
        "m1": {"type": "NUMERIC(20,2)"},
        "m1_yoy": {"type": "NUMERIC(10,4)"},
        "m1_mom": {"type": "NUMERIC(10,4)"},
        "m2": {"type": "NUMERIC(20,2)"},
        "m2_yoy": {"type": "NUMERIC(10,4)"},
        "m2_mom": {"type": "NUMERIC(10,4)"},
    }

    # 6. 索引
    indexes = [
        {"name": "idx_macro_cnm_month", "columns": "month"},
        {"name": "idx_macro_cnm_month_end", "columns": "month_end_date"},
        {"name": "idx_macro_cnm_update_time", "columns": "update_time"},
    ]

    # 7. 验证规则
    validations = [
        (lambda df: df["month"].notna(), "月份不能为空"),
        (
            lambda df: df["month"].astype(str).str.match(r"^\d{6}$"),
            "月份格式必须为YYYYMM",
        ),
        (lambda df: df["m0"].fillna(0) >= 0, "M0 应为非负"),
        (lambda df: df["m1"].fillna(0) >= 0, "M1 应为非负"),
        (lambda df: df["m2"].fillna(0) >= 0, "M2 应为非负"),
        (
            lambda df: df["month_end_date"].notna(),
            "month_end_date 生成失败",
        ),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """全量模式：返回单个空参数批次。"""
        self.logger.info(
            "任务 %s: 采用全量模式，单批次拉取所有 cn_m 数据", self.name
        )
        return [
            {
                "fields": ",".join(self.fields or []),
            }
        ]

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """生成 month_end_date，排序并返回。"""
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

        # 按月末日期排序，保证入库顺序
        df = df.sort_values("month_end_date").reset_index(drop=True)

        self.logger.info("任务 %s: 处理后共 %d 行数据", self.name, len(df))
        return df


