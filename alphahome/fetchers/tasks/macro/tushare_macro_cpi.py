#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
居民消费价格指数 (cn_cpi) 数据任务

接口文档: https://tushare.pro/document/2?doc_id=228
数据说明:
- 获取中国居民消费价格指数(CPI)数据
- 单次最大5000条，一次可以提取全部数据

权限要求: 需要至少120积分
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareMacroCpiTask(TushareTask):
    """获取中国居民消费价格指数(CPI)数据（全量单批）"""

    # 1. 核心属性
    name = "tushare_macro_cpi"
    description = "获取中国居民消费价格指数(CPI)"
    table_name = "macro_cpi"
    primary_keys = ["month"]
    date_column = None
    default_start_date = "19960101"
    data_source = "tushare"
    domain = "macro"
    single_batch = True
    update_type = "full"

    # --- 默认配置 ---
    default_concurrent_limit = 1
    default_page_size = 5000

    # 2. TushareTask 特有属性
    api_name = "cn_cpi"
    fields = [
        "month",
        "nt_val",
        "nt_yoy",
        "nt_mom",
        "nt_accu",
        "town_val",
        "town_yoy",
        "town_mom",
        "town_accu",
        "cnt_val",
        "cnt_yoy",
        "cnt_mom",
        "cnt_accu",
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {field: float for field in fields if field != "month"}

    # 5. 数据库表结构
    schema_def = {
        "month": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "nt_val": {"type": "NUMERIC(15,4)"},      # 全国当月值
        "nt_yoy": {"type": "NUMERIC(15,4)"},      # 全国同比（%）
        "nt_mom": {"type": "NUMERIC(15,4)"},      # 全国环比（%）
        "nt_accu": {"type": "NUMERIC(15,4)"},     # 全国累计值
        "town_val": {"type": "NUMERIC(15,4)"},    # 城市当月值
        "town_yoy": {"type": "NUMERIC(15,4)"},    # 城市同比（%）
        "town_mom": {"type": "NUMERIC(15,4)"},    # 城市环比（%）
        "town_accu": {"type": "NUMERIC(15,4)"},   # 城市累计值
        "cnt_val": {"type": "NUMERIC(15,4)"},     # 农村当月值
        "cnt_yoy": {"type": "NUMERIC(15,4)"},     # 农村同比（%）
        "cnt_mom": {"type": "NUMERIC(15,4)"},     # 农村环比（%）
        "cnt_accu": {"type": "NUMERIC(15,4)"},    # 农村累计值
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_macro_cpi_month", "columns": "month"},
        {"name": "idx_macro_cpi_month_end", "columns": "month_end_date"},
        {"name": "idx_macro_cpi_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["month"].notna(), "月份不能为空"),
        (lambda df: df["month"].astype(str).str.match(r"^\d{6}$"), "月份格式必须为YYYYMM"),
        (lambda df: df["month_end_date"].notna(), "month_end_date 生成失败"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """全量模式：返回单个空参数批次"""
        self.logger.info(f"任务 {self.name}: 采用全量模式，单批次拉取所有数据")
        return [{"fields": ",".join(self.fields or [])}]

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """生成 month_end_date，排序并返回"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df

        df = super().process_data(df, **kwargs)

        if "month" not in df.columns:
            self.logger.error(f"任务 {self.name}: 缺少 month 列")
            return pd.DataFrame()

        def _month_to_end_date(month_str: Optional[str]):
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
            self.logger.warning(f"任务 {self.name}: {invalid} 行 month_end_date 转换失败")
            df = df[df["month_end_date"].notna()].copy()

        df = df.sort_values("month_end_date").reset_index(drop=True)
        self.logger.info(f"任务 {self.name}: 处理后共 {len(df)} 行数据")
        return df


__all__ = ["TushareMacroCpiTask"]
