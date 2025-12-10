#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
工业生产者出厂价格指数 (cn_ppi) 数据任务

接口文档: https://tushare.pro/document/2?doc_id=237
数据说明:
- 获取PPI工业生产者出厂价格指数数据
- 单次最大5000条，一次可以提取全部数据

权限要求: 需要至少600积分
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareMacroPpiTask(TushareTask):
    """获取工业生产者出厂价格指数(PPI)数据（全量单批）"""

    # 1. 核心属性
    name = "tushare_macro_ppi"
    description = "获取工业生产者出厂价格指数(PPI)"
    table_name = "macro_ppi"
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
    api_name = "cn_ppi"
    fields = [
        "month",
        # 当月同比
        "ppi_yoy", "ppi_mp_yoy", "ppi_mp_qm_yoy", "ppi_mp_rm_yoy", "ppi_mp_p_yoy",
        "ppi_cg_yoy", "ppi_cg_f_yoy", "ppi_cg_c_yoy", "ppi_cg_adu_yoy", "ppi_cg_dcg_yoy",
        # 环比
        "ppi_mom", "ppi_mp_mom", "ppi_mp_qm_mom", "ppi_mp_rm_mom", "ppi_mp_p_mom",
        "ppi_cg_mom", "ppi_cg_f_mom", "ppi_cg_c_mom", "ppi_cg_adu_mom", "ppi_cg_dcg_mom",
        # 累计同比
        "ppi_accu", "ppi_mp_accu", "ppi_mp_qm_accu", "ppi_mp_rm_accu", "ppi_mp_p_accu",
        "ppi_cg_accu", "ppi_cg_f_accu", "ppi_cg_c_accu", "ppi_cg_adu_accu", "ppi_cg_dcg_accu",
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {field: float for field in fields if field != "month"}

    # 5. 数据库表结构
    schema_def = {
        "month": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "month_end_date": {"type": "DATE", "constraints": "NOT NULL"},
        # 当月同比
        "ppi_yoy": {"type": "NUMERIC(10,4)"},           # 全部工业品
        "ppi_mp_yoy": {"type": "NUMERIC(10,4)"},        # 生产资料
        "ppi_mp_qm_yoy": {"type": "NUMERIC(10,4)"},     # 采掘业
        "ppi_mp_rm_yoy": {"type": "NUMERIC(10,4)"},     # 原料业
        "ppi_mp_p_yoy": {"type": "NUMERIC(10,4)"},      # 加工业
        "ppi_cg_yoy": {"type": "NUMERIC(10,4)"},        # 生活资料
        "ppi_cg_f_yoy": {"type": "NUMERIC(10,4)"},      # 食品类
        "ppi_cg_c_yoy": {"type": "NUMERIC(10,4)"},      # 衣着类
        "ppi_cg_adu_yoy": {"type": "NUMERIC(10,4)"},    # 一般日用品类
        "ppi_cg_dcg_yoy": {"type": "NUMERIC(10,4)"},    # 耐用消费品类
        # 环比
        "ppi_mom": {"type": "NUMERIC(10,4)"},
        "ppi_mp_mom": {"type": "NUMERIC(10,4)"},
        "ppi_mp_qm_mom": {"type": "NUMERIC(10,4)"},
        "ppi_mp_rm_mom": {"type": "NUMERIC(10,4)"},
        "ppi_mp_p_mom": {"type": "NUMERIC(10,4)"},
        "ppi_cg_mom": {"type": "NUMERIC(10,4)"},
        "ppi_cg_f_mom": {"type": "NUMERIC(10,4)"},
        "ppi_cg_c_mom": {"type": "NUMERIC(10,4)"},
        "ppi_cg_adu_mom": {"type": "NUMERIC(10,4)"},
        "ppi_cg_dcg_mom": {"type": "NUMERIC(10,4)"},
        # 累计同比
        "ppi_accu": {"type": "NUMERIC(10,4)"},
        "ppi_mp_accu": {"type": "NUMERIC(10,4)"},
        "ppi_mp_qm_accu": {"type": "NUMERIC(10,4)"},
        "ppi_mp_rm_accu": {"type": "NUMERIC(10,4)"},
        "ppi_mp_p_accu": {"type": "NUMERIC(10,4)"},
        "ppi_cg_accu": {"type": "NUMERIC(10,4)"},
        "ppi_cg_f_accu": {"type": "NUMERIC(10,4)"},
        "ppi_cg_c_accu": {"type": "NUMERIC(10,4)"},
        "ppi_cg_adu_accu": {"type": "NUMERIC(10,4)"},
        "ppi_cg_dcg_accu": {"type": "NUMERIC(10,4)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_macro_ppi_month", "columns": "month"},
        {"name": "idx_macro_ppi_update_time", "columns": "update_time"},
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


__all__ = ["TushareMacroPpiTask"]
