#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 香港交易所交易日历任务 (tushare_others_hktradecal)
获取港股 (HKEX) 的交易日历数据，包含日期分块逻辑。
"""

import calendar as std_calendar  # Python 标准库 calendar
import datetime
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches

# logger 实例将由 TushareTask 基类提供 (self.logger)


@task_register()
class TushareOthersHktradecalTask(TushareTask):
    """获取港股交易日历 (hk_tradecal)"""

    # 核心任务属性
    name: str = "tushare_others_hktradecal"
    description: str = "获取港股交易日历 (hk_tradecal)"
    table_name: str = "others_calendar"
    primary_keys: List[str] = ["exchange", "cal_date"]
    date_column: Optional[str] = "cal_date"
    default_start_date: Optional[str] = "19900101"

    # Tushare特定属性
    api_name: str = "hk_tradecal"  # 固定API名称
    fields: List[str] = [
        "exchange",
        "cal_date",
        "is_open",
        "pretrade_date",
    ]  # 目标数据库字段
    # 根据用户之前反馈，假设hk_tradecal直接返回pretrade_date，故无需映射pre_trade_date
    column_mapping: Dict[str, str] = {}
    transformations: Dict[str, Any] = {
        "is_open": lambda x: pd.to_numeric(
            x, errors="coerce"
        )  # 'Series' object has no attribute 'as_type'
    }

    # 数据库表结构定义 (与大陆日历任务共享，定义应一致)
    schema_def: Dict[str, Dict[str, Any]] = {
        "exchange": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "cal_date": {"type": "DATE", "constraints": "NOT NULL"},
        "is_open": {"type": "INTEGER"},
        "pretrade_date": {"type": "DATE"},
        # update_time 会自动添加
    }

    # 数据库索引 (与大陆任务共享，定义应一致以避免重复创建或冲突)
    indexes: List[Dict[str, Any]] = [
        {
            "name": "idx_shared_cal_exch_date",
            "columns": ["exchange", "cal_date"],
            "unique": True,
        },
        {"name": "idx_shared_cal_is_open", "columns": ["is_open"]},
        {"name": "idx_shared_cal_pretrade", "columns": ["pretrade_date"]},
        {"name": "idx_shared_cal_update", "columns": ["update_time"]},
    ]

    def __init__(
        self, db_connection, api_token: Optional[str] = None, api: Optional[Any] = None
    ):
        """初始化 TushareOthersHktradecalTask."""
        super().__init__(db_connection, api_token=api_token, api=api)
        self.logger.info(
            f"任务 {self.name} 初始化完成。将从 {self.default_start_date} 开始获取数据。"
        )

    async def get_batch_list(
        self, start_date: str, end_date: str, **kwargs: Any
    ) -> List[Dict]:
        """使用自然日分块生成批次，每批2000天。"""
        batches = await generate_natural_day_batches(
            start_date, end_date, batch_size=2000, logger=self.logger
        )
        self.logger.info(
            f"任务 {self.name}: 生成 {len(batches)} 个自然日批次 (每批2000天)。全局日期范围: {start_date} - {end_date}"
        )
        return batches

    async def fetch_data_for_batch(
        self, batch_params: Dict, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """直接用批次参数的 start_date 和 end_date 调用 Tushare API。"""
        self.logger.info(
            f"任务 {self.name}: 获取数据，日期范围: {start_date} - {end_date}"
        )
        params = {"start_date": start_date, "end_date": end_date, "is_open": ""}
        try:
            df = await self.api.query(api_name=self.api_name, params=params, fields="")
            if df is None:
                self.logger.warning(
                    f"任务 {self.name}: API调用返回 None，参数: {params}"
                )
                return pd.DataFrame()
            if df.empty:
                self.logger.info(f"任务 {self.name}: API返回空数据框，参数: {params}")
            df["exchange"] = "HKEX"
            return df
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 调用 {self.api_name} 时发生错误，参数 {params}: {e}"
            )
            return pd.DataFrame()

    async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """处理从API获取的原始数据框。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()

        df = df.rename(columns=self.column_mapping, errors="ignore")
        for col_name, func in self.transformations.items():
            if col_name in df.columns:
                try:
                    df[col_name] = func(df[col_name])
                except Exception as e:
                    self.logger.warning(
                        f"任务 {self.name}: 对列 '{col_name}' 应用转换失败: {e}"
                    )

        for field in self.fields:
            if field not in df.columns:
                if field == "exchange":  # 确保 exchange 列存在
                    df[field] = "HKEX"
                else:
                    df[field] = None

        date_cols_to_process = ["cal_date", "pretrade_date"]
        for col in date_cols_to_process:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        df = df[[f for f in self.fields if f in df.columns]]
        return df

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """验证处理后的数据。"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df

        required_cols = self.primary_keys + ["is_open"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name} (HKEX): 数据验证失败 - 缺失关键字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        for pk_col in self.primary_keys:
            if df[pk_col].isnull().any():
                error_msg = f"任务 {self.name} (HKEX): 数据验证失败 - 主键列 '{pk_col}' 包含空值。"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        if "is_open" in df.columns:
            valid_is_open_values = df["is_open"].dropna()
            if not valid_is_open_values.empty:
                if not valid_is_open_values.isin([0, 1]).all():
                    self.logger.warning(
                        f"任务 {self.name} (HKEX): 列 'is_open' 包含意外值 (非0或1)。"
                    )
        return df
