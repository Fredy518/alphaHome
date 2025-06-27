#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 中国大陆交易所交易日历任务 (tushare_others_tradecal)
获取A股及中国大陆期货交易所 (SSE, SZSE, CFFEX, SHFE, CZCE, DCE, INE) 的交易日历数据。
"""

import datetime  # 用于类型提示，实际日期操作主要在基类或 pd 中完成
from datetime import timedelta  # 可能在特定日期计算中使用，但当前版本直接获取
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# logger 实例将由 TushareTask 基类提供 (self.logger)


@task_register()
class TushareOthersTradecalTask(TushareTask):
    """获取A股及中国大陆期货交易所交易日历 (trade_cal)"""

    # 核心任务属性
    name: str = "tushare_others_tradecal"  # 更新任务名称
    description: str = "获取A股及中国大陆期货交易所交易日历 (trade_cal)"  # 更新描述
    table_name: str = "others_calendar"  # 两个日历任务共享此表
    primary_keys: List[str] = ["exchange", "cal_date"]
    date_column: Optional[str] = "cal_date"
    default_start_date: Optional[str] = "19900101"

    _EXCHANGES_TO_FETCH: List[str] = [
        "SSE",
        "SZSE",
        "CFFEX",
        "SHFE",
        "CZCE",
        "DCE",
        "INE",
    ]

    # Tushare特定属性
    api_name: str = "trade_cal"  # 固定API名称
    fields: List[str] = [
        "exchange",
        "cal_date",
        "is_open",
        "pretrade_date",
    ]  # 目标数据库字段
    column_mapping: Dict[str, str] = {}  # 大陆日历API字段名与目标一致
    transformations: Dict[str, Any] = {
        "is_open": lambda x: pd.to_numeric(
            x, errors="coerce"
        )  # 'Series' object has no attribute 'as_type'
    }

    # 数据库表结构定义 (与港股任务共享，定义应一致)
    schema_def: Dict[str, Dict[str, Any]] = {
        "exchange": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "cal_date": {"type": "DATE", "constraints": "NOT NULL"},
        "is_open": {"type": "INTEGER"},
        "pretrade_date": {"type": "DATE"},
        # update_time 会自动添加
    }

    # 数据库索引 (与港股任务共享，定义应一致以避免重复创建或冲突)
    # 注意：索引名应在项目中对共享表保持唯一且一致
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

    def get_batch_list(
        self, start_date: str, end_date: str, **kwargs: Any
    ) -> List[Dict]:
        """为每个中国大陆交易所生成批处理参数。"""
        batches: List[Dict] = []
        for ex_code in self._EXCHANGES_TO_FETCH:
            batches.append({"exchange": ex_code})
        self.logger.info(
            f"任务 {self.name}: 生成 {len(batches)} 个批次。全局日期范围: {start_date} - {end_date}"
        )
        return batches

    def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """处理从API获取的原始数据框（重写基类扩展点）"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()

        # 首先调用基类的数据处理方法（应用基础转换）
        df = super().process_data(df, **kwargs)

        # 交易日历特定的数据处理
        for field in self.fields:
            if field not in df.columns:
                if field == "exchange" and "exchange" in kwargs.get("batch_params", {}):
                    df[field] = kwargs["batch_params"]["exchange"]
                else:
                    df[field] = None
        # 确保字段顺序
        df = df[[f for f in self.fields if f in df.columns]]

        return df

    # 验证规则：使用 validations 列表（真正生效的验证机制）
    validations = [
        lambda df: df['exchange'].notna(),      # 交易所代码不能为空
        lambda df: df['cal_date'].notna(),      # 日历日期不能为空
        lambda df: df['is_open'].notna(),       # 是否开市标志不能为空
        lambda df: df['is_open'].isin([0, 1]),  # 开市标志必须是0或1
    ]
