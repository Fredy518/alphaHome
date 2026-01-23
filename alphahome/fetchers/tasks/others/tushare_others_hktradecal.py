#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 香港交易所交易日历任务 (tushare_others_hktradecal)
获取港股 (HKEX) 的交易日历数据，包含日期分块逻辑。
"""

import datetime
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes

# logger 实例将由 TushareTask 基类提供 (self.logger)


@task_register()
class TushareOthersHktradecalTask(TushareTask):
    """获取港股交易日历 (hk_tradecal)"""

    # 核心任务属性
    domain = "others"  # 业务域标识
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

    # def __init__(
    #     self, db_connection, api_token: Optional[str] = None, api: Optional[Any] = None
    # ):
    #     """初始化 TushareOthersHktradecalTask."""
    #     super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
    #     self.logger.info(
    #         f"任务 {self.name} 初始化完成。将从 {self.default_start_date} 开始获取数据。"
    #     )

    async def get_batch_list(
        self, start_date: str, end_date: str, **kwargs: Any
    ) -> List[Dict]:
        """
        生成批处理参数列表（HKEX 不分批）。

        说明：
        - hk_tradecal 数据量不大（~1-2万行），由 TushareAPI 内部分页自动处理 offset/limit，
          不需要按日期拆分批次。
        - SMART/INCREMENTAL 模式下仅传 start_date，不传 end_date，让 Tushare 返回后续全部可得日历。
        """
        update_type = kwargs.get("update_type", UpdateTypes.SMART)

        # SMART：取当前年份的首日，确保能及时发现临时调整的交易日
        # 例如今天是2026/1/23，start_date=20260101
        # 这样可以发现疫情等突发事件导致的交易日临时调整
        if update_type == UpdateTypes.SMART:
            current_year = datetime.datetime.now().year
            effective_start = f"{current_year}0101"
        # FULL：从默认起始日全量拉取（仅传 start_date）
        elif update_type == UpdateTypes.FULL:
            effective_start = self.default_start_date or "19900101"
        # MANUAL：使用外部传入的 start_date（仅传 start_date）
        else:
            effective_start = start_date or (self.default_start_date or "19900101")

        self.logger.info(
            f"任务 {self.name}: HKEX 不分批，参数: exchange=HKEX, start_date={effective_start} (end_date 不传)"
        )
        return [{"exchange": "HKEX", "start_date": effective_start}]

    async def _determine_date_range(self) -> Optional[Dict[str, str]]:
        """
        覆盖基类的日期范围判定逻辑。

        对于交易日历任务，我们不使用传统的 MAX(date) + 1 逻辑，
        而是在 get_batch_list 中按当前年份首日计算起始日期。
        """
        if self.update_type == UpdateTypes.SMART:
            # 返回一个不会被跳过的日期范围，确保能进入 get_batch_list
            return {"start_date": "19900101", "end_date": "20991231"}
        else:
            # 对于其他模式，使用基类逻辑
            return await super()._determine_date_range()

    def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """处理从API获取的原始数据框（重写基类扩展点）"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()

        # 首先调用基类的数据处理方法（应用基础转换）
        # 注意：TushareDataTransformer 已经处理了列名映射、数据转换和日期字段
        df = super().process_data(df, **kwargs)

        # 港股交易日历特定的业务逻辑处理
        # 确保 exchange 字段存在且为 HKEX
        if "exchange" not in df.columns or df["exchange"].isna().all():
            df["exchange"] = "HKEX"

        # 确保字段顺序（如果需要）
        available_fields = [f for f in self.fields if f in df.columns]
        if len(available_fields) < len(df.columns):
            df = df[available_fields]

        return df

    # 验证规则：使用 validations 列表（真正生效的验证机制）
    validations = [
        lambda df: df['exchange'].notna(),      # 交易所代码不能为空
        lambda df: df['cal_date'].notna(),      # 日历日期不能为空
        lambda df: df['is_open'].notna(),       # 是否开市标志不能为空
        lambda df: df['is_open'].isin([0, 1]),  # 开市标志必须是0或1
    ]