#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
财报披露计划 (disclosure_date) 更新任务
获取上市公司财报的预计披露日期和实际披露日期。
继承自 TushareTask，按季度获取数据。
"""

from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional

import pandas as pd
from dateutil.relativedelta import relativedelta

from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from alphahome.common.constants import UpdateTypes
from ...sources.tushare.batch_utils import generate_quarter_end_batches


@task_register()
class TushareFinaDisclosureTask(TushareTask):
    """获取上市公司财报披露计划数据"""

    # 1. 核心属性
    name = "tushare_fina_disclosure"
    description = "获取上市公司财报披露计划数据"
    table_name = "fina_disclosure"
    primary_keys = [
        "ts_code",
        "ann_date",
        "end_date",
    ]  # 财报披露计划数据，按季度获取，有可能更改原披露日期
    date_column = "end_date"  # 财报周期日期作为主要日期列
    default_start_date = "19901231"  # 开始日期
    commit = True  # 数据插入后自动提交事务

    # 智能增量配置：使用180天回溯覆盖季度数据
    smart_lookback_days = 180  # 6个月回溯，确保覆盖季度数据更新

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 3000

    # 2. 自定义索引
    indexes = [
        {"name": "idx_fina_disclosure_code", "columns": "ts_code"},
        {"name": "idx_fina_disclosure_ann_date", "columns": "ann_date"},
        {"name": "idx_fina_disclosure_end_date", "columns": "end_date"},
        {"name": "idx_fina_disclosure_pre_date", "columns": "pre_date"},
        {"name": "idx_fina_disclosure_actual_date", "columns": "actual_date"},
        {"name": "idx_fina_disclosure_update_time", "columns": "update_time"},
    ]

    # 3. Tushare特有属性
    api_name = "disclosure_date"
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "pre_date",
        "actual_date",
        "modify_date",
    ]

    # 4. 数据类型转换
    transformations = {
        # 日期字段由基类的 process_data 方法自动处理
    }

    # 5. 列名映射
    column_mapping = {}

    # 6. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "pre_date": {"type": "DATE", "constraints": ""},
        "actual_date": {"type": "DATE", "constraints": ""},
        "modify_date": {"type": "DATE", "constraints": ""},
    }

    async def _determine_date_range(self) -> Optional[Dict[str, str]]:
        """
        季度对齐的智能增量更新

        重写基类方法以实现财报披露数据的季度特定逻辑：
        - 智能增量：从最新数据的季度开始回溯90天，确保覆盖季度数据更新
        - 日期对齐到季度边界，符合财报披露的季度特性
        """
        self.logger.info(f"任务 {self.name}: 正在确定季度对齐的智能增量日期范围...")

        if self.update_type == UpdateTypes.SMART:
            latest_date_in_db = await self.get_latest_date_for_task()

            if latest_date_in_db:
                # 从最新日期回溯90天，然后对齐到季度开始
                lookback_date = latest_date_in_db - timedelta(days=self.smart_lookback_days)
                start_dt = self._get_quarter_start(lookback_date)

                # 确保不早于默认起始日期
                default_start_dt = datetime.strptime(self.default_start_date, "%Y%m%d").date()
                start_dt = max(start_dt, default_start_dt)

                self.logger.info(
                    f"任务 {self.name}: 找到最新日期 {latest_date_in_db}，从季度开始日期 {start_dt} 开始更新"
                )
            else:
                start_dt = datetime.strptime(self.default_start_date, "%Y%m%d").date()
                self.logger.info(f"任务 {self.name}: 未找到数据，从默认起始日期 {start_dt} 开始更新")

            # 结束日期对齐到当前季度末
            end_dt = self._get_current_quarter_end()

            if start_dt > end_dt:
                self.logger.info(f"任务 {self.name}: 数据已是最新，无需更新")
                return None

            return {
                "start_date": start_dt.strftime("%Y%m%d"),
                "end_date": end_dt.strftime("%Y%m%d"),
            }
        else:
            # 对于非智能增量模式，使用基类的标准处理
            return await super()._determine_date_range()

    def _get_quarter_start(self, date_obj: date) -> date:
        """获取指定日期所在季度的开始日期"""
        quarter_month = ((date_obj.month - 1) // 3) * 3 + 1
        return date_obj.replace(month=quarter_month, day=1)

    def _get_current_quarter_end(self) -> date:
        """获取当前季度的结束日期"""
        current_date = datetime.now().date()
        quarter_month_end = ((current_date.month - 1) // 3 + 1) * 3
        return (datetime(current_date.year, quarter_month_end, 1) +
                relativedelta(months=1) - relativedelta(days=1)).date()

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成按季度的批处理参数列表

        使用 batch_utils.generate_quarter_end_batches 生成按季度的批次。
        对于财报披露数据，按季度获取是最合适的方式，因为财报是按季度发布的。

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表，每个批次包含一个季度的结束日期
        """
        # 获取起止日期参数
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        self.logger.info(
            f"任务 {self.name}: 使用季度批次工具生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用 generate_quarter_end_batches 生成季度批次
            # 这个函数生成的批次参数中使用 'period' 作为日期参数的键名
            quarter_batches = await generate_quarter_end_batches(
                start_date=start_date,
                end_date=end_date,
                ts_code=ts_code,
                logger=self.logger,
            )

            # 将 'period' 参数映射为 'end_date' 参数（因为 disclosure_date API 使用 end_date 参数）
            batch_list = []
            for batch in quarter_batches:
                # 提取 'period' 值作为 'end_date'
                if "end_date" in batch:
                    batch_params = {"end_date": batch["end_date"]}
                    # 复制其他参数
                    for key, value in batch.items():
                        if key != "end_date":
                            batch_params[key] = value
                    batch_list.append(batch_params)

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个季度批次")
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成季度批次时出错: {e}", exc_info=True
            )
            return []

    def _process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从API获取的原始数据。
        此方法可以被子类覆盖以实现特定的数据转换逻辑。
        """
        # 首先调用父类的通用处理逻辑 (如果它存在且做了有用的事)
        df = super()._process_data(df, **kwargs)

        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df

        # 确保没有空的字符串在日期列（处理成NULL更合适）
        for date_col in ["ann_date", "pre_date", "actual_date", "modify_date"]:
            if date_col in df.columns:
                df[date_col] = df[date_col].replace("", None)

        # 返回处理后的数据
        return df
