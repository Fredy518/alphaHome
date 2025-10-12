#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
全球财经日历 (eco_cal) 更新任务
获取全球财经日历、包括经济事件数据。
继承自 TushareTask。
历史全量和手动增量模式使用month_range分批策略，智能增量模式使用单批次策略。
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes

# 导入批次生成工具函数
from ...sources.tushare.batch_utils import generate_quarter_range_batches


@task_register()
class TushareMacroEcocalTask(TushareTask):
    """获取全球财经日历数据"""

    # 1. 核心属性
    domain = "macro"  # 业务域标识
    name = "tushare_macro_ecocal"
    description = "获取全球财经日历、包括经济事件数据"
    table_name = "macro_ecocal"
    primary_keys = ["date", "time", "country", "event"]  # 复合主键确保唯一性
    date_column = "date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20000101"  # 默认开始时间20000101

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 默认并发限制
    default_page_size = 100  # 单次最大获取100行数据

    # 2. TushareTask 特有属性
    api_name = "eco_cal"
    # Tushare eco_cal 接口实际返回的字段 (根据Tushare文档)
    fields = ["date", "time", "currency", "country", "event", "value", "pre_value", "fore_value"]

    # 3. 列名映射 (API字段名与数据库列名一致，无需映射)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在基类处理)
    transformations = {
        "currency": str,
        "country": str, 
        "event": str
    }

    # 5. 数据库表结构
    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL"},
        "time": {"type": "VARCHAR(10)"},  # 时间格式如 "09:30"
        "currency": {"type": "TEXT"},  # 货币代码
        "country": {"type": "TEXT", "constraints": "NOT NULL"},  # 国家
        "event": {"type": "TEXT", "constraints": "NOT NULL"},  # 经济事件
        "value": {"type": "TEXT"},  # 今值
        "pre_value": {"type": "TEXT"},  # 前值
        "fore_value": {"type": "TEXT"},  # 预测值
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_ecocal_date", "columns": "date"},
        {"name": "idx_ecocal_country", "columns": "country"},
        {"name": "idx_ecocal_event", "columns": "event"},
        {"name": "idx_ecocal_update_time", "columns": "update_time"},
    ]

    # 7. 智能增量模式回看天数
    smart_lookback_days = 7

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        历史全量和手动增量模式使用month_range分批策略，
        智能增量模式直接使用单批次的start_date和end_date获取数据。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        update_type = kwargs.get("update_type", "smart")

        # 判断是否为全量模式
        is_full_mode = update_type == UpdateTypes.FULL

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，更新模式: {update_type}, 范围: {start_date} 到 {end_date}"
        )

        try:
            if is_full_mode or update_type == UpdateTypes.MANUAL:
                # 历史全量和手动增量模式：使用quarter_range分批策略，减少API调用次数
                self.logger.info(f"任务 {self.name}: 使用季度范围分批获取财经日历数据")
                return await generate_quarter_range_batches(
                    start_date=start_date,
                    end_date=end_date,
                    logger=self.logger,
                    additional_params={"fields": ",".join(self.fields)}
                )
            else:
                # 智能增量模式：直接使用单批次的start_date和end_date获取数据
                self.logger.info(f"任务 {self.name}: 智能增量模式，直接单批次获取财经日历数据")
                return [{
                    "start_date": start_date,
                    "end_date": end_date,
                    "fields": ",".join(self.fields)
                }]

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            # 抛出异常以便上层调用者感知
            raise RuntimeError(f"任务 {self.name}: 生成批次失败") from e

    def process_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        处理从API获取的原始数据（重写基类扩展点）
        """
        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            self.logger.info(
                f"任务 {self.name}: process_data 接收到空 DataFrame，跳过处理。"
            )
            return df

        # 首先调用基类的数据处理方法（应用基础转换）
        df = super().process_data(df, **kwargs)

        self.logger.info(
            f"任务 {self.name}: process_data 完成，返回 DataFrame (行数: {len(df)})。"
        )
        return df

    # 8. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['date'].notna(), "日期不能为空"),
        (lambda df: df['country'].notna(), "国家不能为空"),
        (lambda df: df['event'].notna(), "经济事件不能为空"),
        (lambda df: ~(df['date'].astype(str).str.strip().eq('') | df['date'].isna()), "日期不能为空字符串"),
        (lambda df: ~(df['country'].astype(str).str.strip().eq('') | df['country'].isna()), "国家不能为空字符串"),
        (lambda df: ~(df['event'].astype(str).str.strip().eq('') | df['event'].isna()), "经济事件不能为空字符串"),
        # 时间格式验证（可选，因为可能为空）
        (lambda df: df['time'].isna() | df['time'].astype(str).str.match(r'^\d{2}:\d{2}$|^$'), "时间格式应为HH:MM或为空"),
    ]
