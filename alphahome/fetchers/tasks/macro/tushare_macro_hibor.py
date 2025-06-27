#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
香港银行同业拆借利率 (hibor) 更新任务
获取香港银行同业拆借利率HIBOR数据。
继承自 TushareTask。
使用自然日按年生成批次。
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# 导入自然日批次生成工具函数
from ...sources.tushare.batch_utils import generate_natural_day_batches


@task_register()
class TushareMacroHiborTask(TushareTask):
    """获取香港银行同业拆借利率HIBOR数据"""

    # 1. 核心属性
    name = "tushare_macro_hibor"
    description = "获取香港银行同业拆借利率HIBOR数据"
    table_name = "macro_hibor"
    primary_keys = ["date"]  # 日期是主键
    date_column = "date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20061231"  # 数据最早可获取日期，根据Tushare文档设置
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 3  # 默认并发限制
    default_page_size = 2000  # 单次最大2000行

    # 启用单批次处理模式
    single_batch = True

    # 2. TushareTask 特有属性
    api_name = "hibor"
    # Tushare hibor 接口实际返回的字段 (根据Tushare文档)
    fields = ["date", "on", "1w", "2w", "1m", "2m", "3m", "6m", "12m"]

    # 3. 列名映射 (API字段名与数据库列名不完全一致，进行映射)
    column_mapping = {}  # 字段名不需要映射

    # 4. 数据类型转换 (日期列在基类处理，数值列转换为 float)
    transformations = {
        "on": float,
        "1w": float,
        "2w": float,
        "1m": float,
        "2m": float,
        "3m": float,
        "6m": float,
        "12m": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "date": {"type": "DATE", "constraints": "NOT NULL"},
        "on": {"type": "NUMERIC(10,4)"},  # 隔夜拆借利率
        "1w": {"type": "NUMERIC(10,4)"},  # 1周拆借利率
        "2w": {"type": "NUMERIC(10,4)"},  # 2周拆借利率
        "1m": {"type": "NUMERIC(10,4)"},  # 1个月拆借利率
        "2m": {"type": "NUMERIC(10,4)"},  # 2个月拆借利率
        "3m": {"type": "NUMERIC(10,4)"},  # 3个月拆借利率
        "6m": {"type": "NUMERIC(10,4)"},  # 6个月拆借利率
        "12m": {"type": "NUMERIC(10,4)"},  # 12个月拆借利率
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_hibor_date", "columns": "date"},
        {"name": "idx_hibor_update_time", "columns": "update_time"},
    ]

    # 7. 分批配置
    # 自然日按年分批，一年365天
    batch_natural_days_year = 365# --- This __init__ was commented out for code simplification. ---
# 
# 
# def __init__(self, db_connection, api_token=None, api=None, **kwargs):
# """初始化任务"""
# super().__init__(db_connection, api_token=api_token, api=api, **kwargs)
# self.logger.info(f"任务 {self.name} 已配置初始化。")
# 
    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表，使用自然日按年分批。
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            from datetime import datetime
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        self.logger.info(
            f"任务 {self.name}: 使用自然日按年生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用自然日批次工具函数，批次大小设置为一年（约365天自然日）
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_natural_days_year,
                ts_code=None,  # 不按代码分批
                logger=self.logger,
            )
            self.logger.info(
                f"任务 {self.name}: 成功生成 {len(batch_list)} 个自然日批次。"
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True
            )
            # 抛出异常以便上层调用者感知
            raise RuntimeError(f"任务 {self.name}: 生成自然日批次失败") from e

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

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['date'].notna(), "日期不能为空"),
        (lambda df: df['on'].notna(), "隔夜利率不能为空"),
        (lambda df: ~(df['date'].astype(str).str.strip().eq('') | df['date'].isna()), "日期不能为空字符串"),
        (lambda df: df['on'] >= 0, "隔夜利率不能为负数"),
        (lambda df: df['on'] <= 50, "隔夜利率应在合理范围内（≤50%）"),
        (lambda df: df['3m'] >= 0 if '3m' in df.columns else True, "3个月利率不能为负数"),
        (lambda df: df['3m'] <= 50 if '3m' in df.columns else True, "3个月利率应在合理范围内（≤50%）"),
    ]
