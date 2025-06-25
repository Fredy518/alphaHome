#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票基本信息 (stock_basic) 全量更新任务
每次执行时，获取所有股票的基本信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ....common.constants import ApiParams

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockBasicTask(TushareTask):
    """获取所有股票基础信息 (全量更新)"""

    # 1. 核心属性
    name = "tushare_stock_basic"
    description = "获取上市公司基本信息"
    table_name = "stock_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = "19700101"  # 全量任务需要一个默认起始日期来满足基类方法调用

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1 # 全量更新，设置为串行执行以简化
    default_page_size = 8000
    update_type = "full" # 明确指定为全量更新任务类型

    # 2. TushareTask 特有属性
    api_name = "stock_basic"
    # Tushare stock_basic 接口实际返回的字段
    fields = [
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
        "act_name",
        "act_ent_type",
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理)
    transformations = {
        # 其他需要转换的列可以在这里添加
        # "some_numeric_string_col": lambda x: pd.to_numeric(x, errors='coerce'),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "symbol": {"type": "VARCHAR(15)"},
        "name": {"type": "VARCHAR(100)"},
        "area": {"type": "VARCHAR(50)"},
        "industry": {"type": "VARCHAR(100)"},  # 行业名称可能较长
        "fullname": {"type": "VARCHAR(255)"},
        "enname": {"type": "VARCHAR(255)"},
        "cnspell": {"type": "VARCHAR(50)"},
        "market": {"type": "VARCHAR(50)"},
        "exchange": {"type": "VARCHAR(10)"},
        "curr_type": {"type": "VARCHAR(5)"},
        "list_status": {"type": "VARCHAR(1)"},
        "list_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "is_hs": {"type": "VARCHAR(1)"},
        "act_name": {"type": "VARCHAR(100)"},
        "act_ent_type": {"type": "VARCHAR(100)"},  # 控制人性质可能较长
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_basic_name", "columns": "name"},
        {"name": "idx_stock_basic_industry", "columns": "industry"},
        {"name": "idx_stock_basic_market", "columns": "market"},
        {"name": "idx_stock_basic_update_time", "columns": "update_time"},
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 stock_basic 全量获取，返回包含不同上市状态的批次。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，按上市状态生成批次。")
        # 分别获取上市 (L), 退市 (D), 暂停上市 (P) 的股票数据
        batch_list = [
            {"list_status": ApiParams.LIST_STATUS_LISTED},
            {"list_status": ApiParams.LIST_STATUS_DELISTED},
            {"list_status": ApiParams.LIST_STATUS_PAUSED},
        ]
        return batch_list

    async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        验证从 Tushare API 获取的数据。
        - 检查 DataFrame 是否为空。
        - 检查关键业务字段 ('symbol', 'name') 是否全部为空。
        """
        if df.empty:
            self.logger.warning(
                f"任务 {self.name}: 从 API 获取的 DataFrame 为空，无需验证。"
            )
            return df

        critical_cols = ["symbol", "name"]
        missing_cols = [col for col in critical_cols if col not in df.columns]
        if missing_cols:
            error_msg = f"任务 {self.name}: 数据验证失败 - 缺失关键业务字段: {', '.join(missing_cols)}。"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 替换空字符串为 NA 以便 isnull() 检测
        df_check = df[critical_cols].replace("", pd.NA)
        if df_check.isnull().all(axis=1).all(): # type: ignore
            error_msg = f"任务 {self.name}: 数据验证失败 - 所有行的关键业务字段 ({', '.join(critical_cols)}) 均为空值。"
            self.logger.critical(error_msg)
            raise ValueError(error_msg)

        self.logger.info(
            f"任务 {self.name}: 数据验证通过，获取了 {len(df)} 条有效记录。"
        )
        return df
