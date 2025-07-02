#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
港股股票基本信息 (hk_basic) 全量更新任务
每次执行时，获取所有港股的基本信息并替换数据库中的旧数据。
继承自 TushareTask。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareHKBasicTask(TushareTask):
    """获取所有港股基础信息 (全量更新)"""

    # 1. 核心属性
    domain = "hk"  # 业务域标识
    name = "tushare_hk_basic"
    description = "获取港股上市公司基本信息"
    table_name = "hk_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = "19700101"  # 全量任务，此日期仅为满足基类全量模式的日期要求，实际API调用不使用此日期

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000  # Tushare hk_basic 接口可能没有分页，但保留

    # 2. TushareTask 特有属性
    api_name = "hk_basic"
    # Tushare hk_basic 接口实际返回的字段 (请根据Tushare文档核实和调整)
    fields = [
        "ts_code",
        "name",
        "fullname",
        "enname",
        "cn_spell",
        "market",
        "list_status",
        "list_date",
        "delist_date",
        "trade_unit",
        "isin",
        "curr_type",  # 确保与API返回一致
    ]

    # 3. 列名映射 (API字段名与数据库列名一致，为空)
    column_mapping = {}

    # 4. 数据类型转换 (日期列在 process_data 中特殊处理)
    transformations = {
        # "trade_unit": int, # Example if conversion needed
        # "min_tick": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "fullname": {"type": "VARCHAR(200)"},
        "enname": {"type": "VARCHAR(255)"},
        "cn_spell": {"type": "VARCHAR(50)"},
        "market": {"type": "VARCHAR(10)"},
        "list_status": {"type": "VARCHAR(1)"},
        "list_date": {"type": "DATE"},
        "delist_date": {"type": "DATE"},
        "trade_unit": {"type": "NUMERIC(10,0)"},  # 每手股数
        "isin": {"type": "VARCHAR(20)"},  # 新增 ISIN 码, VARCHAR(12) 通常也够用
        "curr_type": {"type": "VARCHAR(10)"},  # 货币类型, 从 'currency' 修改而来
        # "min_tick": {"type": "NUMERIC(10,5)"}, # 从 fields 列表来看，此字段不由API提供，移除
        # "security_type": {"type": "VARCHAR(20)"} # 从 fields 列表来看，此字段不由API提供，移除
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_hk_basic_name", "columns": "name"},
        {"name": "idx_hk_basic_market", "columns": "market"},
        {"name": "idx_hk_basic_list_status", "columns": "list_status"},
        {"name": "idx_hk_basic_update_time", "columns": "update_time"},
        # 移除了 security_type 相关的索引
        # {"name": "idx_hk_basic_security_type", "columns": "security_type"}
    ]

    # 7. 数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['ts_code'].str.endswith('.HK'),
        lambda df: df['name'].notna(),
        lambda df: df['market'].notna(),
        lambda df: df['list_status'].isin(['L', 'D', 'P']), # L上市 D退市 P暂停上市
        lambda df: df['trade_unit'] > 0,
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 hk_basic 全量获取，返回空参数字典列表。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # Tushare hk_basic 接口可能支持按 list_status 过滤，但全量通常一次获取
        return [{}]  # 触发一次不带参数的 API 调用

