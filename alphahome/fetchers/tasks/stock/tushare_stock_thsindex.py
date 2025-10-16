#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
同花顺概念和行业指数 (ths_index) 全量更新任务
获取同花顺板块指数基本信息，每次执行时替换数据库中的旧数据。
继承自 TushareTask，利用 pre_execute 清空表。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockThsIndexTask(TushareTask):
    """获取同花顺概念和行业指数基本信息 (全量更新)"""

    # 1. 核心属性
    name = "tushare_stock_thsindex"
    description = "获取同花顺概念和行业指数基本信息"
    table_name = "stock_thsindex"
    primary_keys = ["ts_code"]
    date_column = None  # 全量任务
    default_start_date = "19900101"  # 全量任务，设置一个早期默认起始日期
    data_source = "tushare"
    domain = "stock"  # 业务域标识，属于股票相关数据

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1
    default_page_size = 8000

    # 2. TushareTask 特有属性
    api_name = "ths_index"
    # Tushare ths_index 接口实际返回的字段
    fields = [
        "ts_code",
        "name",
        "count",
        "exchange",
        "list_date",
        "type",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "count": lambda x: pd.to_numeric(x, errors="coerce"),
        # list_date 由覆盖的 process_data 方法处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "count": {"type": "INTEGER"},
        "exchange": {"type": "VARCHAR(10)"},
        "list_date": {"type": "DATE"},
        "type": {"type": "VARCHAR(10)"},
        # 如果 auto_add_update_time=True (默认)，则会自动添加 update_time TIMESTAMP 列
    }

    # 6. 自定义索引
    indexes = [
        # 主键 ts_code 已自动创建索引
        {"name": "idx_ths_index_exchange", "columns": "exchange"},
        {"name": "idx_ths_index_type", "columns": "type"},
        {"name": "idx_ths_index_list_date", "columns": "list_date"},
        {
            "name": "idx_ths_index_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        对于 ths_index 全量获取，不需要分批，返回一个空参数字典的列表。
        基类的 fetch_batch 会使用这个空字典调用 Tushare API。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # 返回包含一个空字典的列表，触发一次不带参数的 API 调用
        return [{}]

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "指数代码不能为空"),
        (lambda df: df['name'].notna(), "指数名称不能为空"),
        (lambda df: df['count'].notna(), "成分个数不能为空"),
        (lambda df: df['exchange'].notna(), "交易所不能为空"),
        (lambda df: df['type'].notna(), "指数类型不能为空"),
        (lambda df: ~(df['name'].astype(str).str.strip().eq('') | df['name'].isna()), "指数名称不能为空字符串"),
        (lambda df: ~(df['exchange'].astype(str).str.strip().eq('') | df['exchange'].isna()), "交易所不能为空字符串"),
        (lambda df: ~(df['type'].astype(str).str.strip().eq('') | df['type'].isna()), "指数类型不能为空字符串"),
        (lambda df: df['count'] >= 0, "成分个数不能为负数"),
    ]


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'ths_index' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. (可能需要) 在 TaskFactory 中注册此任务。
5. 使用相应的运行脚本来执行此任务。
"""
