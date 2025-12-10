#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
公募基金经理 (fund_manager) 数据任务

接口文档: https://tushare.pro/document/2?doc_id=208
数据说明:
- 获取公募基金经理数据，包括基金经理简历等数据
- 单次最大5000条，支持分页提取数据

权限要求: 需要至少500积分，2000积分以上可提高访问频次
"""

from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareFundManagerTask(TushareTask):
    """获取公募基金经理数据 (fund_manager)"""

    # 1. 核心属性
    name = "tushare_fund_manager"
    description = "获取公募基金经理数据"
    table_name = "fund_manager"
    primary_keys = ["ts_code", "name", "begin_date"]
    date_column = "ann_date"
    default_start_date = "19900101"
    data_source = "tushare"
    domain = "fund"

    # --- 默认配置 ---
    default_concurrent_limit = 1
    default_page_size = 5000

    # 2. TushareTask 特有属性
    api_name = "fund_manager"
    fields = [
        "ts_code",      # 基金代码
        "ann_date",     # 公告日期
        "name",         # 基金经理姓名
        "gender",       # 性别
        "birth_year",   # 出生年份
        "edu",          # 学历
        "nationality",  # 国籍
        "begin_date",   # 任职日期
        "end_date",     # 离任日期
        "resume",       # 简历
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {}

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "name": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "gender": {"type": "VARCHAR(10)"},
        "birth_year": {"type": "VARCHAR(10)"},
        "edu": {"type": "VARCHAR(50)"},
        "nationality": {"type": "VARCHAR(50)"},
        "begin_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE"},
        "resume": {"type": "TEXT"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_fund_manager_ts_code", "columns": "ts_code"},
        {"name": "idx_fund_manager_name", "columns": "name"},
        {"name": "idx_fund_manager_ann_date", "columns": "ann_date"},
        {"name": "idx_fund_manager_begin_date", "columns": "begin_date"},
        {"name": "idx_fund_manager_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "基金代码不能为空"),
        (lambda df: df["name"].notna(), "基金经理姓名不能为空"),
    ]

    # 8. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        fund_manager 接口支持分页，直接使用空参数让 API 自动分页获取全量数据。
        """
        self.logger.info(f"任务 {self.name}: 全量获取基金经理数据")

        # 单批次获取，依赖 API 层的自动分页
        return [{}]


__all__ = ["TushareFundManagerTask"]
