#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
申万行业成分 (分级) 数据任务 (tushare: index_member_all)

获取最新的申万行业成分数据 (is_new='Y')，并使用 UPSERT 更新到数据库。
命名遵循 tushare_index_swmember 模式。
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareIndexSwmemberTask(TushareTask):  # <-- 类名改回
    """获取申万行业成分 (分级) - tushare_index_swmember"""

    # 1. 核心属性
    name = "tushare_index_swmember"  # <-- 改回
    description = "获取最新的申万(SW)行业成分 (分级) 数据"  # 描述可以保留
    table_name = "index_swmember"  # <-- 改回
    primary_keys = ["ts_code", "l3_code", "in_date"]  # <-- 增加 in_date
    date_column = None  # 全量任务
    default_start_date = "19700101"  # 全量任务

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 2
    default_page_size = 3000

    # 2. TushareTask 特有属性
    api_name = "index_member_all"  # API 名称保持不变
    fields = [
        "l1_code",
        "l1_name",
        "l2_code",
        "l2_name",
        "l3_code",
        "l3_name",
        "ts_code",
        "name",
        "in_date",
        "out_date",
        "is_new",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {}

    # 5. 数据库表结构
    schema_def = {
        "l1_code": {"type": "VARCHAR(20)"},
        "l1_name": {"type": "VARCHAR(50)"},
        "l2_code": {"type": "VARCHAR(20)"},
        "l2_name": {"type": "VARCHAR(50)"},
        "l3_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "l3_name": {"type": "VARCHAR(100)"},
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "in_date": {"type": "DATE", "constraints": "NOT NULL"},  # <-- 增加 NOT NULL
        "out_date": {"type": "DATE"},
        "is_new": {"type": "VARCHAR(1)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_index_swmember_l1", "columns": "l1_code"},  # <-- 改回
        {"name": "idx_index_swmember_l2", "columns": "l2_code"},  # <-- 改回
        {"name": "idx_index_swmember_l3", "columns": "l3_code"},  # <-- 新增
        {
            "name": "idx_index_swmember_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        获取最新的 (is_new='Y') 和历史的 (is_new='N') 成员信息。
        返回两个批次参数，分别对应 'Y' 和 'N'。
        """
        self.logger.info(f"任务 {self.name}: 生成获取最新(Y)和历史(N)行业成分的批次。")
        return [{"is_new": "Y"}, {"is_new": "N"}]  # <-- 返回两个批次

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "股票代码不能为空"),
        (lambda df: df['l3_code'].notna(), "申万三级行业代码不能为空"),
        (lambda df: df['in_date'].notna(), "纳入日期不能为空"),
        (lambda df: ~(df['ts_code'].astype(str).str.strip().eq('') | df['ts_code'].isna()), "股票代码不能为空字符串"),
        (lambda df: ~(df['l3_code'].astype(str).str.strip().eq('') | df['l3_code'].isna()), "申万三级行业代码不能为空字符串"),
        (lambda df: ~(df['in_date'].astype(str).str.strip().eq('') | df['in_date'].isna()), "纳入日期不能为空字符串"),
    ]
