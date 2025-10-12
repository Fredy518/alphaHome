#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
可转债基本信息 (cb_basic) 全量更新任务
获取所有可转债的基本信息并替换数据库中的旧数据。
继承自 TushareTask。

接口文档: https://tushare.pro/document/2?doc_id=185
权限要求: 需要至少2000积分
限量: 单次最大2000，总量不限制
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareCBondBasicTask(TushareTask):
    """获取可转债基本信息 (全量更新)"""

    # 1. 核心属性
    domain = "cbond"  # 业务域标识
    name = "tushare_cbond_basic"
    description = "获取可转债基本信息"
    table_name = "cbond_basic"
    primary_keys = ["ts_code"]
    date_column = None  # 该任务不以日期为主，全量更新
    default_start_date = "19700101"  # 全量任务需要一个默认起始日期来满足基类方法调用

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 全量更新，设置为串行执行以简化
    default_page_size = 2000  # 单次最大2000条
    update_type = "full"  # 明确指定为全量更新任务类型

    # 2. TushareTask 特有属性
    api_name = "cb_basic"
    fields = [
        "ts_code",
        "bond_full_name",
        "bond_short_name",
        "cb_code",
        "stk_code",
        "stk_short_name",
        "maturity",
        "par",
        "issue_price",
        "issue_size",
        "remain_size",
        "value_date",
        "maturity_date",
        "rate_type",
        "coupon_rate",
        "add_rate",
        "pay_per_year",
        "list_date",
        "delist_date",
        "exchange",
        "conv_start_date",
        "conv_end_date",
        "conv_stop_date",
        "first_conv_price",
        "conv_price",
        "rate_clause",
        "put_clause",
        "maturity_put_price",
        "call_clause",
        "reset_clause",
        "conv_clause",
        "guarantor",
        "guarantee_type",
        "issue_rating",
        "newest_rating",
        "rating_comp",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "maturity": lambda x: pd.to_numeric(x, errors="coerce"),
        "par": lambda x: pd.to_numeric(x, errors="coerce"),
        "issue_price": lambda x: pd.to_numeric(x, errors="coerce"),
        "issue_size": lambda x: pd.to_numeric(x, errors="coerce"),
        "remain_size": lambda x: pd.to_numeric(x, errors="coerce"),
        "coupon_rate": lambda x: pd.to_numeric(x, errors="coerce"),
        "add_rate": lambda x: pd.to_numeric(x, errors="coerce"),
        "pay_per_year": lambda x: pd.to_numeric(x, errors="coerce", downcast="integer"),
        "first_conv_price": lambda x: pd.to_numeric(x, errors="coerce"),
        "conv_price": lambda x: pd.to_numeric(x, errors="coerce"),
        # 文本字段确保为字符串类型，避免float类型错误
        # 简化版：只处理基本转换，复杂逻辑交给data_transformer处理
        "bond_full_name": lambda x: str(x) if pd.notna(x) else None,
        "bond_short_name": lambda x: str(x) if pd.notna(x) else None,
        "cb_code": lambda x: str(x) if pd.notna(x) else None,
        "stk_code": lambda x: str(x) if pd.notna(x) else None,
        "stk_short_name": lambda x: str(x) if pd.notna(x) else None,
        "rate_type": lambda x: str(x) if pd.notna(x) else None,
        "exchange": lambda x: str(x) if pd.notna(x) else None,
        "rate_clause": lambda x: str(x) if pd.notna(x) else None,
        "put_clause": lambda x: str(x) if pd.notna(x) else None,
        "maturity_put_price": lambda x: str(x) if pd.notna(x) else None,  # 这个字段在Tushare中是数值，但schema定义为VARCHAR
        "call_clause": lambda x: str(x) if pd.notna(x) else None,
        "reset_clause": lambda x: str(x) if pd.notna(x) else None,
        "conv_clause": lambda x: str(x) if pd.notna(x) else None,
        "guarantor": lambda x: str(x) if pd.notna(x) else None,
        "guarantee_type": lambda x: str(x) if pd.notna(x) else None,
        "issue_rating": lambda x: str(x) if pd.notna(x) else None,
        "newest_rating": lambda x: str(x) if pd.notna(x) else None,
        "rating_comp": lambda x: str(x) if pd.notna(x) else None,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},  # 转债代码
        "bond_full_name": {"type": "VARCHAR(255)"},  # 转债名称
        "bond_short_name": {"type": "VARCHAR(100)"},  # 转债简称
        "cb_code": {"type": "VARCHAR(20)"},  # 转股申报代码
        "stk_code": {"type": "VARCHAR(20)"},  # 正股代码
        "stk_short_name": {"type": "VARCHAR(100)"},  # 正股简称
        "maturity": {"type": "NUMERIC(10,2)"},  # 发行期限（年）
        "par": {"type": "NUMERIC(15,4)"},  # 面值
        "issue_price": {"type": "NUMERIC(15,4)"},  # 发行价格
        "issue_size": {"type": "NUMERIC(20,2)"},  # 发行总额（元）
        "remain_size": {"type": "NUMERIC(20,2)"},  # 债券余额（元）
        "value_date": {"type": "DATE"},  # 起息日期
        "maturity_date": {"type": "DATE"},  # 到期日期
        "rate_type": {"type": "VARCHAR(50)"},  # 利率类型
        "coupon_rate": {"type": "NUMERIC(10,4)"},  # 票面利率（%）
        "add_rate": {"type": "NUMERIC(10,4)"},  # 补偿利率（%）
        "pay_per_year": {"type": "INTEGER"},  # 年付息次数
        "list_date": {"type": "DATE"},  # 上市日期
        "delist_date": {"type": "DATE"},  # 摘牌日
        "exchange": {"type": "VARCHAR(20)"},  # 上市地点
        "conv_start_date": {"type": "DATE"},  # 转股起始日
        "conv_end_date": {"type": "DATE"},  # 转股截止日
        "conv_stop_date": {"type": "DATE"},  # 停止转股日(提前到期)
        "first_conv_price": {"type": "NUMERIC(15,4)"},  # 初始转股价
        "conv_price": {"type": "NUMERIC(15,4)"},  # 最新转股价
        "rate_clause": {"type": "TEXT"},  # 利率说明
        "put_clause": {"type": "TEXT"},  # 赎回条款
        "maturity_put_price": {"type": "VARCHAR(255)"},  # 到期赎回价格(含税)
        "call_clause": {"type": "TEXT"},  # 回售条款
        "reset_clause": {"type": "TEXT"},  # 特别向下修正条款
        "conv_clause": {"type": "TEXT"},  # 转股条款
        "guarantor": {"type": "VARCHAR(255)"},  # 担保人
        "guarantee_type": {"type": "VARCHAR(100)"},  # 担保方式
        "issue_rating": {"type": "VARCHAR(50)"},  # 发行信用等级
        "newest_rating": {"type": "VARCHAR(50)"},  # 最新信用等级
        "rating_comp": {"type": "VARCHAR(100)"},  # 最新评级机构
        # update_time 会自动添加
        # 主键 ("ts_code") 索引由基类自动处理
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_cbond_basic_stk_code", "columns": "stk_code"},
        {"name": "idx_cbond_basic_exchange", "columns": "exchange"},
        {"name": "idx_cbond_basic_list_date", "columns": "list_date"},
        {"name": "idx_cbond_basic_maturity_date", "columns": "maturity_date"},
        {"name": "idx_cbond_basic_conv_start_date", "columns": "conv_start_date"},
        {"name": "idx_cbond_basic_conv_end_date", "columns": "conv_end_date"},
        {
            "name": "idx_cbond_basic_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。
        对于 cb_basic 全量获取，不需要分批，返回一个空参数字典的列表。
        基类的 fetch_batch 会使用这个空字典调用 Tushare API。
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # 返回包含一个空字典的列表，触发一次不带参数的 API 调用
        return [{}]

    # 7. 数据验证规则
    validations = [
        (lambda df: df['ts_code'].notna(), "转债代码不能为空"),
        (lambda df: df['bond_short_name'].notna(), "转债简称不能为空"),
        (lambda df: df['stk_code'].notna(), "正股代码不能为空"),
        (lambda df: df['stk_short_name'].notna(), "正股简称不能为空"),
        (lambda df: ~(df['bond_short_name'].astype(str).str.strip().eq('') | df['bond_short_name'].isna()), "转债简称不能为空字符串"),
        (lambda df: ~(df['stk_short_name'].astype(str).str.strip().eq('') | df['stk_short_name'].isna()), "正股简称不能为空字符串"),
        (lambda df: df['exchange'].isin(['SZ', 'SH']), "上市地点必须是SZ或SH"),
        (lambda df: df['maturity'].fillna(0) >= 0, "发行期限不能为负数"),
        (lambda df: df['par'].fillna(0) >= 0, "面值不能为负数"),
        (lambda df: df['issue_price'].fillna(0) >= 0, "发行价格不能为负数"),
        (lambda df: df['coupon_rate'].fillna(0).between(0, 100), "票面利率必须在0-100之间"),
        (lambda df: df['first_conv_price'].fillna(0) >= 0, "初始转股价不能为负数"),
        (lambda df: df['conv_price'].fillna(0) >= 0, "最新转股价不能为负数"),
    ]

    async def pre_execute(self):
        """
        预执行处理。
        对于全量更新模式，清空表数据。
        """
        self.logger.info(f"任务 {self.name}: 全量更新模式，预先清空表数据。")
        await self.clear_table()

"""
使用方法:
1. 全量更新：设置 update_type="full" 或通过GUI选择"全量更新"
   - 会清空现有数据，重新获取所有可转债基本信息
2. 权限要求：需要至少2000积分
3. 单次限量：2000条，总量不限制

注意事项:
- 只处理可转债基本信息，不包含行情数据
- 全量更新会清空现有数据并重新获取
- 数据包含转债与正股的对应关系、转股价、利率等信息
"""
