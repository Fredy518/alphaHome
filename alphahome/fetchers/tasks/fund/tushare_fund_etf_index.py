#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETF指数关联信息 (etf_index) 更新任务
获取ETF基准指数列表信息。
继承自 TushareTask。

Tushare API文档: https://tushare.pro/document/2?doc_id=386
"""

import logging
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from datetime import datetime

import pandas as pd
import numpy as np # 新增导入 numpy 用于 NaT

# 导入基础类和装饰器
from ...sources.tushare.tushare_task import TushareTask
from alphahome.common.task_system.task_decorator import task_register

# 导入批处理工具 (根据新的接口特性，可能不再需要，但暂时保留)
# from ...sources.tushare.batch_utils import generate_single_date_batches


@task_register()
class TushareFundEtfIndexTask(TushareTask):
    """获取ETF指数基准指数数据"""

    # 1. 核心属性
    name = "tushare_fund_etf_index"
    description = "获取ETF基准指数列表"
    table_name = "fund_etf_index"
    primary_keys = ["ts_code"] # primary key based on new API
    date_column = None  # 全量更新任务，无日期列
    default_start_date = "19700101" # 全量获取
    data_source = "tushare"
    # smart_lookback_days = 5  # 智能增量模式下，回看5天 (不再需要)

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1 # 调整为串行执行，因为是全量任务
    default_page_size = 5000
    update_type = "full" # 明确指定为全量更新任务类型

    # 2. TushareTask 特有属性
    api_name = "etf_index"
    # Tushare etf_index 接口返回的字段 (根据最新文档)
    fields = [
        "ts_code",          # 指数代码
        "indx_name",        # 指数全称
        "indx_csname",      # 指数简称
        "pub_party_name",   # 指数发布机构
        "pub_date",         # 指数发布日期
        "base_date",        # 指数基日
        "bp",               # 指数基点(点)
        "adj_circle",       # 指数成份证券调整周期
    ]

    # 3. 列名映射 (新API字段 -> 数据库列名)
    column_mapping = {
        "indx_name": "index_name", # 映射到数据库中可能的通用名称
        # "pub_date": "trade_date", # 映射到数据库中可能的通用日期名称 (如果date_column非None时有用)
    }

    # 4. 数据类型转换
    transformations = {
        "bp": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"}, # Increased length for ts_code
        "index_name": {"type": "VARCHAR(200)"},       # from indx_name
        "indx_csname": {"type": "VARCHAR(200)"},      # new
        "pub_party_name": {"type": "VARCHAR(100)"},
        # "trade_date": {"type": "DATE"},               # from pub_date (if mapped)
        "pub_date": {"type": "DATE"},                 # original pub_date
        "base_date": {"type": "DATE"},
        "bp": {"type": "NUMERIC(15,4)"},
        "adj_circle": {"type": "VARCHAR(50)"},
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {
            "name": "idx_fund_etf_index_ts_code",
            "columns": "ts_code",
        },
        {
            "name": "idx_fund_etf_index_pub_date",
            "columns": "pub_date",
        },
        {
            "name": "idx_fund_etf_index_update_time",
            "columns": "update_time",
        },
    ]

    # 7. 数据验证规则
    validations: Optional[List[Union[Callable, Tuple[Callable, str]]]] = [
        (lambda df: df['ts_code'].notna(), "指数代码不能为空"),
        (lambda df: df['index_name'].notna(), "指数全称不能为空"), # Changed from indx_name to index_name
        (lambda df: ~(df['ts_code'].astype(str).str.strip().eq('') | df['ts_code'].isna()), "指数代码不能为空字符串"),
        (lambda df: ~(df['index_name'].astype(str).str.strip().eq('') | df['index_name'].isna()), "指数全称不能为空字符串"), # Changed from indx_name to index_name
        (lambda df: df['bp'].fillna(0) >= 0, "指数基点不能为负数"),
        (lambda df: df['pub_date'].notna(), "发布日期不能为空"), # Added notna validation for pub_date
        (lambda df: df['base_date'].notna(), "基日不能为空"), # Added notna validation for base_date
    ]

    # 8. 分批配置 (不再需要复杂的批处理)
    # batch_trade_days_single_code = 360  # 单ETF查询时，约1年
    # batch_trade_days_all_codes = 5  # 全市场查询时，每个批次5天

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成批处理参数列表。对于 etf_index 全量获取，返回空参数字典列表。
        
        Args:
            **kwargs: 查询参数（全量更新时通常不需要）
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
        # 返回包含一个空字典的列表，触发一次不带参数的 API 调用
        return [{}]

    def process_data(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """
        处理从Tushare获取的ETF指数关联数据（重写基类扩展点）

        Args:
            data: 从API获取的原始数据
            **kwargs: 额外参数

        Returns:
            pd.DataFrame: 处理后的数据
        """
        if data is None or data.empty:
            self.logger.warning("没有ETF指数关联数据需要处理")
            return data

        # 首先调用基类的数据处理方法（应用基础转换）
        data = super().process_data(data, **kwargs)

        # ETF指数关联特定的数据处理
        if not data.empty:
            # 处理权重字段的异常值
            if 'weight' in data.columns:
                # 权重应该在0-100之间
                data.loc[data['weight'] < 0, 'weight'] = 0
                data.loc[data['weight'] > 100, 'weight'] = 100

            # 处理价格字段的异常值
            price_columns = ['close', 'change']
            for col in price_columns:
                if col in data.columns:
                    # 收盘价必须为正数
                    if col == 'close':
                        data = data[data[col] > 0]

            # 处理涨跌幅的异常值
            if 'pct_chg' in data.columns:
                # 过滤掉异常的涨跌幅（超过±50%的视为异常）
                data = data[data['pct_chg'].abs() <= 50]

            self.logger.info(f"ETF指数关联数据处理完成，共 {len(data)} 条记录")

        return data
