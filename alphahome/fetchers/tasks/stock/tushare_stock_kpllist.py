#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
开盘啦榜单数据 (kpl_list) 任务
获取开盘啦涨停、跌停、炸板等榜单数据。
该任务使用Tushare的kpl_list接口获取数据。

{{ AURA-X: [Create] - 基于 Tushare kpl_list API 创建开盘啦榜单数据任务. Source: tushare.pro/document/2?doc_id=347 }}
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockKplListTask(TushareTask):
    """开盘啦榜单数据任务

    获取开盘啦涨停、跌停、炸板等榜单数据，包括涨停时间、跌停时间、开板时间、
    涨停原因、板块、主力净额、竞价成交额、状态等信息。
    该任务使用Tushare的kpl_list接口获取数据。
    """

    # 1.核心属性
    domain = "stock"  # 业务域标识
    name = "tushare_stock_kpllist"
    description = "获取开盘啦榜单数据"
    table_name = "stock_kpllist"
    primary_keys = ["ts_code", "trade_date", "tag"]
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20180101"  # 开盘啦数据起始日期
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 8000  # 单次最大8000行数据

    # 2.自定义索引
    indexes = [
        {"name": "idx_stock_kpllist_code", "columns": "ts_code"},
        {"name": "idx_stock_kpllist_date", "columns": "trade_date"},
        {"name": "idx_stock_kpllist_tag", "columns": "tag"},
        {"name": "idx_stock_kpllist_theme", "columns": "theme"},
        {"name": "idx_stock_kpllist_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "kpl_list"
    # {{ AURA-X: [Define] - 根据 Tushare 文档定义字段列表 }}
    fields = [
        "ts_code",
        "name",
        "trade_date",
        "lu_time",
        "ld_time",
        "open_time",
        "last_time",
        "lu_desc",
        "tag",
        "theme",
        "net_change",
        "bid_amount",
        "status",
        "bid_change",
        "bid_turnover",
        "lu_bid_vol",
        "pct_chg",
        "bid_pct_chg",
        "rt_pct_chg",
        "limit_order",
        "amount",
        "turnover_rate",
        "free_float",
        "lu_limit_order",
    ]

    # 4.数据类型转换
    transformations = {
        "net_change": float,
        "bid_amount": float,
        "bid_change": float,
        "bid_turnover": float,
        "lu_bid_vol": float,
        "pct_chg": float,
        "bid_pct_chg": float,
        "rt_pct_chg": float,
        "limit_order": float,
        "amount": float,
        "turnover_rate": float,
        "free_float": float,
        "lu_limit_order": float,
    }

    # 5.列名映射
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(50)"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "lu_time": {"type": "VARCHAR(20)"},
        "ld_time": {"type": "VARCHAR(20)"},
        "open_time": {"type": "VARCHAR(20)"},
        "last_time": {"type": "VARCHAR(20)"},
        "lu_desc": {"type": "VARCHAR(200)"},
        "tag": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "theme": {"type": "VARCHAR(100)"},
        "net_change": {"type": "NUMERIC(20,2)"},
        "bid_amount": {"type": "NUMERIC(20,2)"},
        "status": {"type": "VARCHAR(20)"},
        "bid_change": {"type": "NUMERIC(20,2)"},
        "bid_turnover": {"type": "NUMERIC(15,4)"},
        "lu_bid_vol": {"type": "NUMERIC(20,2)"},
        "pct_chg": {"type": "NUMERIC(15,4)"},
        "bid_pct_chg": {"type": "NUMERIC(15,4)"},
        "rt_pct_chg": {"type": "NUMERIC(15,4)"},
        "limit_order": {"type": "NUMERIC(20,2)"},
        "amount": {"type": "NUMERIC(20,2)"},
        "turnover_rate": {"type": "NUMERIC(15,4)"},
        "free_float": {"type": "NUMERIC(20,2)"},
        "lu_limit_order": {"type": "NUMERIC(20,2)"},
    }

    # 7.数据验证规则 - 真正生效的验证机制
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["tag"].notna(), "标签不能为空"),
        (lambda df: df["name"].notna(), "股票名称不能为空"),
        (lambda df: ~(df["name"].astype(str).str.strip().eq("") | df["name"].isna()), "股票名称不能为空字符串"),
        (lambda df: ~(df["tag"].astype(str).str.strip().eq("") | df["tag"].isna()), "标签不能为空字符串"),
        (
            lambda df: (df["amount"].isna()) | (df["amount"] >= 0),
            "成交额不能为负数",
        ),
        (
            lambda df: (df["turnover_rate"].isna()) | (df["turnover_rate"] >= 0),
            "换手率不能为负数",
        ),
    ]

    # 8.验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    # 9. 分批配置
    batch_trade_days_full = 90  # 全量更新时，每个批次的交易日数量 (约3个月)
    batch_trade_days_incremental = 5  # 增量更新时，每个批次的交易日数量 (1周)
    
    # 10. 榜单类型配置
    tag_types = ["涨停", "炸板", "跌停", "自然涨停", "竞价"]  # 支持的榜单类型

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成基于交易日的批处理参数列表。

        使用 BatchPlanner 的 generate_trade_day_batches 方案，
        根据更新类型选择不同的批次大小，并为每种榜单类型生成批次。
        """
        from ....common.constants import UpdateTypes
        
        # 参数提取和验证
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        update_type = kwargs.get("update_type", UpdateTypes.FULL)

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(
                f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}"
            )
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 缺少必要的日期参数")
            return []

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        # 根据更新类型选择批次大小
        if update_type == UpdateTypes.FULL:
            batch_size = self.batch_trade_days_full
            self.logger.info(f"任务 {self.name}: 全量更新模式，批次大小: {batch_size} 个交易日")
        else:
            batch_size = self.batch_trade_days_incremental
            self.logger.info(f"任务 {self.name}: 增量更新模式，批次大小: {batch_size} 个交易日")

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用标准的交易日批次生成工具
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            # 为每种榜单类型生成批次
            all_batch_list = []
            for tag in self.tag_types:
                self.logger.info(f"任务 {self.name}: 为榜单类型 '{tag}' 生成批次")
                
                # 为当前榜单类型添加 tag 参数
                tag_additional_params = additional_params.copy()
                tag_additional_params["tag"] = tag
                
                batch_list = await generate_trade_day_batches(
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=batch_size,
                    ts_code=None,  # kpl_list 不需要股票代码参数
                    exchange="SSE",  # 使用上交所作为默认交易所获取交易日历
                    additional_params=tag_additional_params,
                    logger=self.logger,
                )
                
                all_batch_list.extend(batch_list)
                self.logger.info(f"任务 {self.name}: 榜单类型 '{tag}' 生成 {len(batch_list)} 个批次")

            self.logger.info(f"任务 {self.name}: 成功生成总计 {len(all_batch_list)} 个批次")
            return all_batch_list

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'stock_kpllist' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. 任务会自动注册到 TaskFactory。
5. 使用相应的运行脚本来执行此任务。

{{ AURA-X: [Note] - 本任务需要 5000 积分权限，单次最大返回 8000 行数据 }}
{{ AURA-X: [Note] - 支持涨停、炸板、跌停、自然涨停、竞价等多种榜单类型 }}
{{ AURA-X: [Note] - 全量更新：90个交易日/批次，增量更新：5个交易日/批次 }}
{{ AURA-X: [Note] - 自动为每种榜单类型生成批次，确保获取完整的榜单数据 }}
"""
