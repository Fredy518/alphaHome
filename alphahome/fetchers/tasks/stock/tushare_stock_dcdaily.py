#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
东财概念板块行情 (dc_daily) 任务
获取东财概念板块、行业指数板块、地域板块行情数据，历史数据开始于2020年。
该任务使用Tushare的dc_daily接口获取数据。

{{ AURA-X: [Create] - 基于 Tushare dc_daily API 创建东财概念板块行情任务. Source: tushare.pro/document/2?doc_id=382 }}
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockDcDailyTask(TushareTask):
    """东财概念板块行情数据任务

    获取东财概念板块、行业指数板块、地域板块的日线交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额等信息。
    该任务使用Tushare的dc_daily接口获取数据。
    """

    # 1.核心属性
    domain = "stock"  # 业务域标识
    name = "tushare_stock_dcdaily"
    description = "获取东财概念板块行情数据"
    table_name = "stock_dcdaily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20200101"  # 东财概念板块数据起始日期（2020年开始）
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 2000  # 单次最大2000行数据

    # 2.自定义索引
    indexes = [
        {"name": "idx_stock_dcdaily_code", "columns": "ts_code"},
        {"name": "idx_stock_dcdaily_date", "columns": "trade_date"},
        {"name": "idx_stock_dcdaily_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "dc_daily"
    # {{ AURA-X: [Define] - 根据 Tushare 文档定义字段列表 }}
    fields = [
        "ts_code",
        "trade_date",
        "close",
        "open",
        "high",
        "low",
        "change",
        "pct_change",
        "vol",
        "amount",
        "swing",
        "turnover_rate",
    ]

    # 4.数据类型转换
    transformations = {
        "close": float,
        "open": float,
        "high": float,
        "low": float,
        "change": float,
        "pct_change": float,
        "vol": float,
        "amount": float,
        "swing": float,
        "turnover_rate": float,
    }

    # 5.列名映射
    column_mapping = {"vol": "volume"}  # 将vol映射为volume

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "close": {"type": "NUMERIC(15,4)"},
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "change": {"type": "NUMERIC(15,4)"},
        "pct_change": {"type": "NUMERIC(15,4)"},
        "volume": {"type": "NUMERIC(20,2)"},
        "amount": {"type": "NUMERIC(20,3)"},
        "swing": {"type": "NUMERIC(15,4)"},
        "turnover_rate": {"type": "NUMERIC(15,4)"},
    }

    # 7.数据验证规则 - 真正生效的验证机制
    validations = [
        (lambda df: df["close"] > 0, "收盘点位必须为正数"),
        (lambda df: df["open"] > 0, "开盘点位必须为正数"),
        (lambda df: df["high"] > 0, "最高点位必须为正数"),
        (lambda df: df["low"] > 0, "最低点位必须为正数"),
        (lambda df: df["volume"] >= 0, "成交量不能为负数"),
        (lambda df: df["amount"] >= 0, "成交额不能为负数"),
        (lambda df: df["high"] >= df["low"], "最高点位不能低于最低点位"),
        (lambda df: df["high"] >= df["open"], "最高点位不能低于开盘点位"),
        (lambda df: df["high"] >= df["close"], "最高点位不能低于收盘点位"),
        (lambda df: df["low"] <= df["open"], "最低点位不能高于开盘点位"),
        (lambda df: df["low"] <= df["close"], "最低点位不能高于收盘点位"),
        (
            lambda df: (df["turnover_rate"].isna()) | (df["turnover_rate"] >= 0),
            "换手率不能为负数",
        ),
        (
            lambda df: (df["swing"].isna()) | (df["swing"] >= 0),
            "振幅不能为负数",
        ),
    ]

    # 8.验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    # 9. 分批配置
    batch_trade_days_single_code = 240  # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 5  # 全市场查询时，每个批次的交易日数量 (1周)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成基于交易日的批处理参数列表。

        使用 BatchPlanner 的 generate_trade_day_batches 方案，
        根据是否指定板块代码选择不同的批次大小。
        """
        # 参数提取和验证
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")
        exchange = kwargs.get("exchange", "SSE")

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

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 板块代码: {ts_code if ts_code else '所有'}"
        )

        try:
            # 使用标准的交易日批次生成工具
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            # 根据是否有指定板块代码选择不同的批次大小
            batch_size = (
                self.batch_trade_days_single_code
                if ts_code
                else self.batch_trade_days_all_codes
            )

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                ts_code=ts_code,
                exchange=exchange,
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'stock_dcdaily' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. 任务会自动注册到 TaskFactory。
5. 使用相应的运行脚本来执行此任务。

{{ AURA-X: [Note] - 本任务需要 6000 积分权限，单次最大返回 2000 行数据 }}
{{ AURA-X: [Note] - 支持概念板块、行业板块、地域板块三种类型 }}
"""

