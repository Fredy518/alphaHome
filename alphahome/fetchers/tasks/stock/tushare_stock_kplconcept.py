#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
开盘啦题材库 (kpl_concept) 任务
获取开盘啦概念题材列表，每天盘后更新。
该任务使用Tushare的kpl_concept接口获取数据。

{{ AURA-X: [Create] - 基于 Tushare kpl_concept API 创建开盘啦题材库任务. Source: tushare.pro/document/2?doc_id=350 }}
"""

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockKplConceptTask(TushareTask):
    """开盘啦题材库任务

    获取开盘啦概念题材列表，包括题材代码、题材名称、涨停数量、排名上升位数等信息。
    每天盘后更新，该任务使用Tushare的kpl_concept接口获取数据。
    """

    # 1.核心属性
    domain = "stock"  # 业务域标识
    name = "tushare_stock_kplconcept"
    description = "获取开盘啦题材库数据"
    table_name = "stock_kplconcept"
    primary_keys = ["trade_date", "ts_code"]
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "20241014"  # 开盘啦题材库数据起始日期
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 5000  # 单次最大5000行数据

    # 2.自定义索引
    indexes = [
        {"name": "idx_stock_kplconcept_date", "columns": "trade_date"},
        {"name": "idx_stock_kplconcept_code", "columns": "ts_code"},
        {"name": "idx_stock_kplconcept_name", "columns": "name"},
        {"name": "idx_stock_kplconcept_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "kpl_concept"
    # {{ AURA-X: [Define] - 根据 Tushare 文档定义字段列表 }}
    fields = [
        "trade_date",
        "ts_code",
        "name",
        "z_t_num",
        "up_num",
    ]

    # 4.数据类型转换
    transformations = {
        "z_t_num": lambda x: pd.to_numeric(x, errors="coerce"),
        "up_num": lambda x: pd.to_numeric(x, errors="coerce"),
    }

    # 5.列名映射
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "z_t_num": {"type": "INTEGER"},
        "up_num": {"type": "INTEGER"},
    }

    # 7.数据验证规则 - 真正生效的验证机制
    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["ts_code"].notna(), "题材代码不能为空"),
        (lambda df: df["name"].notna(), "题材名称不能为空"),
        (lambda df: ~(df["name"].astype(str).str.strip().eq("") | df["name"].isna()), "题材名称不能为空字符串"),
        (lambda df: ~(df["ts_code"].astype(str).str.strip().eq("") | df["ts_code"].isna()), "题材代码不能为空字符串"),
        (
            lambda df: (df["z_t_num"].isna()) | (df["z_t_num"] >= 0),
            "涨停数量不能为负数",
        ),
        (
            lambda df: (df["up_num"].isna()) | (df["up_num"] >= 0),
            "排名上升位数不能为负数",
        ),
    ]

    # 8.验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    # 9. 分批配置
    batch_trade_days = 1  # 每个批次的交易日数量 (1个交易日)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成基于交易日的批处理参数列表。

        使用 BatchPlanner 的 generate_trade_day_batches 方案，
        每1个交易日为一个批次。
        """
        # 参数提取和验证
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

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
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用标准的交易日批次生成工具
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_trade_days,
                ts_code=None,  # kpl_concept 不需要股票代码参数
                exchange="SSE",  # 使用上交所作为默认交易所获取交易日历
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
3. 确保数据库中存在名为 'stock_kplconcept' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. 任务会自动注册到 TaskFactory。
5. 使用相应的运行脚本来执行此任务。

{{ AURA-X: [Note] - 本任务需要 5000 积分权限，单次最大返回 5000 行数据 }}
{{ AURA-X: [Note] - 每天盘后更新，获取最新的概念题材列表 }}
{{ AURA-X: [Note] - 使用单交易日分批，确保数据获取的准确性 }}
"""
