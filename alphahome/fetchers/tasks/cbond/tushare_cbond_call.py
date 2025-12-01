#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
可转债赎回信息 (cb_call) 任务
获取可转债到期赎回、强制赎回等信息，支持全量更新和基于公告日期的增量更新。
继承自 TushareTask。

接口文档: https://tushare.pro/document/2?doc_id=269
权限要求: 需要至少5000积分
限量: 单次最大2000条数据，可以根据日期循环提取
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

# 确认导入路径正确 (相对于当前文件)
from ...sources.tushare.tushare_task import TushareTask
from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareCBondCallTask(TushareTask):
    """获取可转债赎回信息

    支持两种更新模式：
    1. 全量更新：获取所有可转债赎回信息
    2. 增量更新：基于公告日期 (ann_date) 获取指定日期范围内的赎回信息
    """

    # 1. 核心属性
    domain = "cbond"  # 业务域标识
    name = "tushare_cbond_call"
    description = "获取可转债赎回信息"
    table_name = "cbond_call"
    primary_keys = ["ts_code", "ann_date"]  # 主键：转债代码 + 公告日期
    date_column = "ann_date"  # 增量更新使用的日期字段
    default_start_date = "19900101"  # 默认起始日期
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 默认并发限制
    default_page_size = 2000  # 单次最大2000条

    # 2. TushareTask 特有属性
    api_name = "cb_call"
    # Tushare cb_call 接口返回的字段 (根据文档 https://tushare.pro/document/2?doc_id=269)
    fields = [
        "ts_code",  # 转债代码
        "call_type",  # 赎回类型：到赎、强赎
        "is_call",  # 是否赎回：已满足强赎条件、公告提示强赎、公告实施强赎、公告到期赎回、公告不强赎
        "ann_date",  # 公告/提示日期
        "call_date",  # 赎回日期
        "call_price",  # 赎回价格(含税，元/张)
        "call_price_tax",  # 赎回价格(扣税，元/张)
        "call_vol",  # 赎回债券数量(张)
        "call_amount",  # 赎回金额(万元)
        "payment_date",  # 行权后款项到账日
        "call_reg_date",  # 赎回登记日
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "call_price": float,
        "call_price_tax": float,
        "call_vol": float,
        "call_amount": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},  # 转债代码
        "call_type": {"type": "VARCHAR(20)"},  # 赎回类型：到赎、强赎
        "is_call": {"type": "VARCHAR(50)"},  # 是否赎回
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},  # 公告/提示日期
        "call_date": {"type": "DATE"},  # 赎回日期
        "call_price": {"type": "NUMERIC(15,4)"},  # 赎回价格(含税，元/张)
        "call_price_tax": {"type": "NUMERIC(15,4)"},  # 赎回价格(扣税，元/张)
        "call_vol": {"type": "NUMERIC(20,2)"},  # 赎回债券数量(张)
        "call_amount": {"type": "NUMERIC(20,2)"},  # 赎回金额(万元)
        "payment_date": {"type": "DATE"},  # 行权后款项到账日
        "call_reg_date": {"type": "DATE"},  # 赎回登记日
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_cbond_call_ts_code", "columns": "ts_code"},
        {"name": "idx_cbond_call_ann_date", "columns": "ann_date"},
        {"name": "idx_cbond_call_call_date", "columns": "call_date"},
        {"name": "idx_cbond_call_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "转债代码不能为空"),
        (lambda df: df["ann_date"].notna(), "公告日期不能为空"),
        (lambda df: df["call_price"].fillna(0) >= 0 if "call_price" in df.columns else True, "赎回价格不能为负数"),
        (lambda df: df["call_vol"].fillna(0) >= 0 if "call_vol" in df.columns else True, "赎回债券数量不能为负数"),
        (lambda df: df["call_amount"].fillna(0) >= 0 if "call_amount" in df.columns else True, "赎回金额不能为负数"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        生成批处理参数列表。

        策略说明:
        1. 全量更新模式：返回空参数字典，获取所有赎回信息
        2. 增量更新模式：返回包含日期范围的参数字典，不需要分批（单一批次）
        """
        update_type = kwargs.get("update_type")

        if update_type == UpdateTypes.FULL:
            # 全量更新模式：返回空参数字典
            self.logger.info(f"任务 {self.name}: 全量获取模式，生成单一批次。")
            return [{}]
        else:
            # 增量更新模式（智能增量或手动增量）：使用 ann_date 作为日期字段
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")

            # 如果没有提供日期，使用默认值或从数据库获取最新日期
            if not start_date:
                if update_type == "smart":
                    # 智能增量模式：从数据库获取最新 ann_date，回看 smart_lookback_days 天
                    latest_date = await self.get_latest_date()
                    if latest_date:
                        from datetime import timedelta
                        start_date_obj = latest_date - timedelta(days=self.smart_lookback_days)
                        start_date = start_date_obj.strftime("%Y%m%d")
                    else:
                        start_date = self.default_start_date
                else:
                    start_date = self.default_start_date
                self.logger.info(
                    f"任务 {self.name}: 未提供 start_date，使用: {start_date}"
                )

            if not end_date:
                from datetime import datetime
                end_date = datetime.now().strftime("%Y%m%d")
                self.logger.info(
                    f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}"
                )

            # 增量模式：返回包含日期范围的单一批次
            self.logger.info(
                f"任务 {self.name}: 增量更新模式，日期范围: {start_date} ~ {end_date}"
            )
            return [
                {
                    "fields": ",".join(self.fields or []),
                    "start_date": start_date,
                    "end_date": end_date,
                }
            ]

    async def pre_execute(self, stop_event: Optional[Any] = None, **kwargs):
        """
        预执行处理。
        对于全量更新模式，清空表数据。
        """
        update_type = kwargs.get("update_type")
        if update_type == UpdateTypes.FULL:
            self.logger.info(
                f"任务 {self.name}: 全量更新模式，预先清空表数据。"
            )
            await self.clear_table()


"""
使用方法:
1. 全量更新：设置 update_type="full" 或通过GUI选择"全量更新"
   - 会清空现有数据，重新获取所有可转债赎回信息
2. 增量更新：设置 update_type="smart" 或 "incremental"
   - 基于 ann_date（公告日期）进行增量更新
   - 智能增量模式会自动计算日期范围（最新日期回看 smart_lookback_days 天）
   - 手动增量模式需要提供 start_date 和 end_date
3. 权限要求：需要至少5000积分
4. 单次限量：2000条，总量不限制

注意事项:
- 主键为 (ts_code, ann_date)，同一个转债可能有多次赎回公告
- 增量更新不需要分批，单一批次即可
- 数据包含到期赎回、强制赎回等详细信息
- 赎回类型包括：到赎、强赎
- 是否赎回状态包括：已满足强赎条件、公告提示强赎、公告实施强赎、公告到期赎回、公告不强赎
"""

