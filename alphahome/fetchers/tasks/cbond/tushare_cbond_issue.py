#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
可转债发行数据 (cb_issue) 任务
获取可转债发行数据，支持全量更新和基于公告日期的增量更新。
继承自 TushareTask。

接口文档: https://tushare.pro/document/2?doc_id=186
权限要求: 需要至少2000积分
限量: 单次最大2000，总量不限制
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
class TushareCBondIssueTask(TushareTask):
    """获取可转债发行数据

    支持两种更新模式：
    1. 全量更新：获取所有可转债发行数据
    2. 增量更新：基于公告日期 (ann_date) 获取指定日期范围内的发行数据
    """

    # 1. 核心属性
    domain = "cbond"  # 业务域标识
    name = "tushare_cbond_issue"
    description = "获取可转债发行数据"
    table_name = "cbond_issue"
    primary_keys = ["ts_code", "ann_date"]  # 主键：转债代码 + 公告日期
    date_column = "ann_date"  # 增量更新使用的日期字段
    default_start_date = "19900101"  # 默认起始日期
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 1  # 默认并发限制
    default_page_size = 2000  # 单次最大2000条

    # 2. TushareTask 特有属性
    api_name = "cb_issue"
    # Tushare cb_issue 接口返回的字段 (根据文档 https://tushare.pro/document/2?doc_id=186)
    fields = [
        "ts_code",  # 转债代码
        "ann_date",  # 发行公告日
        "res_ann_date",  # 发行结果公告日
        "plan_issue_size",  # 计划发行总额（元）
        "issue_size",  # 发行总额（元）
        "issue_price",  # 发行价格
        "issue_type",  # 发行方式
        "issue_cost",  # 发行费用（元）
        "onl_code",  # 网上申购代码
        "onl_name",  # 网上申购简称
        "onl_date",  # 网上发行日期
        "onl_size",  # 网上发行总额（张）
        "onl_pch_vol",  # 网上发行有效申购数量（张）
        "onl_pch_num",  # 网上发行有效申购户数
        "onl_pch_excess",  # 网上发行超额认购倍数
        "onl_winning_rate",  # 网上发行中签率（%）
        "shd_ration_code",  # 老股东配售代码
        "shd_ration_name",  # 老股东配售简称
        "shd_ration_date",  # 老股东配售日
        "shd_ration_record_date",  # 老股东配售股权登记日
        "shd_ration_pay_date",  # 老股东配售缴款日
        "shd_ration_price",  # 老股东配售价格
        "shd_ration_ratio",  # 老股东配售比例
        "shd_ration_size",  # 老股东配售数量（张）
        "shd_ration_vol",  # 老股东配售有效申购数量（张）
        "shd_ration_num",  # 老股东配售有效申购户数
        "shd_ration_excess",  # 老股东配售超额认购倍数
        "offl_size",  # 网下发行总额（张）
        "offl_deposit",  # 网下发行定金比例（%）
        "offl_pch_vol",  # 网下发行有效申购数量（张）
        "offl_pch_num",  # 网下发行有效申购户数
        "offl_pch_excess",  # 网下发行超额认购倍数
        "offl_winning_rate",  # 网下发行中签率
        "lead_underwriter",  # 主承销商
        "lead_underwriter_vol",  # 主承销商包销数量（张）
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "plan_issue_size": float,
        "issue_size": float,
        "issue_price": float,
        "issue_cost": float,
        "onl_size": float,
        "onl_pch_vol": float,
        "onl_pch_num": int,
        "onl_pch_excess": float,
        "onl_winning_rate": float,
        "shd_ration_price": float,
        "shd_ration_ratio": float,
        "shd_ration_size": float,
        "shd_ration_vol": float,
        "shd_ration_num": int,
        "shd_ration_excess": float,
        "offl_size": float,
        "offl_deposit": float,
        "offl_pch_vol": float,
        "offl_pch_num": int,
        "offl_pch_excess": float,
        "offl_winning_rate": float,
        "lead_underwriter_vol": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},  # 转债代码
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},  # 发行公告日
        "res_ann_date": {"type": "DATE"},  # 发行结果公告日
        "plan_issue_size": {"type": "NUMERIC(20,2)"},  # 计划发行总额（元）
        "issue_size": {"type": "NUMERIC(20,2)"},  # 发行总额（元）
        "issue_price": {"type": "NUMERIC(15,4)"},  # 发行价格
        "issue_type": {"type": "VARCHAR(50)"},  # 发行方式
        "issue_cost": {"type": "NUMERIC(20,2)"},  # 发行费用（元）
        "onl_code": {"type": "VARCHAR(20)"},  # 网上申购代码
        "onl_name": {"type": "VARCHAR(100)"},  # 网上申购简称
        "onl_date": {"type": "DATE"},  # 网上发行日期
        "onl_size": {"type": "NUMERIC(20,2)"},  # 网上发行总额（张）
        "onl_pch_vol": {"type": "NUMERIC(20,2)"},  # 网上发行有效申购数量（张）
        "onl_pch_num": {"type": "INTEGER"},  # 网上发行有效申购户数
        "onl_pch_excess": {"type": "NUMERIC(15,4)"},  # 网上发行超额认购倍数
        "onl_winning_rate": {"type": "NUMERIC(10,4)"},  # 网上发行中签率（%）
        "shd_ration_code": {"type": "VARCHAR(20)"},  # 老股东配售代码
        "shd_ration_name": {"type": "VARCHAR(100)"},  # 老股东配售简称
        "shd_ration_date": {"type": "DATE"},  # 老股东配售日
        "shd_ration_record_date": {"type": "DATE"},  # 老股东配售股权登记日
        "shd_ration_pay_date": {"type": "DATE"},  # 老股东配售缴款日
        "shd_ration_price": {"type": "NUMERIC(15,4)"},  # 老股东配售价格
        "shd_ration_ratio": {"type": "NUMERIC(10,4)"},  # 老股东配售比例
        "shd_ration_size": {"type": "NUMERIC(20,2)"},  # 老股东配售数量（张）
        "shd_ration_vol": {"type": "NUMERIC(20,2)"},  # 老股东配售有效申购数量（张）
        "shd_ration_num": {"type": "INTEGER"},  # 老股东配售有效申购户数
        "shd_ration_excess": {"type": "NUMERIC(15,4)"},  # 老股东配售超额认购倍数
        "offl_size": {"type": "NUMERIC(20,2)"},  # 网下发行总额（张）
        "offl_deposit": {"type": "NUMERIC(10,4)"},  # 网下发行定金比例（%）
        "offl_pch_vol": {"type": "NUMERIC(20,2)"},  # 网下发行有效申购数量（张）
        "offl_pch_num": {"type": "INTEGER"},  # 网下发行有效申购户数
        "offl_pch_excess": {"type": "NUMERIC(15,4)"},  # 网下发行超额认购倍数
        "offl_winning_rate": {"type": "NUMERIC(10,4)"},  # 网下发行中签率
        "lead_underwriter": {"type": "VARCHAR(255)"},  # 主承销商
        "lead_underwriter_vol": {"type": "NUMERIC(20,2)"},  # 主承销商包销数量（张）
        # update_time 会自动添加
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_cbond_issue_ts_code", "columns": "ts_code"},
        {"name": "idx_cbond_issue_ann_date", "columns": "ann_date"},
        {"name": "idx_cbond_issue_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "转债代码不能为空"),
        (lambda df: df["ann_date"].notna(), "发行公告日不能为空"),
        (lambda df: df["issue_size"].fillna(0) >= 0, "发行总额不能为负数"),
        (lambda df: df["issue_price"].fillna(0) >= 0, "发行价格不能为负数"),
        (lambda df: df["onl_winning_rate"].fillna(0).between(0, 100) if "onl_winning_rate" in df.columns else True, "网上发行中签率应在0-100之间"),
        (lambda df: df["offl_winning_rate"].fillna(0).between(0, 100) if "offl_winning_rate" in df.columns else True, "网下发行中签率应在0-100之间"),
    ]

    # 8. 验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        生成批处理参数列表。

        策略说明:
        1. 全量更新模式：返回空参数字典，获取所有发行数据
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
   - 会清空现有数据，重新获取所有可转债发行数据
2. 增量更新：设置 update_type="smart" 或 "incremental"
   - 基于 ann_date（发行公告日）进行增量更新
   - 智能增量模式会自动计算日期范围（最新日期回看 smart_lookback_days 天）
   - 手动增量模式需要提供 start_date 和 end_date
3. 权限要求：需要至少2000积分
4. 单次限量：2000条，总量不限制

注意事项:
- 主键为 (ts_code, ann_date)，同一个转债可能有多次发行记录
- 增量更新不需要分批，单一批次即可
- 数据包含网上发行、网下发行、老股东配售等详细信息
"""

