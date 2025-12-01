#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AkShare 扩展任务 - 同花顺涨停原因

使用 alphahome.fetchers.sources.akshare.stock_limitup_reason_ext.stock_limitup_reason
作为 akshare 风格接口，通过 AkShareTask 统一调度与入库。
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...sources.akshare.akshare_task import AkShareTask
from ...tools.calendar import get_trade_days_between
from ....common.task_system.task_decorator import task_register
from ....common.constants import UpdateTypes


@task_register()
class AkShareStockLimitupReasonTask(AkShareTask):
    """
    同花顺涨停原因任务（AkShare 扩展接口）

    - 数据来源: http://zx.10jqka.com.cn/event/api/getharden/
    - 通过 AkShareAPI.EXTRA_FUNCS 中注册的 stock_limitup_reason 函数获取
    - 按交易日批量拉取（仅拉取交易日，跳过非交易日）
    """

    # 基本任务信息
    domain = "stock"
    name = "akshare_stock_limitup_reason"
    description = "同花顺涨停原因（AkShare 扩展接口）"
    table_name = "stock_limitup_reason"
    primary_keys = ["trade_date", "ts_code"]
    date_column = "trade_date"
    default_start_date = "20221128"
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # AkShare API 配置
    api_name = "stock_limitup_reason"
    api_params: Optional[Dict[str, Any]] = None

    # 列名映射：扩展函数已经返回英文字段，这里保持为空
    column_mapping: Optional[Dict[str, str]] = None

    # 不需要 melt 操作
    melt_config: Optional[Dict[str, Any]] = None

    # 字段类型转换
    transformations = {}

    # 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(50)"},
        "reason": {"type": "TEXT"},
        "reason_detail": {"type": "TEXT"},  # 详情页获取的完整涨停原因描述
        # update_time 由基础任务统一维护
    }

    # 索引配置
    indexes = [
        {"name": "idx_stock_limitup_reason_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_limitup_reason_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_limitup_reason_update_time", "columns": "update_time"},
    ]

    # 简单校验规则
    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
    ]

    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        生成按交易日拉取的批次列表。

        根据更新模式 (full/manual/smart) 自动确定日期范围，
        使用交易日历仅返回有效交易日。
        """
        update_type = kwargs.get("update_type", UpdateTypes.SMART)
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        if update_type == UpdateTypes.MANUAL:
            if not start_date or not end_date:
                self.logger.error("手动增量模式需要提供 start_date 和 end_date")
                return []
            self.logger.info(
                f"任务 {self.name}: 手动模式，使用指定范围 {start_date} ~ {end_date}"
            )

        elif update_type == UpdateTypes.FULL:
            start_date = self.default_start_date
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(
                f"任务 {self.name}: 全量模式，范围 {start_date} ~ {end_date}"
            )

        else:  # 默认走智能增量
            latest_date = await self.get_latest_date()
            if latest_date:
                start_dt = latest_date - timedelta(days=self.smart_lookback_days)
                default_start = datetime.strptime(self.default_start_date, "%Y%m%d").date()
                start_dt = max(start_dt, default_start)
                start_date = start_dt.strftime("%Y%m%d")
                end_date = datetime.now().strftime("%Y%m%d")
                self.logger.info(
                    f"任务 {self.name}: 智能增量，从 {start_date} 回看 {self.smart_lookback_days} 天至 {end_date}"
                )
            else:
                start_date = self.default_start_date
                end_date = datetime.now().strftime("%Y%m%d")
                self.logger.info(
                    f"任务 {self.name}: 智能增量无历史记录，使用默认范围 {start_date} ~ {end_date}"
                )

        # 转换日期格式为 YYYY-MM-DD（交易日历接口需要）
        try:
            start_str = datetime.strptime(str(start_date), "%Y%m%d").strftime("%Y-%m-%d")
            end_str = datetime.strptime(str(end_date), "%Y%m%d").strftime("%Y-%m-%d")
        except Exception as e:
            self.logger.error(f"日期解析失败: start={start_date}, end={end_date}, err={e}")
            return []

        if start_str > end_str:
            self.logger.info(
                f"任务 {self.name}: 起始日期 {start_str} 晚于结束日期 {end_str}，无需更新"
            )
            return []

        # 使用交易日历获取交易日列表（复用 batch_utils 中的工具函数）
        try:
            # 转换为 YYYYMMDD 格式（calendar 工具需要）
            start_yyyymmdd = start_str.replace("-", "")
            end_yyyymmdd = end_str.replace("-", "")
            trading_dates_raw = await get_trade_days_between(
                start_yyyymmdd, end_yyyymmdd, exchange="SSE"
            )
            # 转换为 YYYY-MM-DD 格式
            trading_dates = [
                f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in trading_dates_raw
            ]
        except Exception as e:
            self.logger.warning(f"获取交易日历失败，将使用全日期模式: {e}")
            trading_dates = []
            cur = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_str, "%Y-%m-%d").date()
            while cur <= end_dt:
                trading_dates.append(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)

        if not trading_dates:
            self.logger.info(f"任务 {self.name}: 日期范围 [{start_str} ~ {end_str}] 内无交易日")
            return []

        batches: List[Dict[str, Any]] = [{"date": d} for d in trading_dates]

        self.logger.info(
            f"任务 {self.name}: 生成涨停原因批次 {len(batches)} 个（交易日），"
            f"日期范围 [{start_str} ~ {end_str}]"
        )
        return batches


__all__ = ["AkShareStockLimitupReasonTask"]

