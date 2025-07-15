from datetime import datetime
from typing import Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from ...base.smart_batch_mixin import SmartBatchMixin
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockDailyTask(TushareTask, SmartBatchMixin):
    """股票日线数据任务

    获取股票的日线交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额等信息。
    该任务使用Tushare的daily接口获取数据。
    """

    # 1.核心属性
    domain = "stock"  # 业务域标识
    name = "tushare_stock_daily"
    description = "获取A股股票日线行情数据"
    table_name = "stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "19901219"  # A股最早交易日
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 6000

    # 2.自定义索引
    indexes = [
        {"name": "idx_stock_daily_code", "columns": "ts_code"},
        {"name": "idx_stock_daily_date", "columns": "trade_date"},
        {"name": "idx_stock_daily_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "daily"
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]

    # 4.数据类型转换
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "pre_close": float,
        "change": float,
        "pct_chg": float,
        "vol": float,  # 原始字段名
        "amount": float,
    }

    # 5.列名映射
    column_mapping = {"vol": "volume"}  # 将vol映射为volume

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "pre_close": {"type": "NUMERIC(15,4)"},
        "change": {"type": "NUMERIC(15,4)"},
        "pct_chg": {"type": "NUMERIC(10,4)"},
        "volume": {"type": "NUMERIC(20,2)"},  # 目标字段名
        "amount": {"type": "NUMERIC(20,3)"},
    }

    # 7.数据验证规则 (使用目标字段名 volume) - 真正生效的验证机制
    validations = [
        (lambda df: df["close"] > 0, "收盘价必须为正数"),
        (lambda df: df["open"] > 0, "开盘价必须为正数"),
        (lambda df: df["high"] > 0, "最高价必须为正数"),
        (lambda df: df["low"] > 0, "最低价必须为正数"),
        (lambda df: df["volume"] >= 0, "成交量不能为负数"),
        (lambda df: df["amount"] >= 0, "成交额不能为负数"),
        (lambda df: df["high"] >= df["low"], "最高价不能低于最低价"),
        (lambda df: df["high"] >= df["open"], "最高价不能低于开盘价"),
        (lambda df: df["high"] >= df["close"], "最高价不能低于收盘价"),
        (lambda df: df["low"] <= df["open"], "最低价不能高于开盘价"),
        (lambda df: df["low"] <= df["close"], "最低价不能高于收盘价"),
    ]

    # 8.验证模式配置 - 使用报告模式记录验证结果
    validation_mode = "report"  # 报告验证结果但保留所有数据

    # 8. 分批配置
    batch_trade_days_single_code = 240  # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 5  # 全市场查询时，每个批次的交易日数量 (1周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """
        使用智能批次拆分策略生成批处理参数列表。

        四级智能拆分策略：
        - ≤3个月：月度拆分（精细粒度）
        - 3个月-2年：季度拆分（平衡效率和精度）
        - 2-10年：半年度拆分（提高长期更新效率）
        - >10年：年度拆分（超长期数据优化）
        """
        # 参数提取和验证
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")
        exchange = kwargs.get("exchange", "SSE")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 缺少必要的日期参数")
            return []

        # 生成智能时间批次
        time_batches = self.generate_smart_time_batches(start_date, end_date)
        if not time_batches:
            return []

        # 转换为任务特定的API参数批次
        batches = []
        for time_batch in time_batches:
            batch = {
                "start_date": time_batch["start_date"],
                "end_date": time_batch["end_date"]
            }
            # 添加任务特有参数
            if ts_code:
                batch["ts_code"] = ts_code
            if exchange:
                batch["exchange"] = exchange

            batches.append(batch)

        # 记录优化效果
        stats = self.get_batch_optimization_stats(start_date, end_date)
        self.logger.info(
            f"任务 {self.name}: 智能批次生成完成 - "
            f"采用{stats.get('strategy', '未知')}策略，"
            f"生成 {len(batches)} 个批次，"
            f"相比原始方案减少 {stats.get('reduction_rate', 0):.1f}% 批次数量"
        )

        return batches


