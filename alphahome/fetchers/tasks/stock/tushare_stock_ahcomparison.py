from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockAHComparisonTask(TushareTask):
    """AH股比价数据任务

    调用 Tushare `stk_ah_comparison` 接口，获取 A/H 股同名公司每日的收盘价、
    涨跌幅以及比价、溢价指标，支持按交易日期或单一代码增量更新。
    """

    # 1.核心属性
    domain = "stock"
    name = "tushare_stock_ahcomparison"
    description = "获取A/H股比价及溢价数据"
    table_name = "stock_ahcomparison"
    primary_keys = ["trade_date", "ts_code", "hk_code"]
    date_column = "trade_date"
    default_start_date = "20250812"  # 接口数据起始日期
    smart_lookback_days = 3

    # --- 代码级默认配置 --- #
    default_concurrent_limit = 3
    default_page_size = 1000

    # 2.索引定义
    indexes = [
        {"name": "idx_stock_ahcomparison_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_ahcomparison_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_ahcomparison_hk_code", "columns": "hk_code"},
        {"name": "idx_stock_ahcomparison_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "stk_ah_comparison"
    fields = [
        "hk_code",
        "ts_code",
        "trade_date",
        "hk_name",
        "hk_pct_chg",
        "hk_close",
        "name",
        "close",
        "pct_chg",
        "ah_comparison",
        "ah_premium",
    ]

    # 4.数据类型转换
    transformations = {
        "hk_pct_chg": float,
        "hk_close": float,
        "close": float,
        "pct_chg": float,
        "ah_comparison": float,
        "ah_premium": float,
    }

    # 5.列名映射
    column_mapping: Dict[str, str] = {}

    # 6.表结构定义
    schema_def = {
        "hk_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "hk_name": {"type": "VARCHAR(100)"},
        "hk_pct_chg": {"type": "NUMERIC(10,4)"},
        "hk_close": {"type": "NUMERIC(15,4)"},
        "name": {"type": "VARCHAR(100)"},
        "close": {"type": "NUMERIC(15,4)"},
        "pct_chg": {"type": "NUMERIC(10,4)"},
        "ah_comparison": {"type": "NUMERIC(15,4)"},
        "ah_premium": {"type": "NUMERIC(10,4)"},
    }

    # 7.数据验证规则
    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["ts_code"].notna(), "A股代码不能为空"),
        (lambda df: df["hk_code"].notna(), "港股代码不能为空"),
        (lambda df: df["close"].fillna(0) > 0, "A股收盘价必须大于0"),
        (lambda df: df["hk_close"].fillna(0) > 0, "港股收盘价必须大于0"),
        (lambda df: df["ah_comparison"].fillna(0) > 0, "A/H比价必须大于0"),
    ]

    # 8.批处理配置
    batch_trade_days_single_code = 240
    batch_trade_days_all_codes = 5

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """生成基于交易日的批处理参数列表。"""
        start_date: Optional[str] = kwargs.get("start_date")
        end_date: Optional[str] = kwargs.get("end_date")
        ts_code: Optional[str] = kwargs.get("ts_code")
        hk_code: Optional[str] = kwargs.get("hk_code")
        exchange: str = kwargs.get("exchange", "SSE")

        if not start_date:
            start_date = self.default_start_date
            self.logger.info(
                f"任务 {self.name}: 未提供 start_date，使用默认值 {start_date}"
            )
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(
                f"任务 {self.name}: 未提供 end_date，使用当前日期 {end_date}"
            )

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"任务 {self.name}: 起始日期 {start_date} 晚于结束日期 {end_date}，无需执行任务"
            )
            return []

        self.logger.info(
            "任务 %s: 生成批处理列表，范围 %s 至 %s，A股代码=%s，港股代码=%s",
            self.name,
            start_date,
            end_date,
            ts_code if ts_code else "全部",
            hk_code if hk_code else "全部",
        )

        try:
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            batch_size = (
                self.batch_trade_days_single_code
                if (ts_code or hk_code)
                else self.batch_trade_days_all_codes
            )

            additional_params: Dict[str, Any] = {"fields": ",".join(self.fields or [])}
            if hk_code:
                additional_params["hk_code"] = hk_code

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                ts_code=ts_code,
                exchange=exchange,
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info("任务 %s: 成功生成 %d 个批次", self.name, len(batch_list))
            return batch_list

        except Exception as exc:
            self.logger.error(
                "任务 %s: 生成批次失败: %s", self.name, exc, exc_info=True
            )
            return []


