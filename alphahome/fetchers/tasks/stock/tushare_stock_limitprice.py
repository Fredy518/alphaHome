from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockLimitPriceTask(TushareTask):
    """每日涨跌停价格任务

    调用 Tushare `stk_limit` 接口，获取全市场（含 A/B 股及基金）
    在指定交易日的涨跌停价格及昨日收盘价。
    """

    # 1. 核心属性
    domain = "stock"
    name = "tushare_stock_limitprice"
    description = "获取每日涨跌停价格"
    table_name = "stock_limitprice"
    primary_keys = ["trade_date", "ts_code"]
    date_column = "trade_date"
    default_start_date = "20070101"
    smart_lookback_days = 3

    # --- 默认配置 ---
    default_concurrent_limit = 1
    default_page_size = 5000  # 单次最多提取 5000 行

    # 2. 索引
    indexes = [
        {"name": "idx_stock_limitprice_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_limitprice_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_limitprice_update_time", "columns": "update_time"},
    ]

    # 3. Tushare 特有属性
    api_name = "stk_limit"
    fields = [
        "trade_date",
        "ts_code",
        "pre_close",
        "up_limit",
        "down_limit",
    ]

    # 4. 数据类型转换
    transformations = {
        "pre_close": float,
        "up_limit": float,
        "down_limit": float,
    }

    # 5. 列名映射
    column_mapping: Dict[str, str] = {}

    # 6. 表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "pre_close": {"type": "NUMERIC(15,4)"},
        "up_limit": {"type": "NUMERIC(15,4)"},
        "down_limit": {"type": "NUMERIC(15,4)"},
    }

    # 7. 校验规则
    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["pre_close"].fillna(0) > 0, "昨日收盘价必须大于0"),
        (lambda df: df["up_limit"].fillna(0) > 0, "涨停价必须大于0"),
        (lambda df: df["down_limit"].fillna(0) > 0, "跌停价必须大于0"),
        (lambda df: df["down_limit"] <= df["up_limit"], "跌停价不得高于涨停价"),
    ]

    # 8. 批处理配置
    batch_trade_days_single_code = 240
    batch_trade_days_all_codes = 5

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """基于交易日的批处理列表。"""
        start_date: Optional[str] = kwargs.get("start_date")
        end_date: Optional[str] = kwargs.get("end_date")
        ts_code: Optional[str] = kwargs.get("ts_code")
        exchange: str = kwargs.get("exchange", "SSE")

        if not start_date:
            start_date = self.default_start_date
            self.logger.info(
                "任务 %s: 未提供 start_date，使用默认值 %s",
                self.name,
                start_date,
            )
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(
                "任务 %s: 未提供 end_date，使用当前日期 %s",
                self.name,
                end_date,
            )

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                "任务 %s: 起始日期 %s 晚于结束日期 %s，跳过执行",
                self.name,
                start_date,
                end_date,
            )
            return []

        self.logger.info(
            "任务 %s: 生成批处理列表，范围 %s-%s，股票代码=%s",
            self.name,
            start_date,
            end_date,
            ts_code if ts_code else "全部",
        )

        try:
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            batch_size = (
                self.batch_trade_days_single_code
                if ts_code
                else self.batch_trade_days_all_codes
            )

            additional_params: Dict[str, Any] = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size,
                ts_code=ts_code,
                exchange=exchange,
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info(
                "任务 %s: 成功生成 %d 个批次",
                self.name,
                len(batch_list),
            )
            return batch_list

        except Exception as exc:
            self.logger.error(
                "任务 %s: 生成批次失败: %s",
                self.name,
                exc,
                exc_info=True,
            )
            return []


