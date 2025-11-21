from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockSTTask(TushareTask):
    """ST 股票列表任务

    调用 Tushare `stock_st` 接口，获取每日 ST / *ST 股票名录，
    支持按交易日或单一股票代码增量更新。
    """

    # 1.核心属性
    domain = "stock"
    name = "tushare_stock_st"
    description = "获取每日 ST 股票列表"
    table_name = "stock_st"
    primary_keys = ["trade_date", "ts_code"]
    date_column = "trade_date"
    default_start_date = "20160101"
    smart_lookback_days = 3

    # --- 代码级默认配置 --- #
    default_concurrent_limit = 3
    default_page_size = 1000

    # 2.索引定义
    indexes = [
        {"name": "idx_stock_st_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_st_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_st_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "stock_st"
    fields = [
        "ts_code",
        "name",
        "trade_date",
        "type",
        "type_name",
    ]

    # 4.数据类型转换（全为字符串，可保持默认）
    transformations: Dict[str, Any] = {}

    # 5.列名映射
    column_mapping: Dict[str, str] = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "type": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "type_name": {"type": "VARCHAR(20)", "constraints": "NOT NULL"},
    }

    # 7.数据验证规则
    validations = [
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["name"].notna(), "股票名称不能为空"),
        (lambda df: df["type"].notna(), "ST 类型标记不能为空"),
        (lambda df: df["type_name"].notna(), "ST 类型名称不能为空"),
    ]

    # 8.批处理配置
    batch_trade_days_single_code = 240
    batch_trade_days_all_codes = 5

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """生成基于交易日的批处理参数列表。"""
        start_date: Optional[str] = kwargs.get("start_date")
        end_date: Optional[str] = kwargs.get("end_date")
        ts_code: Optional[str] = kwargs.get("ts_code")
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
                f"任务 {self.name}: 起始日期 {start_date} 晚于结束日期 {end_date}，无需执行任务。"
            )
            return []

        self.logger.info(
            "任务 %s: 生成 ST 股票批次，范围 %s-%s，股票代码=%s",
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

            self.logger.info("任务 %s: 成功生成 %d 个批次", self.name, len(batch_list))
            return batch_list

        except Exception as exc:
            self.logger.error(
                "任务 %s: 生成批次失败: %s", self.name, exc, exc_info=True
            )
            return []


