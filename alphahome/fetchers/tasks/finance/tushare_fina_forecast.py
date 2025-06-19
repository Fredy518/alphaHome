from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches
from ...tools.calendar import get_trade_days_between


@task_register()
class TushareFinaForecastTask(TushareTask):
    """股票业绩预告数据任务

    获取上市公司业绩预告数据。业绩预告是上市公司对未来一个报告期业绩的预计。
    该任务使用Tushare的forecast接口获取数据。
    """

    # 1.核心属性
    name = "tushare_fina_forecast"
    description = "获取上市公司业绩预告数据"
    table_name = "fina_forecast"
    primary_keys = ["ts_code", "end_date", "ann_date"]
    date_column = "ann_date"  # 应该使用ann_date
    default_start_date = "19900101"
    data_source = "tushare"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 3000

    # 2.自定义索引
    indexes = [
        {"name": "idx_fina_forecast_code", "columns": "ts_code"},
        {"name": "idx_fina_forecast_end_date", "columns": "end_date"},
        {"name": "idx_fina_forecast_ann_date", "columns": "ann_date"},
        {"name": "idx_fina_forecast_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "forecast_vip"
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "type",
        "p_change_min",
        "p_change_max",
        "net_profit_min",
        "net_profit_max",
        "last_parent_net",
        "first_ann_date",
        "summary",
        "change_reason",
    ]

    # 4.数据类型转换
    transformations = {
        "p_change_min": float,
        "p_change_max": float,
        "net_profit_min": float,
        "net_profit_max": float,
        "last_parent_net": float,
    }

    # 5.列名映射
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "type": {"type": "VARCHAR(20)"},
        "p_change_min": {"type": "NUMERIC(20,4)"},
        "p_change_max": {"type": "NUMERIC(20,4)"},
        "net_profit_min": {"type": "NUMERIC(20,4)"},
        "net_profit_max": {"type": "NUMERIC(20,4)"},
        "last_parent_net": {"type": "NUMERIC(20,4)"},
        "first_ann_date": {"type": "DATE"},
        "summary": {"type": "TEXT"},
        "change_reason": {"type": "TEXT"},
    }

    # 7.数据验证规则
    # validations = [
    #     lambda df: pd.to_datetime(df["end_date"], errors="coerce").notna(),
    #     lambda df: pd.to_datetime(df["ann_date"], errors="coerce").notna(),
    #     lambda df: df["type"].isin(["预增", "预减", "扭亏", "首亏", "续亏", "续盈", "略增", "略减"]),
    #     lambda df: (df["p_change_max"].fillna(0) >= df["p_change_min"].fillna(0)) |
    #               (df["p_change_min"].isna() & df["p_change_max"].isna()),
    #     lambda df: (df["net_profit_max"].fillna(0) >= df["net_profit_min"].fillna(0)) |
    #               (df["net_profit_min"].isna() & df["net_profit_max"].isna())
    # ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用自然日批次工具)

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(
            f"任务 {self.name}: 使用自然日批次工具生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用自然日批次生成工具
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=365,  # 使用365天作为批次大小
                ts_code=ts_code,
                logger=self.logger,
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True
            )
            return []
