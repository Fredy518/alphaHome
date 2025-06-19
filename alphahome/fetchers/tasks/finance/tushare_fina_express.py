from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

from ...sources.tushare import TushareTask
from alphahome.common.task_system.task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches
from ...tools.calendar import get_trade_days_between


@task_register()
class TushareFinaExpressTask(TushareTask):
    """股票业绩快报数据任务

    获取上市公司业绩快报数据。业绩快报是上市公司在财报发布前的初步财务数据。
    该任务使用Tushare的express接口获取数据。
    """

    # 1.核心属性
    name = "tushare_fina_express"
    description = "获取上市公司业绩快报"
    table_name = "fina_express"
    data_source = "tushare"
    primary_keys = ["ts_code", "end_date", "ann_date"]
    date_column = "end_date"
    default_start_date = "19900101"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5
    default_page_size = 5000

    # 2.自定义索引
    indexes = [
        {"name": "idx_fina_express_code", "columns": "ts_code"},
        {"name": "idx_fina_express_end_date", "columns": "end_date"},
        {"name": "idx_fina_express_ann_date", "columns": "ann_date"},
        {"name": "idx_fina_express_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "express"
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "revenue",
        "operate_profit",
        "total_profit",
        "n_income",
        "total_assets",
        "total_hldr_eqy_exc_min_int",
        "diluted_eps",
        "diluted_roe",
        "yoy_net_profit",
        "bps",
        "yoy_sales",
        "yoy_op",
        "yoy_tp",
        "yoy_dedu_np",
        "yoy_eps",
        "yoy_roe",
        "growth_assets",
        "yoy_equity",
        "growth_bps",
        "or_last_year",
        "op_last_year",
        "tp_last_year",
        "np_last_year",
        "eps_last_year",
        "open_net_assets",
        "open_bps",
        "perf_summary",
        "is_audit",
        "remark",
    ]

    # 4.数据类型转换
    transformations = {
        "revenue": float,
        "operate_profit": float,
        "total_profit": float,
        "n_income": float,
        "total_assets": float,
        "total_hldr_eqy_exc_min_int": float,
        "diluted_eps": float,
        "diluted_roe": float,
        "yoy_net_profit": float,
        "bps": float,
        "yoy_sales": float,
        "yoy_op": float,
        "yoy_tp": float,
        "yoy_dedu_np": float,
        "yoy_eps": float,
        "yoy_roe": float,
        "growth_assets": float,
        "yoy_equity": float,
        "growth_bps": float,
        "or_last_year": float,
        "op_last_year": float,
        "tp_last_year": float,
        "np_last_year": float,
        "eps_last_year": float,
        "open_net_assets": float,
        "open_bps": float,
        "perf_summary": lambda x: str(x) if pd.notna(x) else None,
        "is_audit": lambda x: int(x) if pd.notna(x) else None,
        "remark": lambda x: str(x) if pd.notna(x) else None,
    }

    # 5.列名映射
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "revenue": {"type": "NUMERIC(20,4)"},
        "operate_profit": {"type": "NUMERIC(20,4)"},
        "total_profit": {"type": "NUMERIC(20,4)"},
        "n_income": {"type": "NUMERIC(20,4)"},
        "total_assets": {"type": "NUMERIC(20,4)"},
        "total_hldr_eqy_exc_min_int": {"type": "NUMERIC(20,4)"},
        "diluted_eps": {"type": "NUMERIC(20,4)"},
        "diluted_roe": {"type": "NUMERIC(20,4)"},
        "yoy_net_profit": {"type": "NUMERIC(20,4)"},
        "bps": {"type": "NUMERIC(20,4)"},
        "yoy_sales": {"type": "NUMERIC(20,4)"},
        "yoy_op": {"type": "NUMERIC(20,4)"},
        "yoy_tp": {"type": "NUMERIC(20,4)"},
        "yoy_dedu_np": {"type": "NUMERIC(20,4)"},
        "yoy_eps": {"type": "NUMERIC(20,4)"},
        "yoy_roe": {"type": "NUMERIC(20,4)"},
        "growth_assets": {"type": "NUMERIC(20,4)"},
        "yoy_equity": {"type": "NUMERIC(20,4)"},
        "growth_bps": {"type": "NUMERIC(20,4)"},
        "or_last_year": {"type": "NUMERIC(20,4)"},
        "op_last_year": {"type": "NUMERIC(20,4)"},
        "tp_last_year": {"type": "NUMERIC(20,4)"},
        "np_last_year": {"type": "NUMERIC(20,4)"},
        "eps_last_year": {"type": "NUMERIC(20,4)"},
        "open_net_assets": {"type": "NUMERIC(20,4)"},
        "open_bps": {"type": "NUMERIC(20,4)"},
        "perf_summary": {"type": "TEXT"},
        "is_audit": {"type": "SMALLINT"},
        "remark": {"type": "TEXT"},
    }

    # 7.数据验证规则
    # validations = [
    #     lambda df: pd.to_datetime(df["end_date"], errors="coerce").notna(),
    #     lambda df: pd.to_datetime(df["ann_date"], errors="coerce").notna(),
    #     lambda df: df["revenue"].fillna(0) >= 0,  # 营业收入应为正
    #     lambda df: df["total_assets"].fillna(0) >= 0,  # 总资产应为正
    #     lambda df: df["diluted_eps"].fillna(0).abs() < 100,  # 每股收益应在合理范围内
    #     lambda df: df["diluted_roe"].fillna(0).abs() < 1,  # ROE应在合理范围内
    #     lambda df: df["yoy_sales"].fillna(0).abs() < 10,  # 销售同比增长率应在合理范围内
    #     lambda df: df["yoy_net_profit"].fillna(0).abs() < 10,  # 净利润同比增长率应在合理范围内
    #     lambda df: df["growth_assets"].fillna(0).abs() < 10  # 总资产同比增长率应在合理范围内
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
