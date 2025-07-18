from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np  # 添加numpy用于处理无穷大值
import pandas as pd

from ...sources.tushare import TushareTask
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockReportRcTask(TushareTask):
    """获取券商盈利预测数据任务

    数据来源: Tushare report_rc 接口 (https://tushare.pro/document/2?doc_id=292)
    """

    # 1.核心属性
    name = "tushare_stock_report_rc"
    description = "获取券商盈利预测数据"
    table_name = "stock_report_rc"
    primary_keys = ["ts_code", "report_date", "org_name", "author_name", "quarter"]
    date_column = "report_date"  # 使用报告日期作为主要日期
    default_start_date = "20050101"  # 券商盈利预测数据合理起始时间
    smart_lookback_days = 3 # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 3000

    # 2.自定义索引
    indexes = [
        {"name": "idx_reportrc_code", "columns": "ts_code"},
        {"name": "idx_reportrc_report_date", "columns": "report_date"},
        {"name": "idx_reportrc_org", "columns": "org_name"},
        {"name": "idx_reportrc_quarter", "columns": "quarter"},
        {"name": "idx_reportrc_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "report_rc"  # Tushare API 名称
    fields = [
        "ts_code",
        "name",
        "report_date",
        "report_title",
        "report_type",
        "classify",
        "org_name",
        "author_name",
        "quarter",
        "op_rt",
        "op_pr",
        "tp",
        "np",
        "eps",
        "pe",
        "rd",
        "roe",
        "ev_ebitda",
        "rating",
        "max_price",
        "min_price",
        "imp_dg",
        "create_time",
    ]

    # 4.数据类型转换
    transformations = {
        "op_rt": float,
        "op_pr": float,
        "tp": float,
        "np": float,
        "eps": float,
        "pe": float,
        "rd": float,
        "roe": float,
        "ev_ebitda": float,
        "max_price": float,
        "min_price": float,
    }

    # 5.列名映射 (No mapping needed for this API)
    column_mapping = {}

    # 6.表结构定义
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "end_date": {"type": "DATE"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "org_name": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},
        "author_name": {"type": "VARCHAR(255)", "constraints": "NOT NULL"},
        "quarter": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(50)"},
        "report_title": {"type": "TEXT"},  # Titles can be long
        "report_type": {"type": "VARCHAR(50)"},
        "classify": {"type": "VARCHAR(50)"},
        "op_rt": {"type": "NUMERIC(20,4)"},
        "op_pr": {"type": "NUMERIC(20,4)"},
        "tp": {"type": "NUMERIC(20,4)"},
        "np": {"type": "NUMERIC(20,4)"},
        "eps": {"type": "NUMERIC(20,4)"},
        "pe": {"type": "NUMERIC(20,4)"},
        "rd": {"type": "NUMERIC(20,4)"},
        "roe": {"type": "NUMERIC(20,4)"},
        "ev_ebitda": {"type": "NUMERIC(20,4)"},
        "rating": {"type": "VARCHAR(50)"},
        "max_price": {"type": "NUMERIC(20,4)"},
        "min_price": {"type": "NUMERIC(20,4)"},
        "imp_dg": {"type": "VARCHAR(50)"},
        "create_time": {"type": "TIMESTAMP"},
    }

    # 7.数据验证规则
    validations = [
        lambda df: df['ts_code'].notna(),
        lambda df: df['report_date'].notna(),
        lambda df: df['org_name'].notna(),
        lambda df: df['author_name'].notna(),
        lambda df: df['quarter'].notna(),
        lambda df: df['quarter'].str.match(r'^\d{4}Q[1-4]$'), # 季度格式应为 YYYYQ[1-4]
        lambda df: (df['max_price'] >= df['min_price']) | df['min_price'].isnull() | df['max_price'].isnull(),
        lambda df: df['roe'].between(-100, 100) | df['roe'].isnull(), # ROE应在合理范围
    ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """使用 BatchPlanner 生成批处理参数列表 (基于 report_date)

        Args:
            **kwargs: 查询参数，包括start_date、end_date (对应 report_date), ts_code, full_update等

        Returns:
            List[Dict]: 批处理参数列表
        """
        full_update = kwargs.get("full_update", False)
        ts_code = kwargs.get("ts_code")  # Allow filtering by ts_code if provided

        if full_update:
            start_date = self.default_start_date
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(
                f"任务 {self.name}: 全量更新模式，自动设置日期范围: {start_date} 到 {end_date}"
            )
        else:
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")
            if not start_date or not end_date:
                self.logger.error(
                    f"任务 {self.name}: 非全量更新模式下，必须提供 start_date 和 end_date 参数 (对应 report_date)"
                )
                return []
            self.logger.info(
                f"任务 {self.name}: 使用 BatchPlanner 生成批处理列表 (基于 report_date)，范围: {start_date} 到 {end_date}"
            )

        batch_size_days = 30  # 30天为一个批次

        try:
            # 使用标准的自然日批次生成工具
            from ...sources.tushare.batch_utils import generate_natural_day_batches

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size_days,
                ts_code=ts_code,  # 传递 ts_code (如果提供)
                additional_params=additional_params,
                logger=self.logger,
            )

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            return []
