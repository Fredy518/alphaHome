from typing import Any, Dict, List, Optional

import pandas as pd

from ....common.constants import UpdateTypes
from ....common.task_system.task_decorator import task_register
from ...sources.tushare import TushareTask
from ...sources.tushare.batch_utils import (
    generate_stock_code_batches,
    normalize_date_range,
)


@task_register()
class TushareStockNameChangeTask(TushareTask):
    """Tushare 股票曾用名任务

    调用 Tushare `namechange` 接口，拉取上市公司历史名称变更记录，并支持
    基于公告日期的增量更新。
    """

    # 1. 核心属性
    domain = "stock"
    name = "tushare_stock_namechange"
    description = "获取上市公司历史名称变更记录"
    table_name = "stock_namechange"
    # 使用 ts_code + start_date 作为主键，避免 ann_date 为空导致主键缺失
    primary_keys = ["ts_code", "start_date"]
    # 仍然以公告日期作为增量日期字段（可能为空，需在增量逻辑中回溯一定区间）
    date_column = "ann_date"
    default_start_date = "20100101"
    smart_lookback_days = 5

    # 2. 索引定义
    indexes = [
        {"name": "idx_stock_namechange_ann_date", "columns": "ann_date"},
        {"name": "idx_stock_namechange_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_namechange_update_time", "columns": "update_time"},
    ]

    # 3. Tushare 配置
    api_name = "namechange"
    fields = [
        "ts_code",
        "name",
        "start_date",
        "end_date",
        "ann_date",
        "change_reason",
    ]

    # 4. 数据转换
    transformations: Dict[str, Any] = {}

    # 5. 列名映射
    column_mapping: Dict[str, str] = {}

    # 6. 表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},
        "start_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NULL"},
        # 公告日期可能为空，不能加 NOT NULL 约束
        "ann_date": {"type": "DATE", "constraints": "NULL"},
        "change_reason": {"type": "VARCHAR(255)", "constraints": "NULL"},
    }

    # 7. 数据验证
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["name"].notna(), "股票名称不能为空"),
        (lambda df: df["start_date"].notna(), "开始日期不能为空"),
    ]

    async def get_batch_list(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """按股票代码分批，覆盖全量/增量日期范围。

        要求：
        1. 全量更新：从 2010-01-01 起，遍历 stock_basic 中所有股票（含退市、暂停）。
        2. 按 ts_code 分批：每个批次一个股票代码，携带统一的 start_date/end_date。

        使用 batch_utils.generate_stock_code_batches 以避免在异步任务中
        直接调用同步的 DataAccess 工具导致事件循环冲突。
        """
        update_type = kwargs.get("update_type")

        start_date: Optional[str] = None
        end_date: Optional[str] = None

        include_date_filters = update_type != UpdateTypes.FULL

        if include_date_filters:
            raw_start = kwargs.get("start_date")
            raw_end = kwargs.get("end_date")
            start_date, end_date = normalize_date_range(
                start_date=raw_start,
                end_date=raw_end,
                default_start_date=self.default_start_date,
                logger=self.logger,
                task_name=self.name,
            )

            if pd.to_datetime(start_date) > pd.to_datetime(end_date):
                self.logger.info(
                    "任务 %s: 起始日期 %s 晚于结束日期 %s，跳过执行。",
                    self.name,
                    start_date,
                    end_date,
                )
                return []

        # 2）构造附加参数：统一的时间范围（仅在增量/手动时传入）和字段列表
        additional_params: Dict[str, Any] = {
            "fields": ",".join(self.fields or []),
        }
        if include_date_filters and start_date and end_date:
            additional_params["start_date"] = start_date
            additional_params["end_date"] = end_date

        # 3）调用批次工具：在智能增量模式下，优先从 stock_namechange 表获取有变更的股票代码
        try:
            if include_date_filters and start_date and end_date:
                # 智能增量模式：查询在指定日期范围内有名称变更记录的股票
                # 查询条件：ann_date 或 start_date 在指定日期范围内
                # 同时包含新上市股票（list_date 在日期范围内），确保不遗漏新股票
                filter_query = f"""
                    SELECT DISTINCT ts_code
                    FROM "tushare"."stock_namechange"
                    WHERE (
                        (ann_date IS NOT NULL AND ann_date BETWEEN '{start_date}' AND '{end_date}')
                        OR
                        (start_date BETWEEN '{start_date}' AND '{end_date}')
                    )
                    UNION
                    SELECT DISTINCT ts_code
                    FROM "tushare"."stock_basic"
                    WHERE list_date IS NOT NULL
                      AND list_date BETWEEN '{start_date}' AND '{end_date}'
                """
                self.logger.info(
                    "任务 %s: 智能增量模式，从 stock_namechange 和 stock_basic 表查询日期范围 %s-%s 内有变更或新上市的股票",
                    self.name,
                    start_date,
                    end_date,
                )
                existing_records = await self.db.fetch(filter_query)
                if existing_records:
                    # 使用查询到的股票代码列表
                    ts_codes = [row["ts_code"] for row in existing_records]
                    self.logger.info(
                        "任务 %s: 找到 %d 个在指定日期范围内有名称变更或新上市的股票，将只查询这些股票",
                        self.name,
                        len(ts_codes),
                    )
                    # 为每个股票代码创建一个批次
                    batch_list = [
                        {**additional_params, "ts_code": code} for code in ts_codes
                    ]
                else:
                    # 如果没有找到任何记录，仍然需要查询所有股票（可能是新股票或新变更）
                    self.logger.info(
                        "任务 %s: 未找到指定日期范围内的变更记录，将查询所有股票以确保完整性",
                        self.name,
                    )
                    batch_list = await generate_stock_code_batches(
                        db_connection=self.db,
                        table_name="tushare.stock_basic",
                        code_column="ts_code",
                        filter_condition=None,
                        api_instance=self.api,
                        additional_params=additional_params,
                        logger=self.logger,
                    )
            else:
                # 全量模式：查询所有股票
                batch_list = await generate_stock_code_batches(
                    db_connection=self.db,
                    table_name="tushare.stock_basic",
                    code_column="ts_code",
                    filter_condition=None,  # 不过滤，包含已退市/暂停股票
                    api_instance=self.api,
                    additional_params=additional_params,
                    logger=self.logger,
                )
        except Exception as exc:
            self.logger.error(
                "任务 %s: 生成按股票代码分批的批次失败: %s", self.name, exc, exc_info=True
            )
            return []

        if not batch_list:
            self.logger.warning("任务 %s: 未生成任何股票批次，跳过执行。", self.name)
            return []

        self.logger.info(
            "任务 %s: 成功生成 %d 个股票批次，日期范围 %s",
            self.name,
            len(batch_list),
            f"{start_date}-{end_date}"
            if include_date_filters and start_date and end_date
            else "全部历史",
        )
        return batch_list


