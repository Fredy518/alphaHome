#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Tushare 龙虎榜机构交易明细数据任务

接口文档: https://tushare.pro/document/2?doc_id=107
数据说明:
- 获取龙虎榜机构成交明细数据
- 包含营业部名称、买入额、卖出额、净买入额等
- side: 0-买入金额最大的前5名，1-卖出金额最大的前5名
- API 必填参数为 trade_date，需按交易日分批获取

权限要求: 需要至少5000积分
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...sources.tushare.tushare_task import TushareTask
from ...sources.tushare.batch_utils import generate_single_date_batches
from ....common.task_system.task_decorator import task_register


@task_register()
class TushareStockTopInstTask(TushareTask):
    """获取龙虎榜机构交易明细数据 (top_inst)

    实现要求:
    - API 必填参数为 trade_date
    - 全量/增量更新: 按交易日分批获取数据
    """

    # 1. 核心属性
    name = "tushare_stock_topinst"
    description = "获取龙虎榜机构交易明细数据"
    table_name = "stock_topinst"
    primary_keys = ["ts_code", "trade_date", "exalter", "side"]
    date_column = "trade_date"
    default_start_date = "20050101"
    data_source = "tushare"
    domain = "stock"
    smart_lookback_days = 3

    # --- 默认配置 ---
    default_concurrent_limit = 1
    default_page_size = 5000
    default_max_retries = 5       # 网络超时时增加重试次数
    default_retry_delay = 10      # 网络超时后等待更长时间再重试

    # 2. TushareTask 特有属性
    api_name = "top_inst"
    fields = [
        "trade_date",    # 交易日期
        "ts_code",       # TS代码
        "exalter",       # 营业部名称
        "side",          # 买卖类型 0:买入金额最大的前5名 1:卖出金额最大的前5名
        "buy",           # 买入额（元）
        "buy_rate",      # 买入占总成交比例
        "sell",          # 卖出额（元）
        "sell_rate",     # 卖出占总成交比例
        "net_buy",       # 净成交额（元）
        "reason",        # 上榜理由
    ]

    # 3. 列名映射
    column_mapping: Dict[str, str] = {}

    # 4. 数据类型转换
    transformations = {
        "buy": float,
        "buy_rate": float,
        "sell": float,
        "sell_rate": float,
        "net_buy": float,
    }

    # 5. 数据库表结构
    schema_def = {
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "exalter": {"type": "VARCHAR(200)", "constraints": "NOT NULL"},
        "side": {"type": "VARCHAR(5)", "constraints": "NOT NULL"},
        "buy": {"type": "NUMERIC(20,2)"},
        "buy_rate": {"type": "NUMERIC(10,6)"},
        "sell": {"type": "NUMERIC(20,2)"},
        "sell_rate": {"type": "NUMERIC(10,6)"},
        "net_buy": {"type": "NUMERIC(20,2)"},
        "reason": {"type": "VARCHAR(200)"},
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_stock_topinst_ts_code", "columns": "ts_code"},
        {"name": "idx_stock_topinst_trade_date", "columns": "trade_date"},
        {"name": "idx_stock_topinst_exalter", "columns": "exalter"},
        {"name": "idx_stock_topinst_side", "columns": "side"},
        {"name": "idx_stock_topinst_update_time", "columns": "update_time"},
    ]

    # 7. 数据验证规则
    validations = [
        (lambda df: df["ts_code"].notna(), "股票代码不能为空"),
        (lambda df: df["trade_date"].notna(), "交易日期不能为空"),
        (lambda df: df["exalter"].notna(), "营业部名称不能为空"),
        (lambda df: df["side"].notna(), "买卖类型不能为空"),
        (lambda df: df["side"].astype(str).isin(["0", "1"]), "买卖类型必须为0(买入榜)或1(卖出榜)"),
        (lambda df: (df["buy"] >= 0) | df["buy"].isna(), "买入额必须非负"),
        (lambda df: (df["sell"] >= 0) | df["sell"].isna(), "卖出额必须非负"),
    ]

    # 8. 验证模式
    validation_mode = "report"

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表
        
        API 必填参数为 trade_date，因此按交易日分批获取数据
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        current_date = datetime.now().strftime("%Y%m%d")

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表 - start_date: {start_date}, end_date: {end_date}"
        )

        try:
            # 确定起止日期
            if not start_date:
                latest_db_date = await self.get_latest_date()
                if latest_db_date:
                    start_date = (latest_db_date + timedelta(days=1)).strftime("%Y%m%d")
                else:
                    start_date = self.default_start_date
                self.logger.info(f"任务 {self.name}: 使用起始日期: {start_date}")

            if not end_date:
                end_date = current_date
                self.logger.info(f"任务 {self.name}: 使用结束日期: {end_date}")

            if datetime.strptime(str(start_date), "%Y%m%d") > datetime.strptime(str(end_date), "%Y%m%d"):
                self.logger.info(f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，跳过执行")
                return []

            # 按交易日分批获取数据
            self.logger.info(f"按交易日分批获取龙虎榜机构数据: {start_date} ~ {end_date}")
            return await generate_single_date_batches(
                start_date=start_date,
                end_date=end_date,
                date_field="trade_date",
                logger=self.logger,
                exchange=kwargs.get("exchange", "SSE"),
                additional_params={"fields": ",".join(self.fields or [])}
            )

        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True)
            return []


__all__ = ["TushareStockTopInstTask"]
