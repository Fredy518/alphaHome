#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
东方财富概念板块 (dc_index) 任务
获取东方财富每个交易日的概念板块数据，支持按日期查询。
该任务使用Tushare的dc_index接口获取数据。

{{ AURA-X: [Create] - 基于 Tushare dc_index API 创建东方财富概念板块任务. Source: tushare.pro/document/2?doc_id=362 }}
"""

import logging
from typing import Any, Dict, List

import pandas as pd

# 确认导入路径正确
from ...sources.tushare.tushare_task import TushareTask
from ....common.task_system.task_decorator import task_register

# logger 由 Task 基类提供
# logger = logging.getLogger(__name__)


@task_register()
class TushareStockDcIndexTask(TushareTask):
    """获取东方财富概念板块数据"""

    # 1. 核心属性
    name = "tushare_stock_dcindex"
    description = "获取东方财富概念板块数据"
    table_name = "stock_dcindex"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 增量任务
    default_start_date = "20241220"  # 根据东方财富数据起始时间设置
    data_source = "tushare"
    domain = "stock"  # 业务域标识，属于股票相关数据

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 3
    default_page_size = 5000  # 单次最大可获取5000条数据

    # 2. TushareTask 特有属性
    api_name = "dc_index"
    # Tushare dc_index 接口实际返回的字段
    fields = [
        "ts_code",
        "trade_date", 
        "name",
        "leading",
        "leading_code",
        "pct_change",
        "leading_pct",
        "total_mv",
        "turnover_rate",
        "up_num",
        "down_num",
    ]

    # 3. 列名映射
    column_mapping = {}

    # 4. 数据类型转换
    transformations = {
        "pct_change": lambda x: pd.to_numeric(x, errors="coerce"),
        "leading_pct": lambda x: pd.to_numeric(x, errors="coerce"),
        "total_mv": lambda x: pd.to_numeric(x, errors="coerce"),
        "turnover_rate": lambda x: pd.to_numeric(x, errors="coerce"),
        "up_num": lambda x: pd.to_numeric(x, errors="coerce"),
        "down_num": lambda x: pd.to_numeric(x, errors="coerce"),
        # trade_date 由覆盖的 process_data 方法处理
    }

    # 5. 数据库表结构
    schema_def = {
        "ts_code": {"type": "VARCHAR(30)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "name": {"type": "VARCHAR(100)"},
        "leading": {"type": "VARCHAR(50)"},
        "leading_code": {"type": "VARCHAR(20)"},
        "pct_change": {"type": "DECIMAL(10,4)"},
        "leading_pct": {"type": "DECIMAL(10,4)"},
        "total_mv": {"type": "DECIMAL(20,2)"},
        "turnover_rate": {"type": "DECIMAL(10,4)"},
        "up_num": {"type": "INTEGER"},
        "down_num": {"type": "INTEGER"},
        # 如果 auto_add_update_time=True (默认)，则会自动添加 update_time TIMESTAMP 列
    }

    # 6. 自定义索引
    indexes = [
        # 复合主键 (ts_code, trade_date) 已自动创建索引
        {"name": "idx_dc_index_trade_date", "columns": "trade_date"},
        {"name": "idx_dc_index_name", "columns": "name"},
        {"name": "idx_dc_index_leading_code", "columns": "leading_code"},
        {
            "name": "idx_dc_index_update_time",
            "columns": "update_time",
        },  # 新增 update_time 索引
    ]

    # 8. 分批配置
    batch_trade_days = 5  # 每个批次的交易日数量 (5个交易日)

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成基于交易日的批处理参数列表。
        
        使用 BatchPlanner 的 generate_trade_day_batches 方案，
        每5个交易日为一个批次。
        """
        # 参数提取和验证
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            from datetime import datetime
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 缺少必要的日期参数")
            return []

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用标准的交易日批次生成工具
            from ...sources.tushare.batch_utils import generate_trade_day_batches

            # 准备附加参数
            additional_params = {"fields": ",".join(self.fields or [])}

            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_trade_days,
                ts_code=None,  # dc_index 不需要股票代码参数
                exchange="SSE",  # 使用上交所作为默认交易所获取交易日历
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

    # 7. 数据验证规则 (真正生效的验证机制)
    validations = [
        (lambda df: df['ts_code'].notna(), "概念代码不能为空"),
        (lambda df: df['trade_date'].notna(), "交易日期不能为空"),
        (lambda df: df['name'].notna(), "概念名称不能为空"),
        (lambda df: ~(df['name'].astype(str).str.strip().eq('') | df['name'].isna()), "概念名称不能为空字符串"),
        (lambda df: df['pct_change'].notna(), "涨跌幅不能为空"),
        (lambda df: df['total_mv'].notna(), "总市值不能为空"),
        (lambda df: df['turnover_rate'].notna(), "换手率不能为空"),
        (lambda df: df['up_num'].notna(), "上涨家数不能为空"),
        (lambda df: df['down_num'].notna(), "下降家数不能为空"),
        (lambda df: df['up_num'] >= 0, "上涨家数不能为负数"),
        (lambda df: df['down_num'] >= 0, "下降家数不能为负数"),
    ]


"""
使用方法:
1. 确保 TaskFactory 和 BaseTask 正确实现或导入。
2. 确保数据库连接 (self.db_engine) 和 Tushare 客户端 (self.tushare_client) 在基类或 TaskFactory 中正确初始化。
3. 确保数据库中存在名为 'stock_dcindex' 的表，且结构与 Tushare 返回的 DataFrame 匹配。
4. (可能需要) 在 TaskFactory 中注册此任务。
5. 使用相应的运行脚本来执行此任务。

注意事项:
- 此接口需要用户积累6000积分才能调取
- 单次最大可获取5000条数据，历史数据可根据日期循环获取
- 支持按日期查询，适合增量更新
- 数据包含概念板块的涨跌幅、领涨股票、市值等信息
"""
