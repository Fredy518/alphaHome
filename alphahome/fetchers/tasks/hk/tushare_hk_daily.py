#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
港股股票日线行情 (hk_daily) 更新任务
获取港股的日线交易数据。
继承自 TushareTask。
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any

from ...sources.tushare.tushare_task import TushareTask
from ...task_decorator import task_register
from ...tools.batch_utils import generate_trade_day_batches

@task_register()
class TushareHKDailyTask(TushareTask):
    """获取港股日线行情数据"""

    # 1. 核心属性
    name = "tushare_hk_daily"
    description = "获取港股日线行情数据"
    table_name = "tushare_hk_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date" # 日期列名，用于确认最新数据日期
    default_start_date = "19860402" # 港股市场较早的参考日期 (恒生指数起始附近，请按实际数据源调整)
    
    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5 # 默认并发限制
    default_page_size = 5000 # Tushare hk_daily 每页最大数量 (或按实际接口调整)

    # 2. TushareTask 特有属性
    api_name = "hk_daily"
    # Tushare hk_daily 接口实际返回的字段 (请根据Tushare文档核实和调整)
    # 包括了复权因子和后复权价格
    fields = [
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", 
        "change", "pct_chg", "vol", "amount", "adj_factor",
        "open_adj", "high_adj", "low_adj", "close_adj", "vwap" # vwap (成交均价) 也是常见日线字段
    ]
    
    # 3. 列名映射 (vol -> volume)
    column_mapping = {
        "vol": "volume"
    }

    # 4. 数据类型转换
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "pre_close": float,
        "change": float,
        "pct_chg": float,
        "vol": float,  # API返回的是vol, 会被映射为volume
        "amount": float,
        "adj_factor": float,
        "open_adj": float,
        "high_adj": float,
        "low_adj": float,
        "close_adj": float,
        "vwap": float
    }
    
    # 5. 数据库表结构
    schema = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(12,4)"},
        "high": {"type": "NUMERIC(12,4)"},
        "low": {"type": "NUMERIC(12,4)"},
        "close": {"type": "NUMERIC(12,4)"},
        "pre_close": {"type": "NUMERIC(12,4)"},
        "change": {"type": "NUMERIC(12,4)"},
        "pct_chg": {"type": "NUMERIC(10,4)"},
        "volume": {"type": "NUMERIC(20,2)"},  # 港股的成交量单位可能是股
        "amount": {"type": "NUMERIC(20,3)"}, # 港股的成交额单位可能是千港元
        "adj_factor": {"type": "NUMERIC(18,6)"},
        "open_adj": {"type": "NUMERIC(12,4)"},
        "high_adj": {"type": "NUMERIC(12,4)"},
        "low_adj": {"type": "NUMERIC(12,4)"},
        "close_adj": {"type": "NUMERIC(12,4)"},
        "vwap": {"type": "NUMERIC(12,4)"}
    }

    # 6. 自定义索引
    indexes = [
        {"name": "idx_hk_daily_code_date", "columns": ["ts_code", "trade_date"], "unique": True}, # Primary key often indexed by default
        {"name": "idx_hk_daily_trade_date", "columns": "trade_date"},
        {"name": "idx_hk_daily_update_time", "columns": "update_time"}  # 新增 update_time 索引
    ]
    
    # 7. 分批配置 (根据港股接口特性和数据量调整)
    batch_trade_days_single_code = 240 * 2 # 单代码查询时，每个批次的交易日数量 (约2年)
    batch_trade_days_all_codes = 5    # 全市场查询时，每个批次的交易日数量 (5天)

    # validations (继承基类或自定义) 
    # async def validate_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    #     df = await super().validate_data(df, **kwargs)
    #     # Add HK-specific validations if needed
    #     return df

    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """生成批处理参数列表 (使用专用交易日批次工具)
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等
            
        Returns: 
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get('start_date', self.default_start_date)
        end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
        ts_code = kwargs.get('ts_code')  # 获取可能的ts_code
        # **重要**: 确保使用正确的交易所代码给 batch_utils, Tushare用 'HK' 代表港股市场日历 (hk_tradecal)
        # 如果 batch_utils 不直接支持 'HK', 可能需要适配或使用 Tushare API 的 'exchange' 参数
        # 假设 generate_trade_day_batches 的 `exchange` 参数可以接受 'HK' 或 Tushare所用港股交易所代码
        exchange_code_for_calendar = kwargs.get('exchange', 'HKEX') # 使用 'HKEX' 作为 Tushare Pro API 中 `hk_tradecal` 的交易所参数
                                                                    # 或根据 generate_trade_day_batches 的期望值调整

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(f"任务 {self.name}: 使用交易日批次工具生成批处理列表，范围: {start_date} 到 {end_date} for exchange: {exchange_code_for_calendar}")

        try:
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_trade_days_single_code if ts_code else self.batch_trade_days_all_codes,
                ts_code=ts_code,
                exchange=exchange_code_for_calendar, # 传递给工具以获取对应市场的交易日
                logger=self.logger
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True)
            return []

    # process_data 通常可以继承基类，除非有非常特殊的港股数据处理逻辑
    # async def process_data(self, df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    #     df = await super().process_data(df, **kwargs)
    #     # Add HK-specific processing if needed
    #     return df 