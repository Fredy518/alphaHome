import pandas as pd
from datetime import datetime
from typing import Dict, List
from ...sources.tushare import TushareTask
from ...task_decorator import task_register
# from ...tools.calendar import get_trade_days_between # 导入交易日工具 # <-- REMOVE
from ...tools.batch_utils import generate_trade_day_batches # 导入交易日批次生成工具函数

@task_register()
class TushareStockDailyTask(TushareTask):
    """股票日线数据任务
    
    获取股票的日线交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额等信息。
    该任务使用Tushare的daily接口获取数据。
    """
    
    # 1.核心属性
    name = "tushare_stock_daily"
    description = "获取A股股票日线行情数据"
    table_name = "tushare_stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date" # 日期列名，用于确认最新数据日期
    default_start_date = "19901219" # A股最早交易日
    
    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 5 # 默认并发限制
    default_page_size = 6000

    # 2.自定义索引
    indexes = [
        {"name": "idx_stock_daily_code", "columns": "ts_code"},
        {"name": "idx_stock_daily_date", "columns": "trade_date"},
        {"name": "idx_stock_daily_update_time", "columns": "update_time"}
    ]

    # 3.Tushare特有属性
    api_name = "daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", 
              "pre_close", "change", "pct_chg", "vol", "amount"]
    
    # 4.数据类型转换
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "pre_close": float,
        "change": float,
        "pct_chg": float,
        "vol": float,  # 原始字段名
        "amount": float
    }
    
    # 5.列名映射
    column_mapping = {
        "vol": "volume"  # 将vol映射为volume
    }

    # 6.表结构定义
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(10,4)"},
        "high": {"type": "NUMERIC(10,4)"},
        "low": {"type": "NUMERIC(10,4)"},
        "close": {"type": "NUMERIC(10,4)"},
        "pre_close": {"type": "NUMERIC(10,4)"},
        "change": {"type": "NUMERIC(10,4)"},
        "pct_chg": {"type": "NUMERIC(10,4)"},
        "volume": {"type": "NUMERIC(20,4)"},  # 目标字段名
        "amount": {"type": "NUMERIC(20,4)"}
    }
    
    # 7.数据验证规则 (使用目标字段名 volume)
    # validations = [
    #     lambda df: df["close"] >= 0,
    #     lambda df: df["volume"] >= 0, # 目标字段名
    #     lambda df: df["amount"] >= 0
    # ]
    
    # 8. 分批配置
    batch_trade_days_single_code = 240 # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 5    # 全市场查询时，每个批次的交易日数量 (1周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用专用交易日批次工具)
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        from ...tools.calendar import get_trade_days_between

        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        ts_code = kwargs.get('ts_code')  # 获取可能的ts_code
        exchange = kwargs.get('exchange', 'SSE')  # 获取可能的交易所参数

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(f"任务 {self.name}: 使用交易日批次工具生成批处理列表，范围: {start_date} 到 {end_date}")

        try:
            # 使用简化的专用函数
            batch_list = await generate_trade_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=self.batch_trade_days_single_code if ts_code else self.batch_trade_days_all_codes,
                ts_code=ts_code,
                exchange=exchange,
                logger=self.logger
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成交易日批次时出错: {e}", exc_info=True)
            return []
