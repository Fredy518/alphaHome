import pandas as pd
from datetime import datetime
from typing import Dict, List
from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.calendar import get_trade_days_between # 导入交易日工具

@task_register()
class TushareStockDailyTask(TushareTask):
    """股票日线数据任务
    
    获取股票的日线交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额等信息。
    该任务使用Tushare的daily接口获取数据。
    """
    
    # 1.核心属性
    name = "tushare_stock_daily"
    description = "获取股票日线交易数据"
    table_name = "tushare_stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date" # 日期列名，用于确认最新数据日期
    
    # 2.自定义索引
    indexes = [
        {"name": "idx_tushare_stock_daily_code", "columns": "ts_code"},
        {"name": "idx_tushare_stock_daily_date", "columns": "trade_date"}
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
    validations = [
        # 验证价格字段是否合理
        lambda df: all(df["close"] >= 0),
        lambda df: all(df["high"] >= df["low"]),
        # 验证交易量是否为正
        lambda df: all(df["volume"] >= 0), # 目标字段名
        # 验证交易额是否为正
        lambda df: all(df["amount"] >= 0)
    ]
    
    # 8. 分批配置
    batch_trade_days_single_code = 240 # 单代码查询时，每个批次的交易日数量 (约1年)
    batch_trade_days_all_codes = 15    # 全市场查询时，每个批次的交易日数量 (3周)

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (基于精确交易日数量)
        
        将查询参数转换为一系列批处理参数，每个批处理参数用于一次API调用。
        使用 get_trade_days_between 获取实际交易日，然后按指定数量分批。
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        # 获取查询参数
        ts_code = kwargs.get('ts_code')
        start_date = kwargs.get('start_date')  # 移除默认值
        end_date = kwargs.get('end_date')      # 移除默认值
        exchange = kwargs.get('exchange', 'SSE') # 允许指定交易所，默认为上交所

        if not start_date or not end_date:
            self.logger.error("必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(f"开始生成批处理列表，范围: {start_date} 到 {end_date}, 代码: {ts_code or '全部'}")

        # 获取范围内的所有交易日
        try:
            trade_days = await get_trade_days_between(start_date, end_date, exchange=exchange)
        except Exception as e:
            self.logger.error(f"获取交易日历失败: {e}")
            return []

        if not trade_days:
            self.logger.warning(f"在 {start_date} 和 {end_date} 之间没有找到交易日")
            return []

        self.logger.info(f"找到 {len(trade_days)} 个交易日")

        # 确定批次大小 N
        if ts_code:
            batch_size_n = self.batch_trade_days_single_code
            self.logger.info(f"使用单代码批次大小: {batch_size_n} 个交易日")
        else:
            batch_size_n = self.batch_trade_days_all_codes
            self.logger.info(f"使用全市场批次大小: {batch_size_n} 个交易日")

        batch_list = []
        for i in range(0, len(trade_days), batch_size_n):
            batch_days = trade_days[i : i + batch_size_n]
            if not batch_days:
                continue

            batch_start = batch_days[0]
            batch_end = batch_days[-1]

            # 构建批次参数
            batch_params = {
                'start_date': batch_start,
                'end_date': batch_end
            }
            if ts_code:
                batch_params['ts_code'] = ts_code

            batch_list.append(batch_params)
            self.logger.debug(f"创建批次 {len(batch_list)}: {batch_start} - {batch_end}")

        # 检查：如果原始参数定义了一个非常小的范围（例如 start=end 且为交易日），但未生成批次
        # 这通常不应该发生，因为 trade_days 非空，上面的循环至少会执行一次
        # 但以防万一，如果批次列表为空但有交易日，生成一个包含所有交易日的批次
        if not batch_list and trade_days:
             self.logger.warning("交易日列表非空但未生成批次，将使用整个范围作为单个批次")
             batch_params = {
                'start_date': trade_days[0],
                'end_date': trade_days[-1]
             }
             if ts_code:
                batch_params['ts_code'] = ts_code
             return [batch_params]

        self.logger.info(f"成功生成 {len(batch_list)} 个批次")
        return batch_list
