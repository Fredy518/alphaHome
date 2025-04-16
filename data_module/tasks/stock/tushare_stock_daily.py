import pandas as pd
from datetime import datetime
from typing import Dict, List
from ...sources.tushare import TushareTask
from ...task_decorator import task_register

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

    # 3.默认配置
    default_concurrent_limit = 5  # 默认并发限制
    default_page_size = 6000  # 默认每页数据量

    # 4.Tushare特有属性
    api_name = "daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", 
              "pre_close", "change", "pct_chg", "vol", "amount"]
    
    # 5.数据类型转换
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
    
    # 6.列名映射
    column_mapping = {
        "vol": "volume"  # 将vol映射为volume
    }

    # 7.表结构定义
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
    
    # 8.数据验证规则 (使用目标字段名 volume)
    validations = [
        # 验证价格字段是否合理
        lambda df: all(df["close"] >= 0),
        lambda df: all(df["high"] >= df["low"]),
        # 验证交易量是否为正
        lambda df: all(df["volume"] >= 0), # 目标字段名
        # 验证交易额是否为正
        lambda df: all(df["amount"] >= 0)
    ]
    
    def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表
        
        将查询参数转换为一系列批处理参数，每个批处理参数用于一次API调用。
        对于股票日线数据，主要按照时间范围和股票代码进行分批。
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        # 获取查询参数
        ts_code = kwargs.get('ts_code')
        start_date = kwargs.get('start_date', '19910101') # 股票市场最早的交易日
        end_date = kwargs.get('end_date', datetime.now().strftime('%Y%m%d'))
        
        # 构建基本参数
        base_params = {}
        if ts_code:
            base_params['ts_code'] = ts_code
        if start_date:
            base_params['start_date'] = start_date
        if end_date:
            base_params['end_date'] = end_date
            
        # 分批策略
        # 1. 如果指定了ts_code，按季度分批（约60-65个交易日）
        # 2. 如果未指定ts_code（全市场数据），按周分批
        
        # 将日期字符串转换为datetime对象
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        if ts_code:
            # 有ts_code时按年度分批
            freq = 'Y'
        else:
            # 无ts_code时按月分批
            freq = 'M'
            
        # 生成日期序列
        date_range = pd.date_range(start=start, end=end, freq=freq)
        
        # 确保第一天和最后一天都被包含
        if date_range.empty or date_range[-1] < end:
            date_range = date_range.append(pd.DatetimeIndex([end]))
        if date_range.empty or date_range[0] > start:
            date_range = pd.DatetimeIndex([start]).append(date_range)
            
        batch_list = []
        for i in range(len(date_range) - 1):
            batch_start = date_range[i].strftime('%Y%m%d')
            batch_end = (date_range[i+1] - pd.Timedelta(days=1)).strftime('%Y%m%d')
            
            # 复制基本参数并添加日期范围
            batch_params = base_params.copy()
            batch_params['start_date'] = batch_start
            batch_params['end_date'] = batch_end
            
            batch_list.append(batch_params)
            
        # 如果没有生成批次（可能是因为日期范围太小），则使用原始参数作为单个批次
        if not batch_list and base_params:
            return [base_params]
            
        return batch_list
    
    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数
        
        将批处理参数转换为Tushare API调用所需的确切参数格式。
        
        Args:
            batch_params: 批处理参数字典
            
        Returns:
            Dict: 准备好的API调用参数
        """
        # 对于daily接口，参数可以直接使用
        return batch_params
