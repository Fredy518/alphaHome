import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from ...sources.tushare import TushareTask

class StockDailyBasicTask(TushareTask):
    """股票每日基本面指标任务
    
    获取股票的每日基本面指标，包括市盈率、市净率、换手率、总市值等数据。
    该任务使用Tushare的daily_basic接口获取数据，并依赖于股票日线数据任务。
    """
    
    # 核心属性
    name = "stock_daily_basic"
    description = "获取股票每日基本面指标"
    table_name = "stock_daily_basic"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # 依赖关系
    dependencies = ["stock_daily"]
    
    # Tushare特有属性
    api_name = "daily_basic"
    fields = [
        "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", 
        "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", 
        "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"
    ]
    
    # 表结构定义
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "close": {"type": "NUMERIC(10,4)"},
        "turnover_rate": {"type": "NUMERIC(10,4)"},
        "turnover_rate_f": {"type": "NUMERIC(10,4)"},
        "volume_ratio": {"type": "NUMERIC(10,4)"},
        "pe": {"type": "NUMERIC(10,4)"},
        "pe_ttm": {"type": "NUMERIC(10,4)"},
        "pb": {"type": "NUMERIC(10,4)"},
        "ps": {"type": "NUMERIC(10,4)"},
        "ps_ttm": {"type": "NUMERIC(10,4)"},
        "dv_ratio": {"type": "NUMERIC(10,4)"},
        "dv_ttm": {"type": "NUMERIC(10,4)"},
        "total_share": {"type": "NUMERIC(20,4)"},
        "float_share": {"type": "NUMERIC(20,4)"},
        "free_share": {"type": "NUMERIC(20,4)"},
        "total_mv": {"type": "NUMERIC(20,4)"},
        "circ_mv": {"type": "NUMERIC(20,4)"},
        "free_mv": {"type": "NUMERIC(20,4)"},
        "float_ratio": {"type": "NUMERIC(10,4)"},
        "bp_ratio": {"type": "NUMERIC(10,4)"},
        "annual_div_yield": {"type": "NUMERIC(10,4)"}
    }
    
    # 数据处理规则
    transformations = {
        "close": float,
        "turnover_rate": float,
        "turnover_rate_f": float,
        "volume_ratio": float,
        "pe": float,
        "pe_ttm": float,
        "pb": float,
        "ps": float,
        "ps_ttm": float,
        "dv_ratio": float,
        "dv_ttm": float,
        "total_share": float,
        "float_share": float,
        "free_share": float,
        "total_mv": float,
        "circ_mv": float
    }
    
    # 数据验证规则
    validations = [
        # 验证市值是否为正
        lambda df: all(df["total_mv"].fillna(0) >= 0),
        # 验证流通市值是否为正
        lambda df: all(df["circ_mv"].fillna(0) >= 0),
        # 验证换手率是否合理
        lambda df: all((df["turnover_rate"].fillna(0) >= 0) & (df["turnover_rate"].fillna(0) <= 100)),
        # 验证股本数据是否合理
        lambda df: all(df["total_share"].fillna(0) >= df["float_share"].fillna(0)),
        # 验证日期格式
        lambda df: all(pd.to_datetime(df["trade_date"], errors="coerce").notna())
    ]
    
    # 自定义索引
    indexes = [
        {"name": "idx_daily_basic_code", "columns": "ts_code"},
        {"name": "idx_daily_basic_date", "columns": "trade_date"}
    ]

    def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表

        对于每日基本面指标，主要按时间范围和股票代码进行分批。
        与日线数据类似，分批策略有助于管理API调用频率和单次数据量。

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

        # 分批策略：与StockDailyTask保持一致
        # 1. 如果指定了ts_code，按年度分批
        # 2. 如果未指定ts_code（全市场数据），按月分批

        try:
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
        except ValueError as e:
            self.logger.error(f"无效的日期格式: start={start_date}, end={end_date}. Error: {e}")
            return [] # 返回空列表表示无法生成批次

        if ts_code:
            # 有ts_code时按年度分批
            freq = 'Y'
        else:
            # 无ts_code时按周分批
            freq = 'W'

        # 生成日期序列
        date_range = pd.date_range(start=start, end=end, freq=freq)

        # 确保第一天和最后一天都被包含
        if date_range.empty or date_range[-1] < end:
            date_range = date_range.append(pd.DatetimeIndex([end]))
        if date_range.empty or date_range[0] > start:
            date_range = pd.DatetimeIndex([start]).append(date_range)

        batch_list = []
        for i in range(len(date_range) - 1):
            # Tushare接口通常包含开始和结束日期
            batch_start_dt = date_range[i]
            batch_end_dt = date_range[i+1]

            # 下一个周期的开始作为当前周期的结束，避免重叠
            # 但 daily_basic 似乎可以直接用范围查询
            batch_start = batch_start_dt.strftime('%Y%m%d')
            # 如果下一个日期不是序列的最后一个，则取下一个日期的前一天
            if i + 1 < len(date_range) -1 :
                 batch_end = (batch_end_dt - pd.Timedelta(days=1)).strftime('%Y%m%d')
            else:
                 # 如果是最后一个区间，结束日期就是end_date
                 batch_end = end.strftime('%Y%m%d')

            # 复制基本参数并添加日期范围
            batch_params = base_params.copy()
            batch_params['start_date'] = batch_start
            batch_params['end_date'] = batch_end

            batch_list.append(batch_params)

        # 如果没有生成批次（可能是因为日期范围太小），则使用原始参数作为单个批次
        if not batch_list and base_params.get('start_date') and base_params.get('end_date'):
            # 确保有起止日期才添加
            return [base_params]
            
        # 如果 base_params 为空（例如只提供了kwargs但无有效参数），返回空列表
        if not base_params and not batch_list:
             return []

        return batch_list

    def prepare_params(self, batch_params: Dict) -> Dict:
        """准备API调用参数

        将批处理参数转换为Tushare API调用所需的确切参数格式。
        对于daily_basic接口，参数可以直接使用。

        Args:
            batch_params: 批处理参数字典

        Returns:
            Dict: 准备好的API调用参数
        """
        # daily_basic接口参数无需特殊处理，直接返回
        return batch_params
