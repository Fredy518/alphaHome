from ...sources.tushare import TushareTask

class IndexDailyTask(TushareTask):
    """指数日线数据任务
    
    获取指数的日线交易数据，包括开盘价、收盘价、最高价、最低价、成交量、成交额等信息。
    该任务使用Tushare的index_daily接口获取数据。
    """
    
    # 核心属性
    name = "index_daily"
    description = "获取指数日线交易数据"
    table_name = "index_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"
    
    # Tushare API名称
    api_name = "index_daily"
    fields = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]
    
    # 表结构定义
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
        "vol": {"type": "NUMERIC(20,4)"},
        "amount": {"type": "NUMERIC(20,4)"}
    }
    
    # 数据处理规则
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "pre_close": float,
        "change": float,
        "pct_chg": float,
        "vol": float,
        "amount": float
    }
    
    # 数据验证规则
    validations = [
        # 验证收盘价是否合理
        lambda df: all(df["close"] >= 0),
        lambda df: all(df["high"] >= df["low"]),
        # 验证成交量是否为正
        lambda df: all(df["vol"] >= 0),
        # 验证成交额是否为正
        lambda df: all(df["amount"] >= 0)
    ]
    
    # 索引定义
    indexes = [
        {"name": "idx_index_daily_code", "columns": "ts_code"},
        {"name": "idx_index_daily_date", "columns": "trade_date"}
    ]
    
    def get_batch_list(self, start_date: str = None, end_date: str = None, **kwargs) -> list:
        """获取批处理列表
        
        根据日期范围和其他参数生成批处理参数列表，用于分批获取数据。
        对于index_daily接口，我们按照日期和指数代码进行批处理。
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            **kwargs: 其他参数，如ts_code（指数代码列表）
            
        Returns:
            list: 批处理参数列表
        """
        batch_list = []
        
        # 处理日期范围
        if start_date and end_date:
            # 将日期字符串转换为datetime对象
            import pandas as pd
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            
            # 生成日期序列
            date_range = pd.date_range(start=start, end=end, freq='D')
            
            # 处理指数代码
            ts_codes = kwargs.get('ts_code', '')
            if ts_codes:
                # 如果提供了多个指数代码，按照代码分批
                codes = ts_codes.split(',')
                for date in date_range:
                    date_str = date.strftime('%Y%m%d')
                    for code in codes:
                        batch_list.append({
                            'ts_code': code.strip(),
                            'trade_date': date_str
                        })
            else:
                # 如果没有提供指数代码，只按日期分批
                for date in date_range:
                    batch_list.append({
                        'trade_date': date.strftime('%Y%m%d')
                    })
        else:
            # 如果没有提供日期范围，使用其他参数
            batch_list.append(kwargs)
        
        return batch_list
    
    def prepare_params(self, batch_params: dict) -> dict:
        """准备API调用参数
        
        将批处理参数转换为Tushare API调用所需的确切参数格式。
        
        Args:
            batch_params: 批处理参数字典
            
        Returns:
            dict: 准备好的API调用参数
        """
        # 对于index_daily接口，参数可以直接使用
        return batch_params
    
    def process_data(self, data):
        """处理指数日线数据"""
        # 首先调用父类的数据处理
        data = super().process_data(data)
        
        # 指数数据处理
        if not data.empty:
            # 按日期排序
            if "trade_date" in data.columns:
                data = data.sort_values("trade_date")
            
            # 计算其他指标
            # 如果有足够的数据，可以计算移动平均线
            if "close" in data.columns and len(data) > 5:
                # 计算5日均线
                data["ma5"] = data["close"].rolling(window=5).mean()
                # 计算10日均线
                data["ma10"] = data["close"].rolling(window=10).mean()
                # 计算20日均线
                data["ma20"] = data["close"].rolling(window=20).mean()
        
        return data
    
    async def pre_execute(self):
        """任务执行前的准备工作"""
        await super().pre_execute()
        self.logger.info("准备执行指数日线数据任务...")
        
    async def post_execute(self, result):
        """任务执行后的清理工作"""
        await super().post_execute(result)
        self.logger.info(f"指数日线数据任务执行完成，结果: {result}")
