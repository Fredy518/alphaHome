import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches

@task_register()
class TushareStockReportRcTask(TushareTask):
    """获取券商盈利预测数据任务
    
    数据来源: Tushare report_rc 接口 (https://tushare.pro/document/2?doc_id=292)
    """
    
    # 1.核心属性
    name = "tushare_stock_report_rc"
    description = "获取卖方盈利预测数据"
    table_name = "tushare_stock_report_rc"
    primary_keys = ["ts_code", "report_date", "org_name", "author_name", "quarter"]
    date_column = "report_date" # 使用报告日期作为主要日期
    default_start_date = "19900101"

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 10
    default_page_size = 3000

    # 2.自定义索引
    indexes = [
        {"name": "idx_reportrc_code", "columns": "ts_code"},
        {"name": "idx_reportrc_report_date", "columns": "report_date"},
        {"name": "idx_reportrc_org", "columns": "org_name"},
        {"name": "idx_reportrc_quarter", "columns": "quarter"},
        {"name": "idx_reportrc_update_time", "columns": "update_time"}
    ]

    # 3.Tushare特有属性
    api_name = "report_rc" # Tushare API 名称
    fields = [
        "ts_code", "name", "report_date", "report_title", "report_type", 
        "classify", "org_name", "author_name", "quarter", "op_rt", "op_pr", 
        "tp", "np", "eps", "pe", "rd", "roe", "ev_ebitda", "rating", 
        "max_price", "min_price", "imp_dg", "create_time"
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
        "create_time": lambda x: pd.to_datetime(x) if pd.notna(x) else None
    }

    # 5.列名映射 (No mapping needed for this API)
    column_mapping = {}

    # 6.表结构定义
    schema = {
        # Primary Keys
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "report_date": {"type": "DATE", "constraints": "NOT NULL"},
        "org_name": {"type": "VARCHAR(100)", "constraints": "NOT NULL"},
        "author_name": {"type": "VARCHAR(255)", "constraints": "NOT NULL"},
        "quarter": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        
        # Other Fields
        "name": {"type": "VARCHAR(50)"},
        "report_title": {"type": "TEXT"}, # Titles can be long
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
        "create_time": {"type": "TIMESTAMP"}
    }

    # 7.数据验证规则 (Optional)
    validations = [] 


    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用自然日批次工具, 基于 report_date)
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date (对应 report_date), ts_code, full_update等
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        full_update = kwargs.get('full_update', False)
        ts_code = kwargs.get('ts_code') # Allow filtering by ts_code if provided

        if full_update:
            start_date = self.default_start_date
            end_date = datetime.now().strftime('%Y%m%d')
            self.logger.info(f"任务 {self.name}: 全量更新模式，自动设置日期范围: {start_date} 到 {end_date}")
        else:
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')
            if not start_date or not end_date:
                self.logger.error(f"任务 {self.name}: 非全量更新模式下，必须提供 start_date 和 end_date 参数 (对应 report_date)")
                return []
            self.logger.info(f"任务 {self.name}: 使用自然日批次工具生成批处理列表 (基于 report_date)，范围: {start_date} 到 {end_date}")

        batch_size_days = 30 # User requested 30-day batches

        try:
            # Note: The batch generator uses start_date/end_date keys. 
            # The TushareTask base class should correctly pass these as start_date/end_date
            # parameters to the Tushare API, which filters on 'report_date'.
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size_days,
                ts_code=ts_code, # Pass ts_code to batch generator if provided
                logger=self.logger
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True)
            return [] 

    async def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        异步处理从API获取的原始数据。
        此方法可以被子类覆盖以实现特定的数据转换逻辑。
        """
        # 假设父类的 process_data 是同步的
        df = super().process_data(df)

        # 如果df为空或者不是DataFrame，则直接返回
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df
        
        # 2. 填充特定列的空值
        if 'org_name' in df.columns:
            # fillna处理None/NaN, replace处理空字符串
            df['org_name'] = df['org_name'].fillna('无').replace('', '无')
            
        if 'author_name' in df.columns:
            df['author_name'] = df['author_name'].fillna('无').replace('', '无')
            
        return df 