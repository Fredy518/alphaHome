import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches

@task_register()
class TushareFinaIndicatorTask(TushareTask):
    """股票财务指标数据任务
    
    获取上市公司财务指标数据，包括每股指标、盈利能力、营运能力、成长能力、偿债能力等指标。
    该任务使用Tushare的fina_indicator接口获取数据。
    """
    
    # 1.核心属性
    name = "tushare_fina_indicator"
    description = "获取股票财务指标数据"
    table_name = "tushare_fina_indicator"
    primary_keys = ["ts_code", "end_date"]
    date_column = "end_date"
    default_start_date = "19901231"  # 最早的财报日期
    
    # 2.自定义索引
    indexes = [
        {"name": "idx_fina_indicator_code", "columns": "ts_code"},
        {"name": "idx_fina_indicator_end_date", "columns": "end_date"},
        {"name": "idx_fina_indicator_ann_date", "columns": "ann_date"}
    ]
    
    # 3.Tushare特有属性
    api_name = "fina_indicator_vip"
    fields = [
        "ts_code", "ann_date", "end_date", "eps", "dt_eps", "total_revenue_ps", 
        "revenue_ps", "capital_rese_ps", "surplus_rese_ps", "undist_profit_ps", 
        "extra_item", "profit_dedt", "gross_margin", "current_ratio", "quick_ratio", 
        "cash_ratio", "invturn_days", "arturn_days", "inv_turn", "ar_turn", "ca_turn", 
        "fa_turn", "assets_turn", "op_income", "valuechange_income", "interst_income", 
        "daa", "ebit", "ebitda", "fcff", "fcfe", "current_exint", "noncurrent_exint", 
        "interestdebt", "netdebt", "tangible_asset", "working_capital", 
        "networking_capital", "invest_capital", "retained_earnings", "diluted2_eps", 
        "bps", "ocfps", "retainedps", "cfps", "ebit_ps", "fcff_ps", "fcfe_ps", 
        "netprofit_margin", "grossprofit_margin", "cogs_of_sales", "expense_of_sales", 
        "profit_to_gr", "saleexp_to_gr", "adminexp_to_gr", "finaexp_to_gr", 
        "impai_ttm", "gc_of_gr", "op_of_gr", "ebit_of_gr", "roe", "roe_waa", 
        "roe_dt", "roa", "npta", "roic", "roe_yearly", "roa2_yearly", "roe_avg", 
        "opincome_of_ebt", "investincome_of_ebt", "n_op_profit_of_ebt", 
        "tax_to_ebt", "dtprofit_to_profit", "salescash_to_or", "ocf_to_or", 
        "ocf_to_opincome", "capitalized_to_da", "debt_to_assets", "assets_to_eqt", 
        "dp_assets_to_eqt", "ca_to_assets", "nca_to_assets", 
        "tbassets_to_totalassets", "int_to_talcap", "eqt_to_talcapital", 
        "currentdebt_to_debt", "longdeb_to_debt", "ocf_to_shortdebt", "debt_to_eqt", 
        "eqt_to_debt", "eqt_to_interestdebt", "tangibleasset_to_debt", 
        "tangasset_to_intdebt", "tangibleasset_to_netdebt", "ocf_to_debt", 
        "ocf_to_interestdebt", "ocf_to_netdebt", "ebit_to_interest", 
        "longdebt_to_workingcapital", "ebitda_to_debt", "turn_days", "roa_yearly", 
        "roa_dp", "fixed_assets", "profit_prefin_exp", "non_op_profit", 
        "op_to_ebt", "nop_to_ebt", "ocf_to_profit", "cash_to_liqdebt", 
        "cash_to_liqdebt_withinterest", "op_to_liqdebt", "op_to_debt", "roic_yearly", 
        "total_fa_trun", "profit_to_op", "q_opincome", "q_investincome", 
        "q_dtprofit", "q_eps", "q_netprofit_margin", "q_gsprofit_margin", 
        "q_exp_to_sales", "q_profit_to_gr", "q_saleexp_to_gr", "q_adminexp_to_gr", 
        "q_finaexp_to_gr", "q_impair_to_gr_ttm", "q_gc_to_gr", "q_op_to_gr", 
        "q_roe", "q_dt_roe", "q_npta", "q_oprate", "q_op_qoq", "q_profit_yoy", 
        "q_profit_qoq", "q_netprofit_yoy", "q_netprofit_qoq", "q_sales_yoy", 
        "q_sales_qoq", "q_gr_yoy", "q_gr_qoq", "q_roe_yoy", "q_dt_roe_yoy", 
        "q_npta_yoy", "q_oprate_yoy", "q_op_yoy", "debt_to_assets_cl", 
        "ca_to_assets_cl", "nca_to_assets_cl", "debt_to_eqt_cl", "eqt_to_debt_cl", 
        "eqt_to_interestdebt_cl", "tangibleasset_to_debt_cl", 
        "tangasset_to_intdebt_cl", "tangibleasset_to_netdebt_cl", "ocf_to_debt_cl", 
        "ocf_to_interestdebt_cl", "ocf_to_netdebt_cl", "ebit_to_interest_cl", 
        "longdebt_to_workingcapital_cl", "ebitda_to_debt_cl", "bps_yoy", 
        "assets_yoy", "eqt_yoy", "tr_yoy", "or_yoy", "op_yoy", "ebt_yoy", 
        "netprofit_yoy", "dt_netprofit_yoy", "ocf_yoy", "roe_yoy", "update_flag", 
        "rd_exp"
    ]
    
    # 4.数据类型转换 (Most are numeric)
    transformations = {
        # Define transformations for non-float types or specific conversions
        "update_flag": lambda x: str(x) if pd.notna(x) else None,
        # All other fields from 'fields' list are expected to be float or can be coerced
        **{field: float for field in fields if field not in ["ts_code", "ann_date", "end_date", "update_flag"]} 
    }

    # 5.列名映射 (No mapping needed for this API)
    column_mapping = {}

    # 6.表结构定义 (Define schema based on fields and expected types)
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        **{field: {"type": "NUMERIC(20,4)"} for field in fields if field not in ["ts_code", "ann_date", "end_date", "update_flag"]},
        "update_flag": {"type": "VARCHAR(1)"} # Assuming update_flag is a single character
    }

    # 7.数据验证规则 (Optional, add specific checks if needed)
    validations = [] 

    async def process_data(self, data):
        """处理从Tushare获取的数据
        
        重写父类方法，额外处理ann_date为NULL的情况
        """
        # 首先调用父类的process_data方法进行基本处理
        data = await super().process_data(data)
        
        # 处理ann_date为NULL的情况，使用end_date作为默认值
        if 'ann_date' in data.columns and data['ann_date'].isna().any():
            null_ann_dates = data['ann_date'].isna().sum()
            self.logger.warning(f"发现 {null_ann_dates} 条记录的ann_date为NULL，将使用end_date作为默认值")
            
            # 使用end_date填充ann_date的NULL值
            data.loc[data['ann_date'].isna(), 'ann_date'] = data.loc[data['ann_date'].isna(), 'end_date']
            
            # 再次检查是否还有NULL值
            if data['ann_date'].isna().any():
                remaining_nulls = data['ann_date'].isna().sum()
                self.logger.error(f"填充后仍有 {remaining_nulls} 条记录的ann_date为NULL")
            else:
                self.logger.info("所有NULL的ann_date已成功填充")
                
        return data

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用自然日批次工具)
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        ts_code = kwargs.get('ts_code') # Allow filtering by ts_code if provided

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        # Using 365 days batch size similar to income/cashflow tasks
        batch_size_days = 365 
        self.logger.info(f"任务 {self.name}: 使用自然日批次工具生成批处理列表，范围: {start_date} 到 {end_date}，批次大小: {batch_size_days} 天")

        try:
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=batch_size_days,
                ts_code=ts_code, # Pass ts_code to batch generator
                logger=self.logger
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True)
            return [] 