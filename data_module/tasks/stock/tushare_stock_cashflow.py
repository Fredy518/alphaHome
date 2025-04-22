import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.calendar import get_trade_days_between
from ...tools.batch_utils import generate_natural_day_batches

@task_register()
class TushareStockCashflowTask(TushareTask):
    """股票现金流量表数据任务
    
    获取上市公司现金流量表数据，包括经营活动、投资活动和筹资活动的现金流量等信息。
    该任务使用Tushare的cashflow接口获取数据。
    """
    
    # 1.核心属性
    name = "tushare_stock_cashflow"
    description = "获取股票现金流量表数据"
    table_name = "tushare_stock_cashflow"
    primary_keys = ["ts_code", "end_date", "f_ann_date"]
    date_column = "end_date"
    default_start_date = "19901231"  # 最早的财报日期
    
    # 2.自定义索引
    indexes = [
        {"name": "idx_tushare_cashflow_code", "columns": "ts_code"},
        {"name": "idx_tushare_cashflow_end_date", "columns": "end_date"},
        {"name": "idx_tushare_cashflow_ann_date", "columns": "f_ann_date"},
        {"name": "idx_tushare_cashflow_report_type", "columns": "report_type"}
    ]
    
    # 3.Tushare特有属性
    api_name = "cashflow_vip"
    fields = [
        "ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type",
        "net_profit", "finan_exp", "c_fr_sale_sg", "recp_tax_rends",
        "n_depos_incr_fi", "n_incr_loans_cb", "n_inc_borr_oth_fi",
        "prem_fr_orig_contr", "n_incr_insured_dep", "n_reinsur_prem",
        "n_incr_disp_tfa", "ifc_cash_incr", "n_incr_disp_faas", "n_incr_loans_oth_bank",
        "n_cap_incr_repur", "c_fr_oth_operate_a", "c_inf_fr_operate_a",
        "c_paid_goods_s", "c_paid_to_for_empl", "c_paid_for_taxes", "n_incr_clt_loan_adv",
        "n_incr_dep_cbob", "c_pay_claims_orig_inco", "pay_handling_chrg",
        "pay_comm_insur_plcy", "oth_cash_pay_oper_act", "st_cash_out_act",
        "n_cashflow_act", "oth_recp_ral_inv_act", "c_disp_withdrwl_invest",
        "c_recp_return_invest", "n_recp_disp_fiolta", "n_recp_disp_sobu",
        "stot_inflows_inv_act", "c_pay_acq_const_fiolta", "c_paid_invest",
        "n_disp_subs_oth_biz", "oth_pay_ral_inv_act", "n_incr_pledge_loan",
        "stot_out_inv_act", "n_cashflow_inv_act", "c_recp_borrow", "proc_issue_bonds",
        "oth_cash_recp_ral_fnc_act", "stot_cash_in_fnc_act", "free_cashflow",
        "c_prepay_amt_borr", "c_pay_dist_dpcp_int_exp", "incl_dvd_profit_paid_sc_ms",
        "oth_cashpay_ral_fnc_act", "stot_cashout_fnc_act", "n_cash_flows_fnc_act",
        "eff_fx_flu_cash", "n_incr_cash_cash_equ", "c_cash_equ_beg_period",
        "c_cash_equ_end_period", "c_recp_cap_contrib", "incl_cash_rec_saims",
        "uncon_invest_loss", "prov_depr_assets", "depr_fa_coga_dpba",
        "amort_intang_assets", "lt_amort_deferred_exp", "decr_deferred_exp",
        "incr_acc_exp", "loss_disp_fiolta", "loss_scr_fa", "loss_fv_chg",
        "invest_loss", "decr_def_inc_tax_assets", "incr_def_inc_tax_liab",
        "decr_inventories", "decr_oper_payable", "incr_oper_payable",
        "others", "im_net_cashflow_oper_act", "conv_debt_into_cap",
        "conv_copbonds_due_within_1y", "fa_fnc_leases", "end_bal_cash",
        "beg_bal_cash", "end_bal_cash_equ", "beg_bal_cash_equ",
        "im_n_incr_cash_equ"
    ]
    
    # 4.数据类型转换
    transformations = {
        "report_type": lambda x: int(x) if pd.notna(x) else None,
        "comp_type": lambda x: int(x) if pd.notna(x) else None,
        "net_profit": float,
        "finan_exp": float,
        "c_fr_sale_sg": float,
        "recp_tax_rends": float,
        "n_depos_incr_fi": float,
        "n_incr_loans_cb": float,
        "n_inc_borr_oth_fi": float,
        "prem_fr_orig_contr": float,
        "n_incr_insured_dep": float,
        "n_reinsur_prem": float,
        "n_incr_disp_tfa": float,
        "ifc_cash_incr": float,
        "n_incr_disp_faas": float,
        "n_incr_loans_oth_bank": float,
        "n_cap_incr_repur": float,
        "c_fr_oth_operate_a": float,
        "c_inf_fr_operate_a": float,
        "c_paid_goods_s": float,
        "c_paid_to_for_empl": float,
        "c_paid_for_taxes": float,
        "n_incr_clt_loan_adv": float,
        "n_incr_dep_cbob": float,
        "c_pay_claims_orig_inco": float,
        "pay_handling_chrg": float,
        "pay_comm_insur_plcy": float,
        "oth_cash_pay_oper_act": float,
        "st_cash_out_act": float,
        "n_cashflow_act": float,
        "oth_recp_ral_inv_act": float,
        "c_disp_withdrwl_invest": float,
        "c_recp_return_invest": float,
        "n_recp_disp_fiolta": float,
        "n_recp_disp_sobu": float,
        "stot_inflows_inv_act": float,
        "c_pay_acq_const_fiolta": float,
        "c_paid_invest": float,
        "n_disp_subs_oth_biz": float,
        "oth_pay_ral_inv_act": float,
        "n_incr_pledge_loan": float,
        "stot_out_inv_act": float,
        "n_cashflow_inv_act": float,
        "c_recp_borrow": float,
        "proc_issue_bonds": float,
        "oth_cash_recp_ral_fnc_act": float,
        "stot_cash_in_fnc_act": float,
        "free_cashflow": float,
        "c_prepay_amt_borr": float,
        "c_pay_dist_dpcp_int_exp": float,
        "incl_dvd_profit_paid_sc_ms": float,
        "oth_cashpay_ral_fnc_act": float,
        "stot_cashout_fnc_act": float,
        "n_cash_flows_fnc_act": float,
        "eff_fx_flu_cash": float,
        "n_incr_cash_cash_equ": float,
        "c_cash_equ_beg_period": float,
        "c_cash_equ_end_period": float,
        "c_recp_cap_contrib": float,
        "incl_cash_rec_saims": float,
        "uncon_invest_loss": float,
        "prov_depr_assets": float,
        "depr_fa_coga_dpba": float,
        "amort_intang_assets": float,
        "lt_amort_deferred_exp": float,
        "decr_deferred_exp": float,
        "incr_acc_exp": float,
        "loss_disp_fiolta": float,
        "loss_scr_fa": float,
        "loss_fv_chg": float,
        "invest_loss": float,
        "decr_def_inc_tax_assets": float,
        "incr_def_inc_tax_liab": float,
        "decr_inventories": float,
        "decr_oper_payable": float,
        "incr_oper_payable": float,
        "others": float,
        "im_net_cashflow_oper_act": float,
        "conv_debt_into_cap": float,
        "conv_copbonds_due_within_1y": float,
        "fa_fnc_leases": float,
        "end_bal_cash": float,
        "beg_bal_cash": float,
        "end_bal_cash_equ": float,
        "beg_bal_cash_equ": float,
        "im_n_incr_cash_equ": float
    }
    
    # 5.列名映射
    column_mapping = {}
    
    # 6.表结构定义
    schema = {
        "ts_code": {"type": "VARCHAR(10)", "constraints": "NOT NULL"},
        "ann_date": {"type": "DATE"},
        "f_ann_date": {"type": "DATE", "constraints": "NOT NULL"},
        "end_date": {"type": "DATE", "constraints": "NOT NULL"},
        "report_type": {"type": "SMALLINT"},
        "comp_type": {"type": "SMALLINT"},
        "net_profit": {"type": "NUMERIC(20,4)"},
        "finan_exp": {"type": "NUMERIC(20,4)"},
        "c_fr_sale_sg": {"type": "NUMERIC(20,4)"},
        "recp_tax_rends": {"type": "NUMERIC(20,4)"},
        "n_depos_incr_fi": {"type": "NUMERIC(20,4)"},
        "n_incr_loans_cb": {"type": "NUMERIC(20,4)"},
        "n_inc_borr_oth_fi": {"type": "NUMERIC(20,4)"},
        "prem_fr_orig_contr": {"type": "NUMERIC(20,4)"},
        "n_incr_insured_dep": {"type": "NUMERIC(20,4)"},
        "n_reinsur_prem": {"type": "NUMERIC(20,4)"},
        "n_incr_disp_tfa": {"type": "NUMERIC(20,4)"},
        "ifc_cash_incr": {"type": "NUMERIC(20,4)"},
        "n_incr_disp_faas": {"type": "NUMERIC(20,4)"},
        "n_incr_loans_oth_bank": {"type": "NUMERIC(20,4)"},
        "n_cap_incr_repur": {"type": "NUMERIC(20,4)"},
        "c_fr_oth_operate_a": {"type": "NUMERIC(20,4)"},
        "c_inf_fr_operate_a": {"type": "NUMERIC(20,4)"},
        "c_paid_goods_s": {"type": "NUMERIC(20,4)"},
        "c_paid_to_for_empl": {"type": "NUMERIC(20,4)"},
        "c_paid_for_taxes": {"type": "NUMERIC(20,4)"},
        "n_incr_clt_loan_adv": {"type": "NUMERIC(20,4)"},
        "n_incr_dep_cbob": {"type": "NUMERIC(20,4)"},
        "c_pay_claims_orig_inco": {"type": "NUMERIC(20,4)"},
        "pay_handling_chrg": {"type": "NUMERIC(20,4)"},
        "pay_comm_insur_plcy": {"type": "NUMERIC(20,4)"},
        "oth_cash_pay_oper_act": {"type": "NUMERIC(20,4)"},
        "st_cash_out_act": {"type": "NUMERIC(20,4)"},
        "n_cashflow_act": {"type": "NUMERIC(20,4)"},
        "oth_recp_ral_inv_act": {"type": "NUMERIC(20,4)"},
        "c_disp_withdrwl_invest": {"type": "NUMERIC(20,4)"},
        "c_recp_return_invest": {"type": "NUMERIC(20,4)"},
        "n_recp_disp_fiolta": {"type": "NUMERIC(20,4)"},
        "n_recp_disp_sobu": {"type": "NUMERIC(20,4)"},
        "stot_inflows_inv_act": {"type": "NUMERIC(20,4)"},
        "c_pay_acq_const_fiolta": {"type": "NUMERIC(20,4)"},
        "c_paid_invest": {"type": "NUMERIC(20,4)"},
        "n_disp_subs_oth_biz": {"type": "NUMERIC(20,4)"},
        "oth_pay_ral_inv_act": {"type": "NUMERIC(20,4)"},
        "n_incr_pledge_loan": {"type": "NUMERIC(20,4)"},
        "stot_out_inv_act": {"type": "NUMERIC(20,4)"},
        "n_cashflow_inv_act": {"type": "NUMERIC(20,4)"},
        "c_recp_borrow": {"type": "NUMERIC(20,4)"},
        "proc_issue_bonds": {"type": "NUMERIC(20,4)"},
        "oth_cash_recp_ral_fnc_act": {"type": "NUMERIC(20,4)"},
        "stot_cash_in_fnc_act": {"type": "NUMERIC(20,4)"},
        "free_cashflow": {"type": "NUMERIC(20,4)"},
        "c_prepay_amt_borr": {"type": "NUMERIC(20,4)"},
        "c_pay_dist_dpcp_int_exp": {"type": "NUMERIC(20,4)"},
        "incl_dvd_profit_paid_sc_ms": {"type": "NUMERIC(20,4)"},
        "oth_cashpay_ral_fnc_act": {"type": "NUMERIC(20,4)"},
        "stot_cashout_fnc_act": {"type": "NUMERIC(20,4)"},
        "n_cash_flows_fnc_act": {"type": "NUMERIC(20,4)"},
        "eff_fx_flu_cash": {"type": "NUMERIC(20,4)"},
        "n_incr_cash_cash_equ": {"type": "NUMERIC(20,4)"},
        "c_cash_equ_beg_period": {"type": "NUMERIC(20,4)"},
        "c_cash_equ_end_period": {"type": "NUMERIC(20,4)"},
        "c_recp_cap_contrib": {"type": "NUMERIC(20,4)"},
        "incl_cash_rec_saims": {"type": "NUMERIC(20,4)"},
        "uncon_invest_loss": {"type": "NUMERIC(20,4)"},
        "prov_depr_assets": {"type": "NUMERIC(20,4)"},
        "depr_fa_coga_dpba": {"type": "NUMERIC(20,4)"},
        "amort_intang_assets": {"type": "NUMERIC(20,4)"},
        "lt_amort_deferred_exp": {"type": "NUMERIC(20,4)"},
        "decr_deferred_exp": {"type": "NUMERIC(20,4)"},
        "incr_acc_exp": {"type": "NUMERIC(20,4)"},
        "loss_disp_fiolta": {"type": "NUMERIC(20,4)"},
        "loss_scr_fa": {"type": "NUMERIC(20,4)"},
        "loss_fv_chg": {"type": "NUMERIC(20,4)"},
        "invest_loss": {"type": "NUMERIC(20,4)"},
        "decr_def_inc_tax_assets": {"type": "NUMERIC(20,4)"},
        "incr_def_inc_tax_liab": {"type": "NUMERIC(20,4)"},
        "decr_inventories": {"type": "NUMERIC(20,4)"},
        "decr_oper_payable": {"type": "NUMERIC(20,4)"},
        "incr_oper_payable": {"type": "NUMERIC(20,4)"},
        "others": {"type": "NUMERIC(20,4)"},
        "im_net_cashflow_oper_act": {"type": "NUMERIC(20,4)"},
        "conv_debt_into_cap": {"type": "NUMERIC(20,4)"},
        "conv_copbonds_due_within_1y": {"type": "NUMERIC(20,4)"},
        "fa_fnc_leases": {"type": "NUMERIC(20,4)"},
        "end_bal_cash": {"type": "NUMERIC(20,4)"},
        "beg_bal_cash": {"type": "NUMERIC(20,4)"},
        "end_bal_cash_equ": {"type": "NUMERIC(20,4)"},
        "beg_bal_cash_equ": {"type": "NUMERIC(20,4)"},
        "im_n_incr_cash_equ": {"type": "NUMERIC(20,4)"}
    }
    
    # 7.数据验证规则
    # validations = [
    #     lambda df: (df['im_net_cashflow_oper_act'] == df['net_profit'] + df['depr_fa_cga_dpba'] + df['amort_intang_assets'] + df['amort_lt_deferred_exp'] + df['loss_disp_fa_cga_intang_assets']).all()
    # ]
    
    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用自然日批次工具)
        
        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等
            
        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        ts_code = kwargs.get('ts_code')

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(f"任务 {self.name}: 使用自然日批次工具生成批处理列表，范围: {start_date} 到 {end_date}")

        try:
            # 使用自然日批次生成工具
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=365,  # 使用365天作为批次大小
                ts_code=ts_code,
                logger=self.logger
            )
            return batch_list
        except Exception as e:
            self.logger.error(f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True)
            return [] 