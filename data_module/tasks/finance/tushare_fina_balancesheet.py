import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.calendar import get_trade_days_between
from ...tools.batch_utils import generate_natural_day_batches

@task_register()
class TushareFinaBalancesheetTask(TushareTask):
    """股票资产负债表数据任务
    
    获取上市公司资产负债表数据，包括总资产、总负债、流动资产、流动负债等多个资产负债表项目。
    该任务使用Tushare的balancesheet接口获取数据。
    """
    
    # 1.核心属性
    name = "tushare_fina_balancesheet"
    description = "获取股票资产负债表数据"
    table_name = "tushare_fina_balancesheet"
    primary_keys = ["ts_code", "end_date", "f_ann_date"]
    date_column = "end_date"
    default_start_date = "19901231"  # 最早的财报日期
    
    # 2.自定义索引
    indexes = [
        {"name": "idx_fina_balancesheet_code", "columns": "ts_code"},
        {"name": "idx_fina_balancesheet_end_date", "columns": "end_date"},
        {"name": "idx_fina_balancesheet_ann_date", "columns": "f_ann_date"},
        {"name": "idx_fina_balancesheet_report_type", "columns": "report_type"}
    ]
    
    # 3.Tushare特有属性
    api_name = "balancesheet_vip"
    fields = [
        "ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type",
        "total_share", "cap_rese", "undistr_porfit", "surplus_rese", "special_rese",
        "money_cap", "trad_asset", "notes_receiv", "accounts_receiv", "oth_receiv",
        "prepayment", "div_receiv", "int_receiv", "inventories", "amor_exp",
        "nca_within_1y", "sett_rsrv", "loanto_oth_bank_fi", "premium_receiv",
        "reinsur_receiv", "reinsur_res_receiv", "pur_resale_fa", "oth_cur_assets",
        "total_cur_assets", "fa_avail_for_sale", "htm_invest", "lt_eqt_invest",
        "invest_real_estate", "time_deposits", "oth_assets", "lt_rec", "fix_assets",
        "cip", "const_materials", "fixed_assets_disp", "produc_bio_assets",
        "oil_and_gas_assets", "intan_assets", "r_and_d", "goodwill",
        "lt_amor_exp", "defer_tax_assets", "decr_in_disbur",
        "oth_nca", "total_nca", "cash_reser_cb", "depos_in_oth_bfi",
        "prec_metals", "deriv_assets", "rr_reins_une_prem", "rr_reins_outstd_cla",
        "rr_reins_lins_liab", "rr_reins_lthins_liab", "refund_depos",
        "ph_pledge_loans", "refund_cap_depos", "indep_acct_assets",
        "client_depos", "client_prov", "transac_seat_fee", "invest_as_receiv",
        "total_assets", "lt_borr", "st_borr", "cb_borr", "depos_ib_deposits",
        "loan_oth_bank", "trading_fl", "notes_payable", "acct_payable",
        "adv_receipts", "sold_for_repur_fa", "comm_payable", "payroll_payable",
        "taxes_payable", "int_payable", "div_payable", "oth_payable",
        "acc_exp", "deferred_inc", "st_bonds_payable", "payable_to_reinsurer",
        "rsrv_insur_cont", "acting_trading_sec", "acting_uw_sec",
        "non_cur_liab_due_1y", "oth_cur_liab", "total_cur_liab",
        "bond_payable", "lt_payable", "specific_payables", "estimated_liab",
        "defer_tax_liab", "defer_inc_non_cur_liab", "oth_ncl", "total_ncl",
        "depos_oth_bfi", "deriv_liab", "depos", "agency_bus_liab",
        "oth_liab", "prem_receiv_adva", "depos_received", "ph_invest",
        "reser_une_prem", "reser_outstd_claims", "reser_lins_liab",
        "reser_lthins_liab", "indept_acc_liab", "pledge_borr", "indem_payable",
        "policy_div_payable", "total_liab", "treasury_share", "ordin_risk_reser",
        "forex_differ", "invest_loss_unconf", "minority_int", "total_hldr_eqy_exc_min_int",
        "total_hldr_eqy_inc_min_int", "total_liab_hldr_eqy", "lt_payroll_payable",
        "oth_comp_income", "oth_eqt_tools", "oth_eqt_tools_p_shr", "lending_funds",
        "acc_receivable", "st_fin_payable", "payables", "hfs_assets", "hfs_sales",
        "cost_fin_assets", "fair_value_fin_assets", "contract_assets",
        "contract_liab", "accounts_receiv_bill", "accounts_pay", "oth_rcv_total",
        "fix_assets_total"
    ]
    
    # 4.数据类型转换
    transformations = {
        "report_type": lambda x: int(x) if pd.notna(x) else None,
        "comp_type": lambda x: int(x) if pd.notna(x) else None,
        "total_share": float,
        "cap_rese": float,
        "undistr_porfit": float,
        "surplus_rese": float,
        "special_rese": float,
        "money_cap": float,
        "trad_asset": float,
        "notes_receiv": float,
        "accounts_receiv": float,
        "oth_receiv": float,
        "prepayment": float,
        "div_receiv": float,
        "int_receiv": float,
        "inventories": float,
        "amor_exp": float,
        "nca_within_1y": float,
        "sett_rsrv": float,
        "loanto_oth_bank_fi": float,
        "premium_receiv": float,
        "reinsur_receiv": float,
        "reinsur_res_receiv": float,
        "pur_resale_fa": float,
        "oth_cur_assets": float,
        "total_cur_assets": float,
        "fa_avail_for_sale": float,
        "htm_invest": float,
        "lt_eqt_invest": float,
        "invest_real_estate": float,
        "time_deposits": float,
        "oth_assets": float,
        "lt_rec": float,
        "fix_assets": float,
        "cip": float,
        "const_materials": float,
        "fixed_assets_disp": float,
        "produc_bio_assets": float,
        "oil_and_gas_assets": float,
        "intan_assets": float,
        "r_and_d": float,
        "goodwill": float,
        "lt_amor_exp": float,
        "defer_tax_assets": float,
        "decr_in_disbur": float,
        "oth_nca": float,
        "total_nca": float,
        "cash_reser_cb": float,
        "depos_in_oth_bfi": float,
        "prec_metals": float,
        "deriv_assets": float,
        "rr_reins_une_prem": float,
        "rr_reins_outstd_cla": float,
        "rr_reins_lins_liab": float,
        "rr_reins_lthins_liab": float,
        "refund_depos": float,
        "ph_pledge_loans": float,
        "refund_cap_depos": float,
        "indep_acct_assets": float,
        "client_depos": float,
        "client_prov": float,
        "transac_seat_fee": float,
        "invest_as_receiv": float,
        "total_assets": float,
        "lt_borr": float,
        "st_borr": float,
        "cb_borr": float,
        "depos_ib_deposits": float,
        "loan_oth_bank": float,
        "trading_fl": float,
        "notes_payable": float,
        "acct_payable": float,
        "adv_receipts": float,
        "sold_for_repur_fa": float,
        "comm_payable": float,
        "payroll_payable": float,
        "taxes_payable": float,
        "int_payable": float,
        "div_payable": float,
        "oth_payable": float,
        "acc_exp": float,
        "deferred_inc": float,
        "st_bonds_payable": float,
        "payable_to_reinsurer": float,
        "rsrv_insur_cont": float,
        "acting_trading_sec": float,
        "acting_uw_sec": float,
        "non_cur_liab_due_1y": float,
        "oth_cur_liab": float,
        "total_cur_liab": float,
        "bond_payable": float,
        "lt_payable": float,
        "specific_payables": float,
        "estimated_liab": float,
        "defer_tax_liab": float,
        "defer_inc_non_cur_liab": float,
        "oth_ncl": float,
        "total_ncl": float,
        "depos_oth_bfi": float,
        "deriv_liab": float,
        "depos": float,
        "agency_bus_liab": float,
        "oth_liab": float,
        "prem_receiv_adva": float,
        "depos_received": float,
        "ph_invest": float,
        "reser_une_prem": float,
        "reser_outstd_claims": float,
        "reser_lins_liab": float,
        "reser_lthins_liab": float,
        "indept_acc_liab": float,
        "pledge_borr": float,
        "indem_payable": float,
        "policy_div_payable": float,
        "total_liab": float,
        "treasury_share": float,
        "ordin_risk_reser": float,
        "forex_differ": float,
        "invest_loss_unconf": float,
        "minority_int": float,
        "total_hldr_eqy_exc_min_int": float,
        "total_hldr_eqy_inc_min_int": float,
        "total_liab_hldr_eqy": float,
        "lt_payroll_payable": float,
        "oth_comp_income": float,
        "oth_eqt_tools": float,
        "oth_eqt_tools_p_shr": float,
        "lending_funds": float,
        "acc_receivable": float,
        "st_fin_payable": float,
        "payables": float,
        "hfs_assets": float,
        "hfs_sales": float,
        "cost_fin_assets": float,
        "fair_value_fin_assets": float,
        "contract_assets": float,
        "contract_liab": float,
        "accounts_receiv_bill": float,
        "accounts_pay": float,
        "oth_rcv_total": float,
        "fix_assets_total": float
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
        "total_share": {"type": "NUMERIC(20,4)"},
        "cap_rese": {"type": "NUMERIC(20,4)"},
        "undistr_porfit": {"type": "NUMERIC(20,4)"},
        "surplus_rese": {"type": "NUMERIC(20,4)"},
        "special_rese": {"type": "NUMERIC(20,4)"},
        "money_cap": {"type": "NUMERIC(20,4)"},
        "trad_asset": {"type": "NUMERIC(20,4)"},
        "notes_receiv": {"type": "NUMERIC(20,4)"},
        "accounts_receiv": {"type": "NUMERIC(20,4)"},
        "oth_receiv": {"type": "NUMERIC(20,4)"},
        "prepayment": {"type": "NUMERIC(20,4)"},
        "div_receiv": {"type": "NUMERIC(20,4)"},
        "int_receiv": {"type": "NUMERIC(20,4)"},
        "inventories": {"type": "NUMERIC(20,4)"},
        "amor_exp": {"type": "NUMERIC(20,4)"},
        "nca_within_1y": {"type": "NUMERIC(20,4)"},
        "sett_rsrv": {"type": "NUMERIC(20,4)"},
        "loanto_oth_bank_fi": {"type": "NUMERIC(20,4)"},
        "premium_receiv": {"type": "NUMERIC(20,4)"},
        "reinsur_receiv": {"type": "NUMERIC(20,4)"},
        "reinsur_res_receiv": {"type": "NUMERIC(20,4)"},
        "pur_resale_fa": {"type": "NUMERIC(20,4)"},
        "oth_cur_assets": {"type": "NUMERIC(20,4)"},
        "total_cur_assets": {"type": "NUMERIC(20,4)"},
        "fa_avail_for_sale": {"type": "NUMERIC(20,4)"},
        "htm_invest": {"type": "NUMERIC(20,4)"},
        "lt_eqt_invest": {"type": "NUMERIC(20,4)"},
        "invest_real_estate": {"type": "NUMERIC(20,4)"},
        "time_deposits": {"type": "NUMERIC(20,4)"},
        "oth_assets": {"type": "NUMERIC(20,4)"},
        "lt_rec": {"type": "NUMERIC(20,4)"},
        "fix_assets": {"type": "NUMERIC(20,4)"},
        "cip": {"type": "NUMERIC(20,4)"},
        "const_materials": {"type": "NUMERIC(20,4)"},
        "fixed_assets_disp": {"type": "NUMERIC(20,4)"},
        "produc_bio_assets": {"type": "NUMERIC(20,4)"},
        "oil_and_gas_assets": {"type": "NUMERIC(20,4)"},
        "intan_assets": {"type": "NUMERIC(20,4)"},
        "r_and_d": {"type": "NUMERIC(20,4)"},
        "goodwill": {"type": "NUMERIC(20,4)"},
        "lt_amor_exp": {"type": "NUMERIC(20,4)"},
        "defer_tax_assets": {"type": "NUMERIC(20,4)"},
        "decr_in_disbur": {"type": "NUMERIC(20,4)"},
        "oth_nca": {"type": "NUMERIC(20,4)"},
        "total_nca": {"type": "NUMERIC(20,4)"},
        "cash_reser_cb": {"type": "NUMERIC(20,4)"},
        "depos_in_oth_bfi": {"type": "NUMERIC(20,4)"},
        "prec_metals": {"type": "NUMERIC(20,4)"},
        "deriv_assets": {"type": "NUMERIC(20,4)"},
        "rr_reins_une_prem": {"type": "NUMERIC(20,4)"},
        "rr_reins_outstd_cla": {"type": "NUMERIC(20,4)"},
        "rr_reins_lins_liab": {"type": "NUMERIC(20,4)"},
        "rr_reins_lthins_liab": {"type": "NUMERIC(20,4)"},
        "refund_depos": {"type": "NUMERIC(20,4)"},
        "ph_pledge_loans": {"type": "NUMERIC(20,4)"},
        "refund_cap_depos": {"type": "NUMERIC(20,4)"},
        "indep_acct_assets": {"type": "NUMERIC(20,4)"},
        "client_depos": {"type": "NUMERIC(20,4)"},
        "client_prov": {"type": "NUMERIC(20,4)"},
        "transac_seat_fee": {"type": "NUMERIC(20,4)"},
        "invest_as_receiv": {"type": "NUMERIC(20,4)"},
        "total_assets": {"type": "NUMERIC(20,4)"},
        "lt_borr": {"type": "NUMERIC(20,4)"},
        "st_borr": {"type": "NUMERIC(20,4)"},
        "cb_borr": {"type": "NUMERIC(20,4)"},
        "depos_ib_deposits": {"type": "NUMERIC(20,4)"},
        "loan_oth_bank": {"type": "NUMERIC(20,4)"},
        "trading_fl": {"type": "NUMERIC(20,4)"},
        "notes_payable": {"type": "NUMERIC(20,4)"},
        "acct_payable": {"type": "NUMERIC(20,4)"},
        "adv_receipts": {"type": "NUMERIC(20,4)"},
        "sold_for_repur_fa": {"type": "NUMERIC(20,4)"},
        "comm_payable": {"type": "NUMERIC(20,4)"},
        "payroll_payable": {"type": "NUMERIC(20,4)"},
        "taxes_payable": {"type": "NUMERIC(20,4)"},
        "int_payable": {"type": "NUMERIC(20,4)"},
        "div_payable": {"type": "NUMERIC(20,4)"},
        "oth_payable": {"type": "NUMERIC(20,4)"},
        "acc_exp": {"type": "NUMERIC(20,4)"},
        "deferred_inc": {"type": "NUMERIC(20,4)"},
        "st_bonds_payable": {"type": "NUMERIC(20,4)"},
        "payable_to_reinsurer": {"type": "NUMERIC(20,4)"},
        "rsrv_insur_cont": {"type": "NUMERIC(20,4)"},
        "acting_trading_sec": {"type": "NUMERIC(20,4)"},
        "acting_uw_sec": {"type": "NUMERIC(20,4)"},
        "non_cur_liab_due_1y": {"type": "NUMERIC(20,4)"},
        "oth_cur_liab": {"type": "NUMERIC(20,4)"},
        "total_cur_liab": {"type": "NUMERIC(20,4)"},
        "bond_payable": {"type": "NUMERIC(20,4)"},
        "lt_payable": {"type": "NUMERIC(20,4)"},
        "specific_payables": {"type": "NUMERIC(20,4)"},
        "estimated_liab": {"type": "NUMERIC(20,4)"},
        "defer_tax_liab": {"type": "NUMERIC(20,4)"},
        "defer_inc_non_cur_liab": {"type": "NUMERIC(20,4)"},
        "oth_ncl": {"type": "NUMERIC(20,4)"},
        "total_ncl": {"type": "NUMERIC(20,4)"},
        "depos_oth_bfi": {"type": "NUMERIC(20,4)"},
        "deriv_liab": {"type": "NUMERIC(20,4)"},
        "depos": {"type": "NUMERIC(20,4)"},
        "agency_bus_liab": {"type": "NUMERIC(20,4)"},
        "oth_liab": {"type": "NUMERIC(20,4)"},
        "prem_receiv_adva": {"type": "NUMERIC(20,4)"},
        "depos_received": {"type": "NUMERIC(20,4)"},
        "ph_invest": {"type": "NUMERIC(20,4)"},
        "reser_une_prem": {"type": "NUMERIC(20,4)"},
        "reser_outstd_claims": {"type": "NUMERIC(20,4)"},
        "reser_lins_liab": {"type": "NUMERIC(20,4)"},
        "reser_lthins_liab": {"type": "NUMERIC(20,4)"},
        "indept_acc_liab": {"type": "NUMERIC(20,4)"},
        "pledge_borr": {"type": "NUMERIC(20,4)"},
        "indem_payable": {"type": "NUMERIC(20,4)"},
        "policy_div_payable": {"type": "NUMERIC(20,4)"},
        "total_liab": {"type": "NUMERIC(20,4)"},
        "treasury_share": {"type": "NUMERIC(20,4)"},
        "ordin_risk_reser": {"type": "NUMERIC(20,4)"},
        "forex_differ": {"type": "NUMERIC(20,4)"},
        "invest_loss_unconf": {"type": "NUMERIC(20,4)"},
        "minority_int": {"type": "NUMERIC(20,4)"},
        "total_hldr_eqy_exc_min_int": {"type": "NUMERIC(20,4)"},
        "total_hldr_eqy_inc_min_int": {"type": "NUMERIC(20,4)"},
        "total_liab_hldr_eqy": {"type": "NUMERIC(20,4)"},
        "lt_payroll_payable": {"type": "NUMERIC(20,4)"},
        "oth_comp_income": {"type": "NUMERIC(20,4)"},
        "oth_eqt_tools": {"type": "NUMERIC(20,4)"},
        "oth_eqt_tools_p_shr": {"type": "NUMERIC(20,4)"},
        "lending_funds": {"type": "NUMERIC(20,4)"},
        "acc_receivable": {"type": "NUMERIC(20,4)"},
        "st_fin_payable": {"type": "NUMERIC(20,4)"},
        "payables": {"type": "NUMERIC(20,4)"},
        "hfs_assets": {"type": "NUMERIC(20,4)"},
        "hfs_sales": {"type": "NUMERIC(20,4)"},
        "cost_fin_assets": {"type": "NUMERIC(20,4)"},
        "fair_value_fin_assets": {"type": "NUMERIC(20,4)"},
        "contract_assets": {"type": "NUMERIC(20,4)"},
        "contract_liab": {"type": "NUMERIC(20,4)"},
        "accounts_receiv_bill": {"type": "NUMERIC(20,4)"},
        "accounts_pay": {"type": "NUMERIC(20,4)"},
        "oth_rcv_total": {"type": "NUMERIC(20,4)"},
        "fix_assets_total": {"type": "NUMERIC(20,4)"}
    }
    
    # 7.数据验证规则
    # validations = [
    #     lambda df: (df['total_assets'] == df['total_liab'] + df['total_hldr_eqy_inc_min_int']).all()
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