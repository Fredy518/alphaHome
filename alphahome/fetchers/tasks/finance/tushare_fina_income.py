from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd

from ...sources.tushare import TushareTask
from ...task_decorator import task_register
from ...tools.batch_utils import generate_natural_day_batches
from ...tools.calendar import get_trade_days_between


@task_register()
class TushareFinaIncomeTask(TushareTask):
    """股票利润表数据任务

    获取上市公司利润表数据，包括营业收入、营业成本、营业利润等数据。
    该任务使用Tushare的income接口获取数据。
    """

    # 1.核心属性
    name = "tushare_fina_income"
    description = "获取股票利润表数据"
    table_name = "tushare_fina_income"
    primary_keys = ["ts_code", "end_date", "f_ann_date"]
    date_column = "end_date"
    default_start_date = "19901231"  # 最早的财报日期

    # 2.自定义索引
    indexes = [
        {"name": "idx_fina_income_code", "columns": "ts_code"},
        {"name": "idx_fina_income_end_date", "columns": "end_date"},
        {"name": "idx_fina_income_ann_date", "columns": "f_ann_date"},
        {"name": "idx_fina_income_report_type", "columns": "report_type"},
        {"name": "idx_fina_income_update_time", "columns": "update_time"},
    ]

    # 3.Tushare特有属性
    api_name = "income_vip"
    fields = [
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "report_type",
        "comp_type",
        "basic_eps",
        "diluted_eps",
        "total_revenue",
        "revenue",
        "int_income",
        "prem_earned",
        "comm_income",
        "n_commis_income",
        "n_oth_income",
        "n_oth_b_income",
        "prem_income",
        "out_prem",
        "une_prem_reser",
        "reins_income",
        "n_sec_tb_income",
        "n_sec_uw_income",
        "n_asset_mg_income",
        "oth_b_income",
        "fv_value_chg_gain",
        "invest_income",
        "ass_invest_income",
        "forex_gain",
        "total_cogs",
        "oper_cost",
        "int_exp",
        "comm_exp",
        "biz_tax_surchg",
        "sell_exp",
        "admin_exp",
        "fin_exp",
        "assets_impair_loss",
        "prem_refund",
        "compens_payout",
        "reser_insur_liab",
        "div_payt",
        "reins_exp",
        "oper_exp",
        "compens_payout_refu",
        "insur_reser_refu",
        "reins_cost_refund",
        "other_bus_cost",
        "operate_profit",
        "non_oper_income",
        "non_oper_exp",
        "nca_disploss",
        "total_profit",
        "income_tax",
        "n_income",
        "n_income_attr_p",
        "minority_gain",
        "oth_compr_income",
        "t_compr_income",
        "compr_inc_attr_p",
        "compr_inc_attr_m_s",
        "ebit",
        "ebitda",
        "insurance_exp",
        "undist_profit",
        "distable_profit",
        "rd_exp",
        "fin_exp_int_exp",
        "fin_exp_int_inc",
        "transfer_surplus_rese",
        "transfer_housing_imprest",
        "transfer_oth",
        "adj_lossgain",
        "withdra_legal_surplus",
        "withdra_legal_pubfund",
        "withdra_biz_devfund",
        "withdra_rese_fund",
        "withdra_oth_ersu",
        "workers_welfare",
        "distr_profit_shrhder",
        "prfshare_payable_dvd",
        "comshare_payable_dvd",
        "capit_comstock_div",
    ]

    # 4.数据类型转换
    transformations = {
        "report_type": lambda x: int(x) if pd.notna(x) else None,
        "comp_type": lambda x: int(x) if pd.notna(x) else None,
        "basic_eps": float,
        "diluted_eps": float,
        "total_revenue": float,
        "revenue": float,
        "int_income": float,
        "prem_earned": float,
        "comm_income": float,
        "n_commis_income": float,
        "n_oth_income": float,
        "n_oth_b_income": float,
        "prem_income": float,
        "out_prem": float,
        "une_prem_reser": float,
        "reins_income": float,
        "n_sec_tb_income": float,
        "n_sec_uw_income": float,
        "n_asset_mg_income": float,
        "oth_b_income": float,
        "fv_value_chg_gain": float,
        "invest_income": float,
        "ass_invest_income": float,
        "forex_gain": float,
        "total_cogs": float,
        "oper_cost": float,
        "int_exp": float,
        "comm_exp": float,
        "biz_tax_surchg": float,
        "sell_exp": float,
        "admin_exp": float,
        "fin_exp": float,
        "assets_impair_loss": float,
        "prem_refund": float,
        "compens_payout": float,
        "reser_insur_liab": float,
        "div_payt": float,
        "reins_exp": float,
        "oper_exp": float,
        "compens_payout_refu": float,
        "insur_reser_refu": float,
        "reins_cost_refund": float,
        "other_bus_cost": float,
        "operate_profit": float,
        "non_oper_income": float,
        "non_oper_exp": float,
        "nca_disploss": float,
        "total_profit": float,
        "income_tax": float,
        "n_income": float,
        "n_income_attr_p": float,
        "minority_gain": float,
        "oth_compr_income": float,
        "t_compr_income": float,
        "compr_inc_attr_p": float,
        "compr_inc_attr_m_s": float,
        "ebit": float,
        "ebitda": float,
        "insurance_exp": float,
        "undist_profit": float,
        "distable_profit": float,
        "rd_exp": float,
        "fin_exp_int_exp": float,
        "fin_exp_int_inc": float,
        "transfer_surplus_rese": float,
        "transfer_housing_imprest": float,
        "transfer_oth": float,
        "adj_lossgain": float,
        "withdra_legal_surplus": float,
        "withdra_legal_pubfund": float,
        "withdra_biz_devfund": float,
        "withdra_rese_fund": float,
        "withdra_oth_ersu": float,
        "workers_welfare": float,
        "distr_profit_shrhder": float,
        "prfshare_payable_dvd": float,
        "comshare_payable_dvd": float,
        "capit_comstock_div": float,
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
        "basic_eps": {"type": "NUMERIC(20,4)"},
        "diluted_eps": {"type": "NUMERIC(20,4)"},
        "total_revenue": {"type": "NUMERIC(20,4)"},
        "revenue": {"type": "NUMERIC(20,4)"},
        "int_income": {"type": "NUMERIC(20,4)"},
        "prem_earned": {"type": "NUMERIC(20,4)"},
        "comm_income": {"type": "NUMERIC(20,4)"},
        "n_commis_income": {"type": "NUMERIC(20,4)"},
        "n_oth_income": {"type": "NUMERIC(20,4)"},
        "n_oth_b_income": {"type": "NUMERIC(20,4)"},
        "prem_income": {"type": "NUMERIC(20,4)"},
        "out_prem": {"type": "NUMERIC(20,4)"},
        "une_prem_reser": {"type": "NUMERIC(20,4)"},
        "reins_income": {"type": "NUMERIC(20,4)"},
        "n_sec_tb_income": {"type": "NUMERIC(20,4)"},
        "n_sec_uw_income": {"type": "NUMERIC(20,4)"},
        "n_asset_mg_income": {"type": "NUMERIC(20,4)"},
        "oth_b_income": {"type": "NUMERIC(20,4)"},
        "fv_value_chg_gain": {"type": "NUMERIC(20,4)"},
        "invest_income": {"type": "NUMERIC(20,4)"},
        "ass_invest_income": {"type": "NUMERIC(20,4)"},
        "forex_gain": {"type": "NUMERIC(20,4)"},
        "total_cogs": {"type": "NUMERIC(20,4)"},
        "oper_cost": {"type": "NUMERIC(20,4)"},
        "int_exp": {"type": "NUMERIC(20,4)"},
        "comm_exp": {"type": "NUMERIC(20,4)"},
        "biz_tax_surchg": {"type": "NUMERIC(20,4)"},
        "sell_exp": {"type": "NUMERIC(20,4)"},
        "admin_exp": {"type": "NUMERIC(20,4)"},
        "fin_exp": {"type": "NUMERIC(20,4)"},
        "assets_impair_loss": {"type": "NUMERIC(20,4)"},
        "prem_refund": {"type": "NUMERIC(20,4)"},
        "compens_payout": {"type": "NUMERIC(20,4)"},
        "reser_insur_liab": {"type": "NUMERIC(20,4)"},
        "div_payt": {"type": "NUMERIC(20,4)"},
        "reins_exp": {"type": "NUMERIC(20,4)"},
        "oper_exp": {"type": "NUMERIC(20,4)"},
        "compens_payout_refu": {"type": "NUMERIC(20,4)"},
        "insur_reser_refu": {"type": "NUMERIC(20,4)"},
        "reins_cost_refund": {"type": "NUMERIC(20,4)"},
        "other_bus_cost": {"type": "NUMERIC(20,4)"},
        "operate_profit": {"type": "NUMERIC(20,4)"},
        "non_oper_income": {"type": "NUMERIC(20,4)"},
        "non_oper_exp": {"type": "NUMERIC(20,4)"},
        "nca_disploss": {"type": "NUMERIC(20,4)"},
        "total_profit": {"type": "NUMERIC(20,4)"},
        "income_tax": {"type": "NUMERIC(20,4)"},
        "n_income": {"type": "NUMERIC(20,4)"},
        "n_income_attr_p": {"type": "NUMERIC(20,4)"},
        "minority_gain": {"type": "NUMERIC(20,4)"},
        "oth_compr_income": {"type": "NUMERIC(20,4)"},
        "t_compr_income": {"type": "NUMERIC(20,4)"},
        "compr_inc_attr_p": {"type": "NUMERIC(20,4)"},
        "compr_inc_attr_m_s": {"type": "NUMERIC(20,4)"},
        "ebit": {"type": "NUMERIC(20,4)"},
        "ebitda": {"type": "NUMERIC(20,4)"},
        "insurance_exp": {"type": "NUMERIC(20,4)"},
        "undist_profit": {"type": "NUMERIC(20,4)"},
        "distable_profit": {"type": "NUMERIC(20,4)"},
        "rd_exp": {"type": "NUMERIC(20,4)"},
        "fin_exp_int_exp": {"type": "NUMERIC(20,4)"},
        "fin_exp_int_inc": {"type": "NUMERIC(20,4)"},
        "transfer_surplus_rese": {"type": "NUMERIC(20,4)"},
        "transfer_housing_imprest": {"type": "NUMERIC(20,4)"},
        "transfer_oth": {"type": "NUMERIC(20,4)"},
        "adj_lossgain": {"type": "NUMERIC(20,4)"},
        "withdra_legal_surplus": {"type": "NUMERIC(20,4)"},
        "withdra_legal_pubfund": {"type": "NUMERIC(20,4)"},
        "withdra_biz_devfund": {"type": "NUMERIC(20,4)"},
        "withdra_rese_fund": {"type": "NUMERIC(20,4)"},
        "withdra_oth_ersu": {"type": "NUMERIC(20,4)"},
        "workers_welfare": {"type": "NUMERIC(20,4)"},
        "distr_profit_shrhder": {"type": "NUMERIC(20,4)"},
        "prfshare_payable_dvd": {"type": "NUMERIC(20,4)"},
        "comshare_payable_dvd": {"type": "NUMERIC(20,4)"},
        "capit_comstock_div": {"type": "NUMERIC(20,4)"},
    }

    # 7.数据验证规则
    # validations = [
    #     lambda df: (df['total_profit'] == df['income_befr_tax'] + df['income_tax']).all() | df['total_profit'].isnull() | df['income_befr_tax'].isnull() | df['income_tax'].isnull()
    # ]

    async def get_batch_list(self, **kwargs) -> List[Dict]:
        """生成批处理参数列表 (使用自然日批次工具)

        Args:
            **kwargs: 查询参数，包括start_date、end_date、ts_code等

        Returns:
            List[Dict]: 批处理参数列表
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        ts_code = kwargs.get("ts_code")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 必须提供 start_date 和 end_date 参数")
            return []

        self.logger.info(
            f"任务 {self.name}: 使用自然日批次工具生成批处理列表，范围: {start_date} 到 {end_date}"
        )

        try:
            # 使用自然日批次生成工具
            batch_list = await generate_natural_day_batches(
                start_date=start_date,
                end_date=end_date,
                batch_size=365,  # 使用365天作为批次大小
                ts_code=ts_code,
                logger=self.logger,
            )
            return batch_list
        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成自然日批次时出错: {e}", exc_info=True
            )
            return []
