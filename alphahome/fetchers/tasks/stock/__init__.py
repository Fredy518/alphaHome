# 股票数据任务包
from .tushare_stock_adjfactor import TushareStockAdjFactorTask
from .tushare_stock_basic import TushareStockBasicTask
from .tushare_stock_chips import TushareStockChipsTask
from .tushare_stock_daily import TushareStockDailyTask
from .tushare_stock_dailybasic import TushareStockDailyBasicTask
from .tushare_stock_weekly import TushareStockWeeklyTask
from .tushare_stock_monthly import TushareStockMonthlyTask
from .tushare_stock_dividend import TushareStockDividendTask
from .tushare_stock_factor import TushareStockFactorProTask
from .tushare_stock_report_rc import TushareStockReportRcTask
from .tushare_stock_holdernumber import TushareStockHolderNumberTask
from .tushare_stock_moneyflow import TushareStockMoneyFlowTask
from .tushare_stock_limitlist import TushareStockLimitListTask
from .tushare_stock_margin import TushareStockMarginTask
from .tushare_stock_margindetail import TushareStockMarginDetailTask
from .tushare_stock_thsindex import TushareStockThsIndexTask
from .tushare_stock_thsdaily import TushareStockThsDailyTask
from .tushare_stock_thsmember import TushareStockThsMemberTask
from .tushare_stock_dcindex import TushareStockDcIndexTask
from .tushare_stock_dcdaily import TushareStockDcDailyTask
from .tushare_stock_dcmember import TushareStockDcMemberTask
from .tushare_stock_kpllist import TushareStockKplListTask
from .tushare_stock_kplconcept import TushareStockKplConceptTask
from .tushare_stock_kplmember import TushareStockKplMemberTask  
from .tushare_stock_ahcomparison import TushareStockAHComparisonTask
from .tushare_stock_st import TushareStockSTTask
from .tushare_stock_limitprice import TushareStockLimitPriceTask
from .tushare_stock_namechange import TushareStockNameChangeTask
from .tushare_stock_sharefloat import TushareStockShareFloatTask
from .tushare_stock_pledgestat import TushareStockPledgeStatTask
from .tushare_stock_repurchase import TushareStockRepurchaseTask
from .tushare_stock_holdertrade import TushareStockHolderTradeTask
from .tushare_stock_toplist import TushareStockTopListTask
from .tushare_stock_topinst import TushareStockTopInstTask
from .tushare_stock_blocktrade import TushareStockBlockTradeTask
from .tushare_stock_moneyflow_hsgt import TushareStockMoneyflowHsgtTask
from .tushare_stock_hsgt_top10 import TushareStockHsgtTop10Task
from .tushare_stock_hk_hold import TushareStockHkHoldTask

from .akshare_stock_limitup_reason import AkShareStockLimitupReasonTask
from .akshare_stock_analyst_rank_em import AkShareStockAnalystRankEmTask
from .tinysoft_stock_minute import TinySoftStockMinuteTask

__all__ = [
    "TushareStockBasicTask",
    "TushareStockDailyTask",
    "TushareStockDailyBasicTask",
    "TushareStockAdjFactorTask",
    "TushareStockWeeklyTask",
    "TushareStockMonthlyTask",
    "TushareStockReportRcTask",
    "TushareStockFactorProTask",
    "TushareStockChipsTask",
    "TushareStockDividendTask",
    "TushareStockHolderNumberTask",
    "TushareStockMoneyFlowTask",
    "TushareStockLimitListTask",
    "TushareStockMarginTask",
    "TushareStockMarginDetailTask",
    "TushareStockThsIndexTask",
    "TushareStockThsDailyTask",
    "TushareStockThsMemberTask",
    "TushareStockDcIndexTask",
    "TushareStockDcDailyTask",  
    "TushareStockDcMemberTask",
    "TushareStockKplListTask",
    "TushareStockKplConceptTask",
    "TushareStockKplMemberTask",
    "TushareStockAHComparisonTask",
    "TushareStockSTTask",
    "TushareStockLimitPriceTask",
    "TushareStockNameChangeTask",
    "TushareStockShareFloatTask",
    "TushareStockPledgeStatTask",
    "TushareStockRepurchaseTask",
    "TushareStockHolderTradeTask",
    "TushareStockTopListTask",
    "TushareStockTopInstTask",
    "TushareStockBlockTradeTask",
    "TushareStockMoneyflowHsgtTask",
    "TushareStockHsgtTop10Task",
    "TushareStockHkHoldTask",
    "AkShareStockLimitupReasonTask",
    "AkShareStockAnalystRankEmTask",
    "TinySoftStockMinuteTask",
]
