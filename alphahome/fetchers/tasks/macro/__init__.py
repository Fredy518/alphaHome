from .tushare_macro_cpi import TushareMacroCpiTask
# from .tushare_macro_hibor import TushareMacroHiborTask 
from .tushare_macro_pmi import TushareMacroPmiTask
from .tushare_macro_ppi import TushareMacroPpiTask
from .tushare_macro_shibor import TushareMacroShiborTask
from .tushare_macro_ecocal import TushareMacroEcocalTask
from .tushare_macro_yieldcurve import TushareMacroYieldCurveTask
from .tushare_macro_sf import TushareMacroSFTTask
from .tushare_macro_cnm import TushareMacroCNMTask
from .macro_release_calendar import MacroReleaseCalendarTask

# AkShare 数据源任务
from .akshare_macro_bond_rate import AkShareMacroBondRateTask
from .akshare_macro_china_rmb_fixing import AkShareMacroChinaRmbFixingTask
from .akshare_macro_china_market_margin import (
    AkShareMacroChinaMarketMarginSHTask,
    AkShareMacroChinaMarketMarginSZTask,
)
from .akshare_macro_ths_rmb_loan import AkShareMacroThsRmbLoanTask
from .akshare_macro_ths_rmb_deposit import AkShareMacroThsRmbDepositTask
from .akshare_macro_china_nbs_nation import AkShareMacroChinaNBSNationTask

__all__ = [
    # Tushare 宏观任务
    # "TushareMacroHiborTask", # Hibor 数据源已下线
    "TushareMacroShiborTask",
    "TushareMacroCpiTask",
    "TushareMacroPmiTask",
    "TushareMacroPpiTask",
    "TushareMacroEcocalTask",
    "TushareMacroYieldCurveTask",
    "TushareMacroSFTTask",
    "TushareMacroCNMTask",
    "MacroReleaseCalendarTask",
    # AkShare 宏观任务
    "AkShareMacroBondRateTask",
    "AkShareMacroChinaRmbFixingTask",
    "AkShareMacroChinaMarketMarginSHTask",
    "AkShareMacroChinaMarketMarginSZTask",
    "AkShareMacroThsRmbLoanTask",
    "AkShareMacroThsRmbDepositTask",
    "AkShareMacroChinaNBSNationTask",
]
