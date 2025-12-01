from .tushare_macro_cpi import TushareMacroCpiTask
# from .tushare_macro_hibor import TushareMacroHiborTask 
from .tushare_macro_shibor import TushareMacroShiborTask
from .tushare_macro_ecocal import TushareMacroEcocalTask
from .tushare_macro_yieldcurve import TushareMacroYieldCurveTask
from .tushare_macro_sf import TushareMacroSFTTask
from .tushare_macro_cnm import TushareMacroCNMTask

# AkShare 数据源任务
from .akshare_macro_bond_rate import AkShareMacroBondRateTask

__all__ = [
    # Tushare 宏观任务
    # "TushareMacroHiborTask", # Hibor 数据源已下线
    "TushareMacroShiborTask",
    "TushareMacroCpiTask",
    "TushareMacroEcocalTask",
    "TushareMacroYieldCurveTask",
    "TushareMacroSFTTask",
    "TushareMacroCNMTask",
    # AkShare 宏观任务
    "AkShareMacroBondRateTask",
]
