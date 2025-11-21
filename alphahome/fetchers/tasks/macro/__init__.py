from .tushare_macro_cpi import TushareMacroCpiTask
# from .tushare_macro_hibor import TushareMacroHiborTask 
from .tushare_macro_shibor import TushareMacroShiborTask
from .tushare_macro_ecocal import TushareMacroEcocalTask
from .tushare_macro_yieldcurve import TushareMacroYieldCurveTask
from .tushare_macro_sf import TushareMacroSFTTask
from .tushare_macro_cnm import TushareMacroCNMTask

__all__ = [
    # "TushareMacroHiborTask", # Hibor 数据源已下线
    "TushareMacroShiborTask",
    "TushareMacroCpiTask",
    "TushareMacroEcocalTask",
    "TushareMacroYieldCurveTask",
    "TushareMacroSFTTask",
    "TushareMacroCNMTask",
]
