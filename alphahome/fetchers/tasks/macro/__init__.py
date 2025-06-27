from .tushare_macro_cpi import TushareMacroCpiTask
# from .tushare_macro_hibor import TushareMacroHiborTask 
from .tushare_macro_shibor import TushareMacroShiborTask

__all__ = [
    # "TushareMacroHiborTask", # Hibor 数据源已下线
    "TushareMacroShiborTask",
    "TushareMacroCpiTask",
]
