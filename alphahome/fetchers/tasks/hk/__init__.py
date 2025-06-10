# Initializes the hk tasks module
from .tushare_hk_basic import TushareHKBasicTask
from .tushare_hk_daily import TushareHKDailyTask

# from .tushare_hk_dailyadj import TushareHkDailyadjTask # 新增的任务

__all__ = [
    # "TushareHKAdjFactorTask", # 已删除
    "TushareHKDailyTask",
    "TushareHKBasicTask",
    # "TushareHkDailyadjTask"
]
