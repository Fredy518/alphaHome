#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Compatibility exports for Tinysoft stock-side P0 tasks.

Concrete task classes live in smaller modules grouped by business area. This
module remains as a stable import path for older callers.
"""

from __future__ import annotations

from .tinysoft_stock_basic_ext import TinySoftStockBasicExtTask
from .tinysoft_stock_events_ext import (
    TinySoftStockHolderChangeExtTask,
    TinySoftStockPublicTradeInfoTask,
    TinySoftStockRepurchaseExtTask,
    TinySoftStockUnlockScheduleTask,
)
from .tinysoft_stock_hsgt_ext import (
    TinySoftStockHsgtDailyTask,
    TinySoftStockHsgtHoldTask,
    TinySoftStockHsgtShortBalanceTask,
    TinySoftStockHsgtTop10Task,
)
from .tinysoft_stock_lending_ext import (
    TinySoftStockLendingBalanceTask,
    TinySoftStockLendingSummaryTask,
    TinySoftStockLendingTradeTask,
)
from .tinysoft_stock_margin import TinySoftStockMarginDetailTask, TinySoftStockMarginTask
from .tinysoft_stock_p0_base import (
    TinySoftHsgtChannelTask,
    TinySoftMarketCodeInfoArrayTask,
    TinySoftStockSymbolInfoArrayTask,
)
from .tinysoft_stock_pledge_ext import (
    TinySoftStockPledgeBalanceTask,
    TinySoftStockPledgeDetailTask,
    TinySoftStockPledgeRateTask,
    TinySoftStockPledgeSummaryTask,
)

__all__ = [
    "TinySoftStockSymbolInfoArrayTask",
    "TinySoftHsgtChannelTask",
    "TinySoftMarketCodeInfoArrayTask",
    "TinySoftStockBasicExtTask",
    "TinySoftStockHsgtDailyTask",
    "TinySoftStockHsgtTop10Task",
    "TinySoftStockHsgtHoldTask",
    "TinySoftStockHsgtShortBalanceTask",
    "TinySoftStockLendingSummaryTask",
    "TinySoftStockLendingTradeTask",
    "TinySoftStockLendingBalanceTask",
    "TinySoftStockMarginTask",
    "TinySoftStockMarginDetailTask",
    "TinySoftStockPublicTradeInfoTask",
    "TinySoftStockUnlockScheduleTask",
    "TinySoftStockHolderChangeExtTask",
    "TinySoftStockRepurchaseExtTask",
    "TinySoftStockPledgeSummaryTask",
    "TinySoftStockPledgeDetailTask",
    "TinySoftStockPledgeBalanceTask",
    "TinySoftStockPledgeRateTask",
]
