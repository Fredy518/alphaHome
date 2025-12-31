"""估值计算器"""

from typing import Optional
from datetime import date
import pandas as pd

from ..core.portfolio import Portfolio


class Valuator:
    """
    估值计算器
    
    职责：
    - 计算组合市值
    - 计算组合净值
    - 处理净值对齐
    """
    
    def __init__(self):
        pass
    
    def calc_market_value(self, portfolio: Portfolio) -> float:
        """计算组合市值"""
        return float(portfolio.market_value)
    
    def calc_nav(self, market_value: float, initial_value: float) -> float:
        """计算组合净值"""
        if initial_value <= 0:
            return 1.0
        return market_value / initial_value
    
    def update_portfolio_nav(self, portfolio: Portfolio, nav_series: pd.Series, dt: date) -> None:
        """更新组合中所有持仓的净值"""
        portfolio.update_nav(nav_series, dt)
    
    def align_nav_panel(
        self,
        nav_panel: pd.DataFrame,
        calendar: pd.DatetimeIndex,
        fill_method: str = 'ffill'
    ) -> pd.DataFrame:
        """
        对齐净值面板到指定日历
        
        Args:
            nav_panel: 原始净值面板
            calendar: 目标日历
            fill_method: 填充方法 ('ffill' | 'bfill' | None)
        
        Returns:
            对齐后的净值面板
        """
        aligned = nav_panel.reindex(calendar)
        
        if fill_method == 'ffill':
            aligned = aligned.ffill()
        elif fill_method == 'bfill':
            aligned = aligned.bfill()
        
        return aligned
