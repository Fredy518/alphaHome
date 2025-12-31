"""
回撤分析器

本模块实现回撤相关的分析功能，包括：
- 最大回撤计算
- 水下曲线
- 前 N 大回撤周期（不重叠）
- 回撤持续时间统计

符号约定：
- max_drawdown: 输出为正数（如 0.1 表示 10% 回撤）
- underwater_curve: 输出为非正数（范围 [-1, 0]）
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from ._constants import DEFAULT_TOP_N_DRAWDOWNS

logger = logging.getLogger(__name__)


@dataclass
class DrawdownPeriod:
    """
    回撤周期数据结构
    
    属性:
        peak_date: 回撤开始前的最高点日期
        trough_date: 回撤最低点日期
        recovery_date: NAV 首次回到或超过 peak 的日期，未恢复为 None
        drawdown: 回撤幅度（正数，如 0.1 表示 10%）
        duration_days: 峰值到谷值的交易日数
        recovery_days: 谷值到恢复的交易日数，未恢复为 None
    """
    peak_date: Optional[date]
    trough_date: Optional[date]
    recovery_date: Optional[date]
    drawdown: float
    duration_days: int
    recovery_days: Optional[int]
    
    def to_dict(self) -> dict:
        """转换为字典，便于 JSON 序列化"""
        return {
            'peak_date': str(self.peak_date) if self.peak_date else None,
            'trough_date': str(self.trough_date) if self.trough_date else None,
            'recovery_date': str(self.recovery_date) if self.recovery_date else None,
            'drawdown': self.drawdown,
            'duration_days': self.duration_days,
            'recovery_days': self.recovery_days,
        }


class DrawdownAnalyzer:
    """
    回撤分析器
    
    提供回撤相关的各类分析功能。
    
    示例:
        >>> analyzer = DrawdownAnalyzer()
        >>> nav = pd.Series([1.0, 1.1, 0.9, 1.0, 1.2], 
        ...                 index=pd.date_range('2024-01-01', periods=5))
        >>> dd, peak, trough = analyzer.max_drawdown(nav)
        >>> print(f"最大回撤: {dd:.2%}")
        最大回撤: 18.18%
    """
    
    def _is_valid_nav(self, nav_series: pd.Series) -> bool:
        """检查净值序列是否有效"""
        if nav_series is None:
            return False
        if not isinstance(nav_series, pd.Series):
            return False
        nav_series = nav_series.dropna()
        if nav_series.empty:
            return False
        return True
    
    def underwater_curve(self, nav_series: pd.Series) -> pd.Series:
        """
        计算水下曲线
        
        公式: DD_t = NAV_t / max(NAV_s for s <= t) - 1
        
        参数:
            nav_series: 净值序列
        
        返回:
            pd.Series: 水下曲线，值 <= 0
        """
        if not self._is_valid_nav(nav_series):
            return pd.Series(dtype=float)
        
        nav_series = nav_series.dropna()
        
        # 计算累计最大值
        cummax = nav_series.cummax()
        
        # 计算水下曲线（非正数）
        underwater = (nav_series - cummax) / cummax
        
        return underwater
    
    def max_drawdown(self, nav_series: pd.Series) -> Tuple[float, Optional[date], Optional[date]]:
        """
        计算最大回撤
        
        参数:
            nav_series: 净值序列
        
        返回:
            (最大回撤(正数), 峰值日期, 谷值日期)
            空数据返回 (NaN, None, None)
        """
        if not self._is_valid_nav(nav_series):
            return np.nan, None, None
        
        nav_series = nav_series.dropna()
        underwater = self.underwater_curve(nav_series)
        
        if underwater.empty:
            return np.nan, None, None
        
        # 找到最大回撤点（水下曲线最小值）
        trough_idx = underwater.idxmin()
        max_dd = float(abs(underwater.min()))
        
        # 找到回撤开始点（峰值）
        before_trough = nav_series.loc[:trough_idx]
        peak_idx = before_trough.idxmax() if not before_trough.empty else trough_idx
        
        # 转换为 date 类型
        peak_date = peak_idx.date() if hasattr(peak_idx, 'date') else peak_idx
        trough_date = trough_idx.date() if hasattr(trough_idx, 'date') else trough_idx
        
        return max_dd, peak_date, trough_date
    
    def _find_drawdown_periods(self, nav_series: pd.Series) -> List[DrawdownPeriod]:
        """
        识别所有回撤周期（基于峰谷恢复段分割）
        
        算法：
        1. 找到所有局部峰值点
        2. 对每个峰值，找到后续的谷值和恢复点
        3. 确保周期不重叠
        
        参数:
            nav_series: 净值序列
        
        返回:
            List[DrawdownPeriod]: 回撤周期列表
        """
        if not self._is_valid_nav(nav_series):
            return []
        
        nav_series = nav_series.dropna()
        if len(nav_series) < 2:
            return []
        
        periods = []
        cummax = nav_series.cummax()
        underwater = self.underwater_curve(nav_series)
        
        i = 0
        n = len(nav_series)
        
        while i < n:
            # 找到当前峰值点（cummax 等于当前值的点）
            if nav_series.iloc[i] >= cummax.iloc[i] * 0.9999:  # 容差处理浮点误差
                peak_idx = nav_series.index[i]
                peak_value = nav_series.iloc[i]
                
                # 寻找后续的谷值
                trough_idx = None
                trough_value = peak_value
                trough_pos = i
                
                j = i + 1
                while j < n:
                    if nav_series.iloc[j] < trough_value:
                        trough_value = nav_series.iloc[j]
                        trough_idx = nav_series.index[j]
                        trough_pos = j
                    # 如果恢复到峰值，结束当前周期
                    if nav_series.iloc[j] >= peak_value:
                        break
                    j += 1
                
                # 如果有回撤（谷值低于峰值）
                if trough_idx is not None and trough_value < peak_value:
                    drawdown = (peak_value - trough_value) / peak_value
                    
                    # 寻找恢复点
                    recovery_idx = None
                    recovery_pos = None
                    for k in range(trough_pos + 1, n):
                        if nav_series.iloc[k] >= peak_value:
                            recovery_idx = nav_series.index[k]
                            recovery_pos = k
                            break
                    
                    # 计算持续时间
                    duration_days = trough_pos - i
                    recovery_days = None
                    if recovery_pos is not None:
                        recovery_days = recovery_pos - trough_pos
                    
                    # 转换日期
                    peak_date = peak_idx.date() if hasattr(peak_idx, 'date') else peak_idx
                    trough_date = trough_idx.date() if hasattr(trough_idx, 'date') else trough_idx
                    recovery_date = None
                    if recovery_idx is not None:
                        recovery_date = recovery_idx.date() if hasattr(recovery_idx, 'date') else recovery_idx
                    
                    period = DrawdownPeriod(
                        peak_date=peak_date,
                        trough_date=trough_date,
                        recovery_date=recovery_date,
                        drawdown=drawdown,
                        duration_days=duration_days,
                        recovery_days=recovery_days
                    )
                    periods.append(period)
                    
                    # 移动到恢复点或谷值后继续
                    if recovery_pos is not None:
                        i = recovery_pos
                    else:
                        i = trough_pos + 1
                    continue
            
            i += 1
        
        return periods
    
    def top_n_drawdowns(self, nav_series: pd.Series, n: int = DEFAULT_TOP_N_DRAWDOWNS) -> List[DrawdownPeriod]:
        """
        获取前 N 大回撤周期
        
        基于峰谷恢复段分割，确保周期不重叠。
        按回撤深度降序排列。
        
        参数:
            nav_series: 净值序列
            n: 返回的回撤周期数，默认 5
        
        返回:
            List[DrawdownPeriod]: 前 N 大回撤周期列表
        """
        periods = self._find_drawdown_periods(nav_series)
        
        if not periods:
            return []
        
        # 按回撤深度降序排序
        periods.sort(key=lambda x: x.drawdown, reverse=True)
        
        return periods[:n]
    
    def avg_drawdown_duration(self, nav_series: pd.Series) -> float:
        """
        计算平均回撤持续时间
        
        仅计算已恢复的回撤周期。
        
        参数:
            nav_series: 净值序列
        
        返回:
            float: 平均持续时间（交易日），无已恢复周期返回 NaN
        """
        periods = self._find_drawdown_periods(nav_series)
        
        # 筛选已恢复的周期
        recovered_periods = [p for p in periods if p.recovery_date is not None]
        
        if not recovered_periods:
            return np.nan
        
        # 计算平均持续时间（峰值到谷值）
        total_duration = sum(p.duration_days for p in recovered_periods)
        avg_duration = float(total_duration / len(recovered_periods))
        
        return avg_duration
    
    def max_drawdown_duration(self, nav_series: pd.Series) -> int:
        """
        计算最大回撤持续时间
        
        若最大回撤尚未恢复，计算到最后一个日期。
        
        参数:
            nav_series: 净值序列
        
        返回:
            int: 最大回撤持续时间（交易日），空数据返回 0
        """
        if not self._is_valid_nav(nav_series):
            return 0
        
        nav_series = nav_series.dropna()
        max_dd, peak_date, trough_date = self.max_drawdown(nav_series)
        
        if np.isnan(max_dd) or peak_date is None or trough_date is None:
            return 0
        
        # 找到峰值和谷值在序列中的位置
        peak_idx = None
        trough_idx = None
        
        for i, idx in enumerate(nav_series.index):
            idx_date = idx.date() if hasattr(idx, 'date') else idx
            if idx_date == peak_date and peak_idx is None:
                peak_idx = i
            if idx_date == trough_date:
                trough_idx = i
        
        if peak_idx is None or trough_idx is None:
            return 0
        
        duration = trough_idx - peak_idx
        return duration
