#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
通用数据变换函数

提供标准化、去极值、滚动计算等常用变换，供处理任务复用。

Notes
-----
所有函数都处理边缘情况（零方差、常数列、空数据），返回安全的结果而非 inf/NaN。
"""

from __future__ import annotations

from typing import List, Optional, Literal

import numpy as np
import pandas as pd


# =============================================================================
# 基础标准化函数 (Task 1.1)
# =============================================================================

def zscore(
    series: pd.Series,
    mean: Optional[float] = None,
    std: Optional[float] = None,
) -> pd.Series:
    """Z-Score 标准化
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    mean : float, optional
        预计算的均值，None 时从 series 计算
    std : float, optional
        预计算的标准差，None 时从 series 计算
    
    Returns
    -------
    pd.Series
        标准化后的序列，零方差时返回全 0（保留原始 NaN 位置）
    
    Notes
    -----
    处理零方差边界情况：当标准差为 0、NaN 或接近零（相对于均值）时，
    返回全零序列而非 NaN/inf。使用相对容差来处理浮点精度问题。
    
    符合特征层接口契约：
    - NaN 输入产生 NaN 输出（Requirements 6.5）
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    # 记录原始 NaN 位置，用于后续恢复
    nan_mask = series.isna()
    
    if mean is None:
        mean = series.mean()
    if std is None:
        std = series.std()
    
    # 处理零方差和近零方差（浮点精度问题）
    # 使用相对容差：std 相对于 |mean| 非常小时视为零方差
    if std == 0 or pd.isna(std):
        result = pd.Series(0.0, index=series.index)
        # 恢复原始 NaN 位置
        result[nan_mask] = np.nan
        return result
    
    # 检查相对容差：如果 std / |mean| < 1e-14，视为零方差
    # 对于 mean 接近 0 的情况，使用绝对容差 1e-14
    abs_mean = abs(mean) if not pd.isna(mean) else 0.0
    if abs_mean > 0 and std / abs_mean < 1e-14:
        result = pd.Series(0.0, index=series.index)
        # 恢复原始 NaN 位置
        result[nan_mask] = np.nan
        return result
    if abs_mean == 0 and std < 1e-14:
        result = pd.Series(0.0, index=series.index)
        # 恢复原始 NaN 位置
        result[nan_mask] = np.nan
        return result
    
    return (series - mean) / std


def minmax_scale(
    series: pd.Series,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> pd.Series:
    """Min-Max 缩放到 [0, 1]
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    min_val : float, optional
        预计算的最小值
    max_val : float, optional
        预计算的最大值
    
    Returns
    -------
    pd.Series
        缩放后的序列，常数列返回全 0（保留原始 NaN 位置）
    
    Notes
    -----
    处理零范围边界情况：当 max == min 时，返回全零序列而非 NaN/inf。
    
    符合特征层接口契约：
    - NaN 输入产生 NaN 输出（Requirements 6.5）
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    # 记录原始 NaN 位置，用于后续恢复
    nan_mask = series.isna()
    
    if min_val is None:
        min_val = series.min()
    if max_val is None:
        max_val = series.max()
    
    range_val = max_val - min_val
    if range_val == 0 or pd.isna(range_val):
        result = pd.Series(0.0, index=series.index)
        # 恢复原始 NaN 位置
        result[nan_mask] = np.nan
        return result
    
    return (series - min_val) / range_val



# =============================================================================
# 滚动计算函数 (Task 1.4)
# =============================================================================

def rolling_zscore(
    series: pd.Series,
    window: int,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """滚动 Z-Score 标准化
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    window : int
        滚动窗口大小
    min_periods : int, optional
        最小观测数，默认为 window // 2
    
    Returns
    -------
    pd.Series
        滚动标准化后的序列
    
    Notes
    -----
    处理零方差情况：当滚动标准差为 0 时，该位置返回 0 而非 NaN/inf。
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    if min_periods is None:
        # 观测不足时保持 NaN，避免早期窗口产生伪信号
        min_periods = window
    
    mean = series.rolling(window, min_periods=min_periods).mean()
    std = series.rolling(window, min_periods=min_periods).std()
    
    # 对零方差位置返回 0，其他保持原始计算结果（不足窗口保持 NaN）
    std_safe = std.where(std != 0)
    result = (series - mean) / std_safe
    zero_var_mask = (std == 0)
    result = result.where(~zero_var_mask, 0.0)
    return result


def rolling_percentile(
    series: pd.Series,
    window: int,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """滚动百分位排名
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    window : int
        滚动窗口大小
    min_periods : int, optional
        最小观测数
    
    Returns
    -------
    pd.Series
        滚动百分位 [0, 1]
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    if min_periods is None:
        # 使用完整窗口，避免早期窗口的偏置百分位
        min_periods = window
    
    return series.rolling(window, min_periods=min_periods).rank(pct=True)


def rolling_sum(
    series: pd.Series,
    window: int,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """滚动求和
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    window : int
        滚动窗口大小
    min_periods : int, optional
        最小观测数，默认为 window（符合接口契约）
    
    Returns
    -------
    pd.Series
        滚动求和结果
    
    Notes
    -----
    符合特征层接口契约：
    - min_periods 默认等于 window（Requirements 6.7）
    - 不足窗口返回 NaN（Requirements 6.8）
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    if min_periods is None:
        min_periods = window
    
    return series.rolling(window, min_periods=min_periods).sum()


def rolling_rank(
    series: pd.Series,
    window: int,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """滚动排名（返回排名值而非百分位）
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    window : int
        滚动窗口大小
    min_periods : int, optional
        最小观测数，默认为 window（符合接口契约）
    
    Returns
    -------
    pd.Series
        滚动排名
    
    Notes
    -----
    符合特征层接口契约：
    - min_periods 默认等于 window（Requirements 6.7）
    - 不足窗口返回 NaN（Requirements 6.8）
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    if min_periods is None:
        min_periods = window
    
    return series.rolling(window, min_periods=min_periods).rank()



# =============================================================================
# 去极值和分箱函数 (Task 1.6)
# =============================================================================

def winsorize(
    series: pd.Series,
    window: int,
    n_std: float = 3.0,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """滚动去极值（Winsorize）
    
    将超过 n_std 倍滚动标准差的值截断到边界。
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    window : int
        滚动窗口大小
    n_std : float, default 3.0
        标准差倍数
    min_periods : int, optional
        最小观测数，默认为 window（符合接口契约）
    
    Returns
    -------
    pd.Series
        去极值后的序列
    
    Notes
    -----
    符合特征层接口契约：
    - min_periods 默认等于 window（Requirements 6.7）
    - 不足窗口返回 NaN（Requirements 6.8）
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    if min_periods is None:
        min_periods = window
    
    mean = series.rolling(window, min_periods=min_periods).mean()
    std = series.rolling(window, min_periods=min_periods).std()
    
    upper = mean + n_std * std
    lower = mean - n_std * std
    
    return series.clip(lower=lower, upper=upper)


def quantile_bins(
    series: pd.Series,
    boundaries: Optional[List[float]] = None,
    quantiles: Optional[List[float]] = None,
) -> pd.Series:
    """分位数分箱
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    boundaries : list of float, optional
        预计算的分位边界值
    quantiles : list of float, optional
        分位点列表（如 [0.25, 0.5, 0.75]），用于从 series 计算边界
    
    Returns
    -------
    pd.Series
        分箱后的整数序列
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    if boundaries is None:
        if quantiles is None:
            quantiles = [0.25, 0.5, 0.75]
        boundaries = list(series.quantile(quantiles))
    
    # pd.cut 使用 (-inf, b1], (b1, b2], ..., (bn, inf]
    bins = [-np.inf] + list(boundaries) + [np.inf]
    labels = list(range(len(bins) - 1))
    
    return pd.cut(series, bins=bins, labels=labels).astype(float)


# =============================================================================
# 收益率计算函数 (Task 3.1)
# =============================================================================

def diff_pct(series: pd.Series, periods: int = 1) -> pd.Series:
    """计算百分比变化
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    periods : int, default 1
        差分周期
    
    Returns
    -------
    pd.Series
        百分比变化
    
    Notes
    -----
    处理空数据：返回空 Series。
    
    符合特征层接口契约：
    - NaN 输入产生 NaN 输出（Requirements 6.5）
    - 除零返回 NaN（Requirements 6.6）
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    # 记录原始 NaN 位置，用于后续恢复
    nan_mask = series.isna()
    
    result = series.pct_change(periods=periods)
    
    # 将 inf 转换为 NaN（除零情况）
    result = result.replace([np.inf, -np.inf], np.nan)
    
    # 恢复原始 NaN 位置
    result[nan_mask] = np.nan
    
    return result


def log_return(series: pd.Series, periods: int = 1) -> pd.Series:
    """计算对数收益率
    
    Parameters
    ----------
    series : pd.Series
        价格序列
    periods : int, default 1
        差分周期
    
    Returns
    -------
    pd.Series
        对数收益率
    
    Notes
    -----
    处理空数据：返回空 Series。
    对数收益率 = ln(P_t / P_{t-periods})
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    base = series.shift(periods)
    safe_base = base.replace(0, np.nan)
    ratio = series / safe_base
    # 非正比值或无穷视为缺失，避免产生 +/-inf
    ratio = ratio.replace([np.inf, -np.inf, 0], np.nan)
    ratio = ratio.where(ratio > 0)
    return np.log(ratio)


def ema(series: pd.Series, span: int) -> pd.Series:
    """指数移动平均
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    span : int
        EMA 跨度
    
    Returns
    -------
    pd.Series
        EMA 结果
    
    Notes
    -----
    处理空数据：返回空 Series。
    使用 adjust=False 以获得标准 EMA 计算。
    
    符合特征层接口契约：
    - NaN 输入产生 NaN 输出（Requirements 6.5）
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    # 记录原始 NaN 位置，用于后续恢复
    nan_mask = series.isna()
    
    result = series.ewm(span=span, adjust=False).mean()
    
    # 恢复原始 NaN 位置
    result[nan_mask] = np.nan
    
    return result


# =============================================================================
# 滚动斜率函数 (Task 3.2)
# =============================================================================

def rolling_slope(
    series: pd.Series,
    window: int,
    min_periods: Optional[int] = None,
    method: Literal["ols", "roc"] = "ols",
) -> pd.Series:
    """滚动斜率/动量估计
    
    - method="ols": 在窗口内对 (t, y) 做简单线性回归，返回标准化后的斜率
      （x 使用 0,1,...,window-1，斜率可视为单位时间变化率）
    - method="roc": 使用窗口首尾的 Rate of Change 近似斜率
    
    Parameters
    ----------
    series : pd.Series
        输入序列
    window : int
        滚动窗口大小
    min_periods : int, optional
        最小观测数，默认为 max(3, window // 2)
    method : {"ols", "roc"}, default "ols"
        计算方法
    
    Returns
    -------
    pd.Series
        滚动斜率
    
    Notes
    -----
    - 对常数序列返回 0
    - 对短于 min_periods 的窗口返回 NaN
    - 处理空数据：返回空 Series
    """
    if series.empty:
        return pd.Series(dtype=float)
    
    if min_periods is None:
        min_periods = max(3, window // 2)
    
    if window <= 1:
        return pd.Series(np.nan, index=series.index)
    
    if method == "roc":
        # 简单动量近似斜率：末值相对首值的百分比变化
        shifted = series.shift(window - 1)
        return (series - shifted) / shifted
    
    # 默认 OLS 斜率：避免为每个窗口重复构造 x
    x = np.arange(window, dtype=float)
    x_mean = x.mean()
    x_var = np.sum((x - x_mean) ** 2)
    if x_var == 0:
        return pd.Series(0.0, index=series.index)
    
    def _slope(arr: np.ndarray) -> float:
        # arr 可能包含 NaN：仅在有效长度足够时计算
        mask = np.isfinite(arr)
        if mask.sum() < min_periods:
            return np.nan
        y = arr[mask]
        x_valid = x[-len(y):]  # 对齐到窗口尾部
        xv = x_valid - x_valid.mean()
        yv = y - y.mean()
        denom = np.sum(xv * xv)
        if denom == 0:
            return 0.0
        return float(np.sum(xv * yv) / denom)
    
    return series.rolling(window, min_periods=min_periods).apply(
        lambda arr: _slope(arr), raw=True
    )


# =============================================================================
# 高级特征函数 (Task 3.3)
# =============================================================================

def price_acceleration(
    price: pd.Series,
    long_window: int = 252,
    short_window: int = 63,
    vol_window: int = 20,
    min_periods_ratio: float = 0.5,
    vol_adjusted: bool = True,
) -> pd.DataFrame:
    """价格加速度特征
    
    计算价格趋势的梯度变化（加速度），用于衡量趋势的强化或减弱。
    
    计算逻辑：
    1. 对长期窗口（如1年）和短期窗口（如3个月）分别计算价格的线性回归斜率
    2. 可选：对斜率进行波动率调整（除以滚动波动率），使不同波动率环境下的斜率可比
    3. 计算加速度 = 短期斜率 - 长期斜率
       - 正值：趋势加速（上涨加速或下跌减速）
       - 负值：趋势减速（上涨减速或下跌加速）
    
    Parameters
    ----------
    price : pd.Series
        价格序列（建议使用复权价格）
    long_window : int, default 252
        长期窗口大小（约1年交易日）
    short_window : int, default 63
        短期窗口大小（约3个月交易日）
    vol_window : int, default 20
        波动率计算窗口
    min_periods_ratio : float, default 0.5
        最小观测数占窗口的比例
    vol_adjusted : bool, default True
        是否对斜率进行波动率调整
    
    Returns
    -------
    pd.DataFrame
        包含以下列：
        - slope_long: 长期斜率（波动率调整后，如果 vol_adjusted=True）
        - slope_short: 短期斜率（波动率调整后，如果 vol_adjusted=True）
        - acceleration: 加速度 = slope_short - slope_long
        - acceleration_zscore: 加速度的滚动 Z-Score
        - slope_ratio: 短期斜率 / 长期斜率（趋势强度比）
    
    Examples
    --------
    >>> import pandas as pd
    >>> prices = pd.Series([100, 101, 103, 106, 110, 115, 121, 128])
    >>> result = price_acceleration(prices, long_window=6, short_window=3)
    >>> print(result['acceleration'])
    
    Notes
    -----
    - 加速度为正且斜率为正：上涨趋势加速
    - 加速度为负且斜率为正：上涨趋势减速（可能见顶）
    - 加速度为正且斜率为负：下跌趋势减速（可能见底）
    - 加速度为负且斜率为负：下跌趋势加速
    - 处理空数据：返回空 DataFrame
    
    References
    ----------
    价格加速度是动量策略的二阶导数指标，常用于：
    - 趋势反转预警
    - 动量因子的增强
    - 择时信号的辅助判断
    """
    if price.empty:
        return pd.DataFrame()
    
    # 计算最小观测数
    long_min_periods = max(3, int(long_window * min_periods_ratio))
    short_min_periods = max(3, int(short_window * min_periods_ratio))
    
    # 计算滚动波动率（用于标准化）
    returns = price.pct_change()
    rolling_vol = returns.rolling(vol_window, min_periods=max(5, vol_window // 2)).std()
    
    # 安全处理零波动率
    rolling_vol = rolling_vol.replace(0, np.nan)
    
    # 计算长期和短期斜率
    slope_long_raw = rolling_slope(price, window=long_window, min_periods=long_min_periods, method="ols")
    slope_short_raw = rolling_slope(price, window=short_window, min_periods=short_min_periods, method="ols")
    
    # 波动率调整：将斜率除以价格水平和波动率，使其可比
    # 调整后的斜率 = 原始斜率 / (价格 * 波动率)
    # 这样不同价格水平和波动率环境下的斜率具有可比性
    if vol_adjusted:
        # 使用价格的滚动均值作为基准，避免单日价格波动影响
        price_base = price.rolling(vol_window, min_periods=5).mean()
        normalizer = price_base * rolling_vol
        normalizer = normalizer.replace(0, np.nan)
        
        slope_long = slope_long_raw / normalizer
        slope_short = slope_short_raw / normalizer
    else:
        slope_long = slope_long_raw
        slope_short = slope_short_raw
    
    # 计算加速度
    acceleration = slope_short - slope_long
    
    # 加速度的 Z-Score（用于判断加速度是否异常）
    accel_zscore = rolling_zscore(acceleration, window=long_window, min_periods=long_min_periods)
    
    # 斜率比（短期/长期，衡量趋势强度变化）
    # 避免除以零
    slope_long_safe = slope_long.replace(0, np.nan)
    slope_ratio = slope_short / slope_long_safe
    
    # 构建结果 DataFrame
    result = pd.DataFrame({
        'slope_long': slope_long,
        'slope_short': slope_short,
        'acceleration': acceleration,
        'acceleration_zscore': accel_zscore,
        'slope_ratio': slope_ratio,
    }, index=price.index)
    
    return result


def rolling_slope_volatility_adjusted(
    price: pd.Series,
    window: int = 60,
    vol_window: int = 20,
    min_periods: Optional[int] = None,
) -> pd.Series:
    """波动率调整后的滚动斜率
    
    计算价格的线性回归斜率，并除以滚动波动率进行标准化。
    这使得不同波动率环境下的趋势强度具有可比性。
    
    Parameters
    ----------
    price : pd.Series
        价格序列
    window : int, default 60
        斜率计算窗口
    vol_window : int, default 20
        波动率计算窗口
    min_periods : int, optional
        最小观测数
    
    Returns
    -------
    pd.Series
        波动率调整后的斜率
    
    Notes
    -----
    - 正值表示上涨趋势，负值表示下跌趋势
    - 绝对值越大，趋势越强
    - 相比原始斜率，该指标在高波动和低波动环境下更具可比性
    - 处理空数据：返回空 Series
    """
    if price.empty:
        return pd.Series(dtype=float)
    
    if min_periods is None:
        min_periods = max(3, window // 2)
    
    # 计算原始斜率
    slope_raw = rolling_slope(price, window=window, min_periods=min_periods, method="ols")
    
    # 计算滚动波动率
    returns = price.pct_change()
    rolling_vol = returns.rolling(vol_window, min_periods=max(5, vol_window // 2)).std()
    
    # 使用价格均值作为基准
    price_base = price.rolling(vol_window, min_periods=5).mean()
    
    # 标准化
    normalizer = price_base * rolling_vol
    normalizer = normalizer.replace(0, np.nan)
    
    return slope_raw / normalizer


# =============================================================================
# 趋势强度指数 (Task 3.5)
# =============================================================================

def trend_strength_index(
    price: pd.Series,
    windows: Optional[List[int]] = None,
    vol_window: int = 20,
) -> pd.DataFrame:
    """多周期趋势强度指数
    
    计算多个时间窗口的波动率调整斜率，并汇总为综合趋势强度指标。
    
    Parameters
    ----------
    price : pd.Series
        价格序列
    windows : list of int, optional
        斜率计算窗口列表，默认 [20, 60, 120, 252]
    vol_window : int, default 20
        波动率计算窗口
    
    Returns
    -------
    pd.DataFrame
        包含各周期斜率和综合指标：
        - slope_{window}: 各周期的波动率调整斜率
        - trend_strength: 综合趋势强度（各周期斜率的加权平均）
        - trend_consistency: 趋势一致性（各周期斜率符号一致的程度，范围 [0, 1]）
    
    Notes
    -----
    - trend_strength > 0: 整体上涨趋势
    - trend_strength < 0: 整体下跌趋势
    - trend_consistency 接近 1: 各周期趋势方向一致
    - trend_consistency 接近 0: 各周期趋势方向分歧
    - 处理空数据：返回空 DataFrame
    """
    if price.empty:
        return pd.DataFrame()
    
    if windows is None:
        windows = [20, 60, 120, 252]
    
    result = pd.DataFrame(index=price.index)
    slopes = []
    
    for w in windows:
        col_name = f'slope_{w}'
        slope = rolling_slope_volatility_adjusted(price, window=w, vol_window=vol_window)
        result[col_name] = slope
        slopes.append(slope)
    
    # 综合趋势强度：短期权重更高
    # 权重：1/sqrt(window)，短期窗口权重更大
    weights = [1 / np.sqrt(w) for w in windows]
    weight_sum = sum(weights)
    weights = [w / weight_sum for w in weights]
    
    trend_strength = sum(w * s for w, s in zip(weights, slopes))
    result['trend_strength'] = trend_strength
    
    # 趋势一致性：各周期斜率符号一致的程度
    # 计算方法：所有斜率符号相同的比例
    slope_signs = pd.concat([s.apply(np.sign) for s in slopes], axis=1)
    
    def consistency(row):
        signs = row.dropna()
        if len(signs) == 0:
            return np.nan
        # 计算最常见符号的占比
        sign_counts = signs.value_counts()
        return sign_counts.max() / len(signs)
    
    result['trend_consistency'] = slope_signs.apply(consistency, axis=1)
    
    return result
