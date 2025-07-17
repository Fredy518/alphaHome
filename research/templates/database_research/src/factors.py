"""
因子计算函数库

包含各种技术因子和特征的计算逻辑
"""

import pandas as pd
import numpy as np
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


def calculate_moving_averages(df: pd.DataFrame, windows: List[int] = [5, 10, 20, 60]) -> pd.DataFrame:
    """
    计算移动平均线
    
    Args:
        df: 包含价格数据的DataFrame
        windows: 移动平均窗口列表
        
    Returns:
        包含移动平均线的DataFrame
    """
    result = df[['ts_code', 'trade_date']].copy()
    
    for window in windows:
        # 简单移动平均
        result[f'ma_{window}'] = df['close'].rolling(window=window).mean()
        
        # 指数移动平均
        result[f'ema_{window}'] = df['close'].ewm(span=window, adjust=False).mean()
        
        # 价格与MA的比值
        result[f'close_ma_{window}_ratio'] = df['close'] / result[f'ma_{window}']
        
        # MA斜率（变化率）
        result[f'ma_{window}_slope'] = result[f'ma_{window}'].pct_change()
    
    # 计算MA金叉死叉信号
    if 5 in windows and 10 in windows:
        result['ma_5_10_cross'] = (result['ma_5'] > result['ma_10']).astype(int)
        result['ma_5_10_cross_signal'] = result['ma_5_10_cross'].diff()
    
    if 10 in windows and 20 in windows:
        result['ma_10_20_cross'] = (result['ma_10'] > result['ma_20']).astype(int)
        result['ma_10_20_cross_signal'] = result['ma_10_20_cross'].diff()
    
    return result


def calculate_volume_features(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    计算成交量相关特征
    
    Args:
        df: 包含成交量数据的DataFrame
        window: 计算窗口大小
        
    Returns:
        包含成交量特征的DataFrame
    """
    result = df[['ts_code', 'trade_date']].copy()
    
    # 成交量移动平均
    result[f'vol_ma_{window}'] = df['vol'].rolling(window=window).mean()
    
    # 成交量比率
    result['vol_ratio'] = df['vol'] / result[f'vol_ma_{window}']
    
    # 成交额
    if 'amount' in df.columns:
        result[f'amount_ma_{window}'] = df['amount'].rolling(window=window).mean()
        result['amount_ratio'] = df['amount'] / result[f'amount_ma_{window}']
    
    # 量价关系
    result['price_volume_corr'] = df['close'].rolling(window=window).corr(df['vol'])
    
    # 成交量标准差
    result[f'vol_std_{window}'] = df['vol'].rolling(window=window).std()
    
    # 异常成交量标记（超过2倍标准差）
    vol_zscore = (df['vol'] - result[f'vol_ma_{window}']) / result[f'vol_std_{window}']
    result['high_volume_flag'] = (vol_zscore > 2).astype(int)
    result['low_volume_flag'] = (vol_zscore < -2).astype(int)
    
    return result


def calculate_price_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算价格相关特征
    
    Args:
        df: 包含价格数据的DataFrame
        
    Returns:
        包含价格特征的DataFrame
    """
    result = df[['ts_code', 'trade_date']].copy()
    
    # 价格区间特征
    result['high_low_ratio'] = df['high'] / df['low']
    result['close_open_ratio'] = df['close'] / df['open']
    
    # 振幅
    result['amplitude'] = (df['high'] - df['low']) / df['pre_close'] * 100
    
    # K线实体大小
    result['body_size'] = abs(df['close'] - df['open']) / df['open'] * 100
    
    # 上下影线
    result['upper_shadow'] = (df['high'] - df[['close', 'open']].max(axis=1)) / df['open'] * 100
    result['lower_shadow'] = (df[['close', 'open']].min(axis=1) - df['low']) / df['open'] * 100
    
    # 价格位置（当日收盘价在日内区间的位置）
    result['price_position'] = (df['close'] - df['low']) / (df['high'] - df['low'])
    
    # 连续涨跌天数
    result['up_days'] = count_consecutive_days(df['pct_chg'] > 0)
    result['down_days'] = count_consecutive_days(df['pct_chg'] < 0)
    
    # 历史最高最低
    result['rolling_high_20'] = df['high'].rolling(window=20).max()
    result['rolling_low_20'] = df['low'].rolling(window=20).min()
    result['close_to_high_20'] = df['close'] / result['rolling_high_20']
    result['close_to_low_20'] = df['close'] / result['rolling_low_20']
    
    return result


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算技术指标
    
    Args:
        df: 包含OHLCV数据的DataFrame
        
    Returns:
        包含技术指标的DataFrame
    """
    result = df[['ts_code', 'trade_date']].copy()
    
    # RSI (相对强弱指标)
    result['rsi_14'] = calculate_rsi(df['close'], period=14)
    
    # MACD
    macd_result = calculate_macd(df['close'])
    result = pd.concat([result, macd_result], axis=1)
    
    # 布林带
    bb_result = calculate_bollinger_bands(df['close'], window=20, num_std=2)
    result = pd.concat([result, bb_result], axis=1)
    
    # ATR (平均真实波幅)
    result['atr_14'] = calculate_atr(df[['high', 'low', 'close']], period=14)
    
    # 威廉指标
    result['williams_r'] = calculate_williams_r(df[['high', 'low', 'close']], period=14)
    
    # 随机指标
    stoch_result = calculate_stochastic(df[['high', 'low', 'close']], period=14)
    result = pd.concat([result, stoch_result], axis=1)
    
    return result


def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """计算RSI指标"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_macd(prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """计算MACD指标"""
    exp1 = prices.ewm(span=fast, adjust=False).mean()
    exp2 = prices.ewm(span=slow, adjust=False).mean()
    
    macd = exp1 - exp2
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    macd_hist = macd - macd_signal
    
    return pd.DataFrame({
        'macd': macd,
        'macd_signal': macd_signal,
        'macd_hist': macd_hist
    })


def calculate_bollinger_bands(prices: pd.Series, window: int = 20, num_std: float = 2) -> pd.DataFrame:
    """计算布林带"""
    middle_band = prices.rolling(window=window).mean()
    std = prices.rolling(window=window).std()
    
    upper_band = middle_band + (std * num_std)
    lower_band = middle_band - (std * num_std)
    
    return pd.DataFrame({
        'bb_upper': upper_band,
        'bb_middle': middle_band,
        'bb_lower': lower_band,
        'bb_width': upper_band - lower_band,
        'bb_position': (prices - lower_band) / (upper_band - lower_band)
    })


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算ATR (平均真实波幅)"""
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    
    return atr


def calculate_williams_r(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算威廉指标"""
    highest_high = df['high'].rolling(window=period).max()
    lowest_low = df['low'].rolling(window=period).min()
    
    williams_r = -100 * (highest_high - df['close']) / (highest_high - lowest_low)
    
    return williams_r


def calculate_stochastic(df: pd.DataFrame, period: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> pd.DataFrame:
    """计算随机指标"""
    lowest_low = df['low'].rolling(window=period).min()
    highest_high = df['high'].rolling(window=period).max()
    
    k_percent = 100 * (df['close'] - lowest_low) / (highest_high - lowest_low)
    k_percent = k_percent.rolling(window=smooth_k).mean()
    
    d_percent = k_percent.rolling(window=smooth_d).mean()
    
    return pd.DataFrame({
        'stoch_k': k_percent,
        'stoch_d': d_percent,
        'stoch_k_d_diff': k_percent - d_percent
    })


def count_consecutive_days(condition: pd.Series) -> pd.Series:
    """
    计算连续满足条件的天数
    
    Args:
        condition: 布尔Series，表示每天是否满足条件
        
    Returns:
        连续天数Series
    """
    # 创建分组标记
    group = (condition != condition.shift()).cumsum()
    
    # 计算每组的累计和
    consecutive = condition.groupby(group).cumsum()
    
    return consecutive


def calculate_custom_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算自定义因子
    
    这里可以添加您自己的因子计算逻辑
    """
    result = df[['ts_code', 'trade_date']].copy()
    
    # 示例：动量因子
    result['momentum_5'] = df['close'].pct_change(5)
    result['momentum_10'] = df['close'].pct_change(10)
    result['momentum_20'] = df['close'].pct_change(20)
    
    # 示例：波动率因子
    result['volatility_20'] = df['pct_chg'].rolling(window=20).std() * np.sqrt(252)
    
    # 示例：换手率因子（如果有流通股本数据）
    # result['turnover_rate'] = df['vol'] / df['float_share'] * 100
    
    return result
