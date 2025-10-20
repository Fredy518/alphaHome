#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hikyuu C 路径数据适配器
实现 AlphaHome 数据到 Hikyuu 的实时内存适配（不写入磁盘）
"""

import numpy as np
import pandas as pd
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import logging

try:
    from hikyuu.interactive import *
    HIKYUU_AVAILABLE = True
except ImportError:
    HIKYUU_AVAILABLE = False
    print("Warning: Hikyuu not installed, adapter will work in mock mode")

logger = logging.getLogger(__name__)


class HikyuuDataAdapter:
    """Hikyuu 内存数据适配器（C 路径）"""
    
    def __init__(self):
        """初始化适配器"""
        self.logger = logger
        self._cache = {}  # 缓存转换后的 Hikyuu 对象
        
        if not HIKYUU_AVAILABLE:
            self.logger.warning("Hikyuu 未安装，适配器将在模拟模式下工作")
    
    def create_kdata_from_dataframe(self, df: pd.DataFrame, 
                                   stock_code: str,
                                   ktype: str = "DAY") -> Any:
        """
        从 DataFrame 创建 Hikyuu KData 对象
        
        Args:
            df: 包含 OHLCV 数据的 DataFrame
                期望列：['trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']
            stock_code: 股票代码（Hikyuu 格式，如 'sz000001'）
            ktype: K线类型，默认 'DAY'
            
        Returns:
            Hikyuu KData 对象或模拟对象
        """
        if not HIKYUU_AVAILABLE:
            return self._create_mock_kdata(df, stock_code)
        
        try:
            # 确保数据按日期排序
            df = df.copy()
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.sort_values('trade_date')
            
            # 创建 KData 对象 - 使用 Stock 和 Query
            # 先获取或创建一个 Stock 对象
            from hikyuu import Stock, Query, KQuery
            
            # 创建一个虚拟的 Stock 对象
            market = stock_code[:2].upper() if len(stock_code) > 2 else "SZ"
            code = stock_code[2:] if len(stock_code) > 2 else stock_code
            
            # 创建 KData 
            kdata = KData()
            
            # 手动添加 K 线记录
            for _, row in df.iterrows():
                # 使用正确的日期时间格式
                dt = Datetime(row['trade_date'].strftime('%Y%m%d'))
                
                # 添加 K 线记录到 KData
                # 注意：KData 可能需要通过其他方式创建，这里使用替代方法
                k_record = KRecord()
                k_record.datetime = dt
                k_record.open = float(row['open'])
                k_record.high = float(row['high'])
                k_record.low = float(row['low'])
                k_record.close = float(row['close'])
                k_record.amount = float(row.get('amount', 0))
                k_record.volume = float(row.get('vol', 0))
                
                # KData 需要通过 append 方法添加记录
                kdata.append(k_record)
            
            # 缓存结果
            cache_key = f"{stock_code}_{ktype}"
            self._cache[cache_key] = kdata
            
            self.logger.info(f"创建 KData 成功: {stock_code}, {len(df)} 条记录")
            return kdata
            
        except Exception as e:
            # 回退：返回 DataFrame，由后续指标/信号计算函数直接基于 pandas 计算
            self.logger.error(f"创建 KData 失败: {e}")
            self.logger.info("回退到 DataFrame 直算模式")
            return df
    
    def _create_mock_kdata(self, df: pd.DataFrame, stock_code: str) -> Dict:
        """创建模拟 KData 对象（用于测试）"""
        return {
            'stock_code': stock_code,
            'data': df.to_dict('records'),
            'size': len(df),
            'start_date': df['trade_date'].min(),
            'end_date': df['trade_date'].max()
        }
    
    def create_stock_from_info(self, stock_info: Dict[str, Any]) -> Any:
        """
        从股票信息创建 Hikyuu Stock 对象
        
        Args:
            stock_info: 股票基本信息字典
                期望字段：ts_code, symbol, name, market, list_date
                
        Returns:
            Hikyuu Stock 对象或模拟对象
        """
        if not HIKYUU_AVAILABLE:
            return self._create_mock_stock(stock_info)
        
        try:
            # 创建股票对象
            market = stock_info['market'].upper()
            code = stock_info['symbol']
            name = stock_info['name']
            
            # 使用 Hikyuu 的股票创建方法
            stock = Stock(market, code, name)
            
            # 缓存结果
            cache_key = f"{market}{code}"
            self._cache[cache_key] = stock
            
            self.logger.info(f"创建 Stock 成功: {market}{code} - {name}")
            return stock
            
        except Exception as e:
            self.logger.error(f"创建 Stock 失败: {e}")
            raise
    
    def _create_mock_stock(self, stock_info: Dict) -> Dict:
        """创建模拟 Stock 对象（用于测试）"""
        return {
            'market': stock_info['market'],
            'code': stock_info['symbol'],
            'name': stock_info['name'],
            'list_date': stock_info.get('list_date')
        }
    
    def calculate_indicator(self, kdata: Any, 
                          indicator_name: str,
                          params: Optional[Dict] = None) -> Union[np.ndarray, List[float]]:
        """
        计算技术指标
        
        Args:
            kdata: Hikyuu KData 对象
            indicator_name: 指标名称（如 'MA', 'EMA', 'RSI', 'MACD'）
            params: 指标参数
            
        Returns:
            指标值数组
        """
        # 如果传入的是 DataFrame（直算模式）则用 pandas/numpy 计算
        if isinstance(kdata, pd.DataFrame):
            return self._calculate_indicator_from_df(kdata, indicator_name, params)
        
        if not HIKYUU_AVAILABLE:
            return self._calculate_mock_indicator(kdata, indicator_name, params)
        
        try:
            params = params or {}
            
            # 根据指标名称调用相应的 Hikyuu 指标函数
            if indicator_name.upper() == 'MA':
                n = params.get('n', 20)
                indicator = MA(CLOSE(kdata), n)
                
            elif indicator_name.upper() == 'EMA':
                n = params.get('n', 20)
                indicator = EMA(CLOSE(kdata), n)
                
            elif indicator_name.upper() == 'RSI':
                n = params.get('n', 14)
                indicator = RSI(CLOSE(kdata), n)
                
            elif indicator_name.upper() == 'MACD':
                fast_n = params.get('fast_n', 12)
                slow_n = params.get('slow_n', 26)
                signal_n = params.get('signal_n', 9)
                indicator = MACD(CLOSE(kdata), fast_n, slow_n, signal_n)
                
            elif indicator_name.upper() == 'KDJ':
                n = params.get('n', 9)
                m1 = params.get('m1', 3)
                m2 = params.get('m2', 3)
                indicator = KDJ(kdata, n, m1, m2)
                
            elif indicator_name.upper() == 'BOLL':
                n = params.get('n', 20)
                k = params.get('k', 2)
                indicator = BOLL(CLOSE(kdata), n, k)
                
            elif indicator_name.upper() == 'ATR':
                n = params.get('n', 14)
                indicator = ATR(kdata, n)
                
            elif indicator_name.upper() == 'VOL':
                indicator = VOL(kdata)
                
            else:
                raise ValueError(f"不支持的指标: {indicator_name}")
            
            # 转换为 numpy 数组
            result = np.array([indicator[i] for i in range(len(indicator))])
            
            self.logger.debug(f"计算指标 {indicator_name} 成功，结果长度: {len(result)}")
            return result
            
        except Exception as e:
            self.logger.error(f"计算指标 {indicator_name} 失败: {e}")
            # 回退到 DataFrame 直算（若可能）
            if isinstance(kdata, pd.DataFrame):
                return self._calculate_indicator_from_df(kdata, indicator_name, params)
            raise
    
    def _calculate_mock_indicator(self, kdata: Dict, 
                                 indicator_name: str, 
                                 params: Dict) -> List[float]:
        """计算模拟指标（用于测试）"""
        # 返回随机数据作为模拟指标
        size = kdata.get('size', 100)
        return np.random.randn(size).tolist()

    def _calculate_indicator_from_df(self, df: pd.DataFrame, indicator_name: str, params: Optional[Dict]) -> np.ndarray:
        """基于 DataFrame 的指标计算（直算模式）"""
        params = params or {}
        s = df['close'].astype(float).values
        
        if indicator_name.upper() == 'MA':
            n = int(params.get('n', 20))
            if n <= 1:
                return s
            ma = pd.Series(s).rolling(n, min_periods=1).mean().values
            return ma
        
        if indicator_name.upper() == 'EMA':
            n = int(params.get('n', 12))
            ema = pd.Series(s).ewm(span=n, adjust=False).mean().values
            return ema
        
        if indicator_name.upper() == 'RSI':
            n = int(params.get('n', 14))
            diff = np.diff(s, prepend=s[0])
            gain = np.where(diff > 0, diff, 0.0)
            loss = np.where(diff < 0, -diff, 0.0)
            avg_gain = pd.Series(gain).rolling(n, min_periods=1).mean()
            avg_loss = pd.Series(loss).rolling(n, min_periods=1).mean()
            rs = avg_gain / (avg_loss.replace(0, np.nan))
            rsi = 100 - (100 / (1 + rs))
            return rsi.fillna(0).values
        
        if indicator_name.upper() == 'MACD':
            fast_n = int(params.get('fast_n', 12))
            slow_n = int(params.get('slow_n', 26))
            signal_n = int(params.get('signal_n', 9))
            ema_fast = pd.Series(s).ewm(span=fast_n, adjust=False).mean()
            ema_slow = pd.Series(s).ewm(span=slow_n, adjust=False).mean()
            diff = ema_fast - ema_slow
            dea = diff.ewm(span=signal_n, adjust=False).mean()
            macd = (diff - dea) * 2
            return macd.values
        
        if indicator_name.upper() == 'BOLL':
            n = int(params.get('n', 20))
            k = float(params.get('k', 2))
            ma = pd.Series(s).rolling(n, min_periods=1).mean()
            std = pd.Series(s).rolling(n, min_periods=1).std(ddof=0)
            upper = ma + k * std
            lower = ma - k * std
            # 返回上轨，调用侧如需中轨/下轨可扩展
            return upper.values
        
        if indicator_name.upper() == 'ATR':
            n = int(params.get('n', 14))
            high = df['high'].astype(float).values
            low = df['low'].astype(float).values
            close = df['close'].astype(float).values
            prev_close = np.roll(close, 1)
            prev_close[0] = close[0]
            tr = np.maximum.reduce([
                high - low,
                np.abs(high - prev_close),
                np.abs(low - prev_close)
            ])
            atr = pd.Series(tr).rolling(n, min_periods=1).mean().values
            return atr
        
        if indicator_name.upper() == 'VOL':
            return df['vol'].astype(float).values
        
        raise ValueError(f"不支持的指标(DF): {indicator_name}")
    
    def generate_signals(self, kdata: Any,
                        signal_type: str,
                        params: Optional[Dict] = None) -> pd.DataFrame:
        """
        生成交易信号
        
        Args:
            kdata: Hikyuu KData 对象
            signal_type: 信号类型（如 'MA_CROSS', 'RSI_OVERBOUGHT', 'MACD_CROSS'）
            params: 信号参数
            
        Returns:
            包含信号的 DataFrame，列：['datetime', 'signal', 'value']
            signal: 1=买入, -1=卖出, 0=无信号
        """
        # DataFrame 直算模式
        if isinstance(kdata, pd.DataFrame):
            return self._generate_signals_from_df(kdata, signal_type, params)
        
        if not HIKYUU_AVAILABLE:
            return self._generate_mock_signals(kdata, signal_type, params)
        
        try:
            params = params or {}
            signals = []
            
            if signal_type == 'MA_CROSS':
                # 双均线交叉信号
                fast_n = params.get('fast_n', 5)
                slow_n = params.get('slow_n', 20)
                
                ma_fast = MA(CLOSE(kdata), fast_n)
                ma_slow = MA(CLOSE(kdata), slow_n)
                
                # 生成交叉信号
                cross_up = CROSS(ma_fast, ma_slow)
                cross_down = CROSS(ma_slow, ma_fast)
                
                for i in range(len(kdata)):
                    signal = 0
                    if i < len(cross_up) and cross_up[i] > 0:
                        signal = 1  # 金叉买入
                    elif i < len(cross_down) and cross_down[i] > 0:
                        signal = -1  # 死叉卖出
                    
                    signals.append({
                        'datetime': kdata[i].datetime,
                        'signal': signal,
                        'value': ma_fast[i] if i < len(ma_fast) else 0
                    })
            
            elif signal_type == 'RSI_OVERBOUGHT':
                # RSI 超买超卖信号
                n = params.get('n', 14)
                overbought = params.get('overbought', 70)
                oversold = params.get('oversold', 30)
                
                rsi = RSI(CLOSE(kdata), n)
                
                for i in range(len(kdata)):
                    signal = 0
                    if i < len(rsi):
                        if rsi[i] > overbought:
                            signal = -1  # 超买卖出
                        elif rsi[i] < oversold:
                            signal = 1  # 超卖买入
                    
                    signals.append({
                        'datetime': kdata[i].datetime,
                        'signal': signal,
                        'value': rsi[i] if i < len(rsi) else 0
                    })
            
            elif signal_type == 'MACD_CROSS':
                # MACD 交叉信号
                fast_n = params.get('fast_n', 12)
                slow_n = params.get('slow_n', 26)
                signal_n = params.get('signal_n', 9)
                
                macd = MACD(CLOSE(kdata), fast_n, slow_n, signal_n)
                
                # MACD 有三条线：DIFF, DEA, MACD柱
                for i in range(1, len(kdata)):
                    signal = 0
                    if i < len(macd):
                        # 简化：使用 MACD 柱的正负变化作为信号
                        if i > 0 and macd[i] > 0 and macd[i-1] <= 0:
                            signal = 1  # MACD 由负转正，买入
                        elif i > 0 and macd[i] < 0 and macd[i-1] >= 0:
                            signal = -1  # MACD 由正转负，卖出
                    
                    signals.append({
                        'datetime': kdata[i].datetime,
                        'signal': signal,
                        'value': macd[i] if i < len(macd) else 0
                    })
            
            else:
                raise ValueError(f"不支持的信号类型: {signal_type}")
            
            # 转换为 DataFrame
            df_signals = pd.DataFrame(signals)
            
            self.logger.info(f"生成信号 {signal_type} 成功，信号数: {len(df_signals[df_signals['signal'] != 0])}")
            return df_signals
            
        except Exception as e:
            self.logger.error(f"生成信号 {signal_type} 失败: {e}")
            # 回退到 DataFrame 直算
            if isinstance(kdata, pd.DataFrame):
                return self._generate_signals_from_df(kdata, signal_type, params)
            raise
    
    def _generate_mock_signals(self, kdata: Dict, 
                              signal_type: str, 
                              params: Dict) -> pd.DataFrame:
        """生成模拟信号（用于测试）"""
        size = kdata.get('size', 100)
        dates = pd.date_range(end=pd.Timestamp.now(), periods=size, freq='D')
        
        signals = []
        for i, date in enumerate(dates):
            # 随机生成信号
            if i % 10 == 0:
                signal = np.random.choice([-1, 0, 1])
            else:
                signal = 0
            
            signals.append({
                'datetime': date,
                'signal': signal,
                'value': np.random.randn()
            })
        
        return pd.DataFrame(signals)

    def _generate_signals_from_df(self, df: pd.DataFrame, signal_type: str, params: Optional[Dict]) -> pd.DataFrame:
        """基于 DataFrame 的信号生成（直算模式）"""
        params = params or {}
        out = []
        dates = pd.to_datetime(df['trade_date']).values
        close = df['close'].astype(float).values
        
        if signal_type == 'MA_CROSS':
            fast_n = int(params.get('fast_n', 5))
            slow_n = int(params.get('slow_n', 20))
            ma_fast = pd.Series(close).rolling(fast_n, min_periods=1).mean()
            ma_slow = pd.Series(close).rolling(slow_n, min_periods=1).mean()
            diff = ma_fast - ma_slow
            sign = np.sign(diff)
            cross = np.diff(sign, prepend=sign.iloc[0])
            for i in range(len(close)):
                sig = 0
                if cross[i] > 0:
                    sig = 1
                elif cross[i] < 0:
                    sig = -1
                out.append({'datetime': dates[i], 'signal': sig, 'value': float(ma_fast.iloc[i])})
            return pd.DataFrame(out)
        
        if signal_type == 'RSI_OVERBOUGHT':
            n = int(params.get('n', 14))
            overbought = float(params.get('overbought', 70))
            oversold = float(params.get('oversold', 30))
            diff = np.diff(close, prepend=close[0])
            gain = np.where(diff > 0, diff, 0.0)
            loss = np.where(diff < 0, -diff, 0.0)
            avg_gain = pd.Series(gain).rolling(n, min_periods=1).mean()
            avg_loss = pd.Series(loss).rolling(n, min_periods=1).mean()
            rs = avg_gain / (avg_loss.replace(0, np.nan))
            rsi = (100 - 100 / (1 + rs)).fillna(0)
            for i in range(len(close)):
                sig = 0
                if rsi.iloc[i] > overbought:
                    sig = -1
                elif rsi.iloc[i] < oversold:
                    sig = 1
                out.append({'datetime': dates[i], 'signal': sig, 'value': float(rsi.iloc[i])})
            return pd.DataFrame(out)
        
        if signal_type == 'MACD_CROSS':
            fast_n = int(params.get('fast_n', 12))
            slow_n = int(params.get('slow_n', 26))
            signal_n = int(params.get('signal_n', 9))
            ema_fast = pd.Series(close).ewm(span=fast_n, adjust=False).mean()
            ema_slow = pd.Series(close).ewm(span=slow_n, adjust=False).mean()
            diff = ema_fast - ema_slow
            dea = diff.ewm(span=signal_n, adjust=False).mean()
            macd = (diff - dea) * 2
            sign = np.sign(macd)
            cross = np.diff(sign, prepend=sign.iloc[0])
            for i in range(len(close)):
                sig = 0
                if cross[i] > 0:
                    sig = 1
                elif cross[i] < 0:
                    sig = -1
                out.append({'datetime': dates[i], 'signal': sig, 'value': float(macd.iloc[i])})
            return pd.DataFrame(out)
        
        raise ValueError(f"不支持的信号类型(DF): {signal_type}")
    
    def batch_calculate_indicators(self, df_dict: Dict[str, pd.DataFrame],
                                  indicators: List[Dict]) -> Dict[str, Dict[str, np.ndarray]]:
        """
        批量计算多个股票的多个指标
        
        Args:
            df_dict: {股票代码: DataFrame} 字典
            indicators: 指标配置列表，每个元素为 {'name': 指标名, 'params': 参数}
            
        Returns:
            {股票代码: {指标名: 指标值数组}} 嵌套字典
        """
        results = {}
        
        for stock_code, df in df_dict.items():
            # 创建 KData
            hikyuu_code = self._convert_to_hikyuu_code(stock_code)
            kdata = self.create_kdata_from_dataframe(df, hikyuu_code)
            
            # 计算所有指标
            stock_indicators = {}
            for indicator_config in indicators:
                indicator_name = indicator_config['name']
                params = indicator_config.get('params', {})
                
                try:
                    indicator_values = self.calculate_indicator(kdata, indicator_name, params)
                    stock_indicators[indicator_name] = indicator_values
                except Exception as e:
                    self.logger.warning(f"计算 {stock_code} 的 {indicator_name} 失败: {e}")
                    stock_indicators[indicator_name] = np.array([])
            
            results[stock_code] = stock_indicators
        
        self.logger.info(f"批量计算完成: {len(df_dict)} 个股票, {len(indicators)} 个指标")
        return results
    
    def _convert_to_hikyuu_code(self, ts_code: str) -> str:
        """
        将 AlphaHome 的 ts_code 转换为 Hikyuu 格式
        例如：'000001.SZ' -> 'sz000001'
        """
        if '.' in ts_code:
            code, market = ts_code.split('.')
            return f"{market.lower()}{code}"
        return ts_code.lower()
    
    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        self.logger.info("缓存已清空")


class HikyuuSignalGenerator:
    """Hikyuu 信号生成器"""
    
    def __init__(self, adapter: HikyuuDataAdapter):
        """
        初始化信号生成器
        
        Args:
            adapter: HikyuuDataAdapter 实例
        """
        self.adapter = adapter
        self.logger = logger
    
    def generate_portfolio_signals(self, 
                                  stock_data: Dict[str, pd.DataFrame],
                                  strategy: str,
                                  params: Optional[Dict] = None) -> pd.DataFrame:
        """
        为投资组合生成信号
        
        Args:
            stock_data: {股票代码: DataFrame} 字典
            strategy: 策略名称
            params: 策略参数
            
        Returns:
            包含所有股票信号的 DataFrame
        """
        all_signals = []
        
        for stock_code, df in stock_data.items():
            # 创建 KData
            hikyuu_code = self.adapter._convert_to_hikyuu_code(stock_code)
            kdata = self.adapter.create_kdata_from_dataframe(df, hikyuu_code)
            
            # 生成信号
            signals = self.adapter.generate_signals(kdata, strategy, params)
            signals['stock_code'] = stock_code
            
            all_signals.append(signals)
        
        # 合并所有信号
        portfolio_signals = pd.concat(all_signals, ignore_index=True)
        
        # 按日期排序
        portfolio_signals = portfolio_signals.sort_values(['datetime', 'stock_code'])
        
        self.logger.info(f"组合信号生成完成: {len(stock_data)} 个股票, "
                        f"{len(portfolio_signals[portfolio_signals['signal'] != 0])} 个有效信号")
        
        return portfolio_signals
    
    def filter_signals(self, signals: pd.DataFrame, 
                      filters: Optional[List[Dict]] = None) -> pd.DataFrame:
        """
        过滤信号
        
        Args:
            signals: 原始信号 DataFrame
            filters: 过滤条件列表
            
        Returns:
            过滤后的信号 DataFrame
        """
        if not filters:
            return signals
        
        filtered = signals.copy()
        
        for filter_config in filters:
            filter_type = filter_config.get('type')
            
            if filter_type == 'min_value':
                # 最小值过滤
                min_val = filter_config.get('value', 0)
                filtered = filtered[filtered['value'] >= min_val]
                
            elif filter_type == 'max_signals_per_day':
                # 每日最大信号数过滤
                max_signals = filter_config.get('value', 10)
                filtered = filtered.groupby('datetime').head(max_signals)
                
            elif filter_type == 'signal_type':
                # 信号类型过滤（只保留买入或卖出）
                signal_type = filter_config.get('value', 1)  # 1=买入, -1=卖出
                filtered = filtered[filtered['signal'] == signal_type]
        
        self.logger.info(f"信号过滤完成: {len(signals)} -> {len(filtered)}")
        return filtered
