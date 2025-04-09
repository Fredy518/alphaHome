import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timedelta
import statsmodels.api as sm
from tqdm import tqdm
from data_loader import DataLoader
import tushare as ts
from concurrent.futures import ProcessPoolExecutor
import warnings
import os
from concurrent.futures import as_completed
warnings.filterwarnings('ignore')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/industry_crowd_factor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def calculate_beta(data):
    """计算单个股票的贝塔值"""
    if len(data) <= 10:
        return np.nan
    X = sm.add_constant(data.iloc[:, 0])
    try:
        model = sm.OLS(data.iloc[:, 1], X).fit()
        return model.params[1]
    except:
        return np.nan

class IndustryCrowdFactor:
    def __init__(self):
        """初始化数据加载器和基础参数"""
        try:
            data_loader = DataLoader()  # 临时创建DataLoader对象
            
            self.lookback_days = 63
            self.rolling_days = 252
            
            self.output_dir = Path('output/industry_crowd_factor')
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            # 设置并行处理参数
            self.chunk_size = 50  # 增加chunk大小
            self.max_workers = min(16, os.cpu_count())  # 增加worker数量，但不超过CPU核心数
            
            # 预加载所有数据
            self._load_all_data(data_loader)
            
            # 删除临时对象
            del data_loader
            
            logger.info("初始化完成")
            
        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise

    def _load_all_data(self, data_loader):
        """一次性预加载所有需要的数据"""
        logger.info("开始加载数据...")
        
        try:
            # 1. 加载中证800指数数据
            logger.info("加载中证800指数数据...")
            start_date = '20050101'
            end_date = datetime.now().strftime('%Y%m%d')
            
            # 临时创建pro_api对象
            pro = ts.pro_api()
            df_index = pro.index_daily(
                ts_code='000985.CSI',
                start_date=start_date,
                end_date=end_date,
                fields='trade_date,pct_chg'
            )
            
            # 处理指数数据
            df_index['trade_date'] = pd.to_datetime(df_index['trade_date'])
            df_index['pct_chg'] = df_index['pct_chg'] / 100  # 转换为小数
            self.market_index = df_index.set_index('trade_date')['pct_chg'].sort_index()
            
            # 2. 一次性加载所有基础数据
            logger.info("加载基础数据...")
            daily_basic = data_loader.load_data('daily_basic_combined')
            industry_members = data_loader.load_data('sw_industry_members_monthly')
            
            # 3. 数据预处理
            logger.info("开始数据预处理...")
            
            # 转换日期格式
            daily_basic['trade_date'] = pd.to_datetime(daily_basic['trade_date'])
            industry_members['trade_date'] = pd.to_datetime(industry_members['trade_date'])
            
            # 筛选2005年之后的数据
            start_dt = pd.to_datetime('2005-01-01')
            daily_basic = daily_basic[daily_basic['trade_date'] >= start_dt]
            industry_members = industry_members[industry_members['trade_date'] >= start_dt]
            
            # 获取所有交易日期并排序
            self.trade_dates = np.sort(daily_basic['trade_date'].unique())
            
            # 对daily_basic数据进行预处理和排序
            daily_basic.sort_values(['trade_date', 'ts_code'], inplace=True)
            
            # 预先创建turnover和close的透视表
            logger.info("创建turnover和close的透视表...")
            self.turnover_pivot = daily_basic.pivot(
                index='trade_date',
                columns='ts_code',
                values='turnover_rate'
            )
            self.close_pivot = daily_basic.pivot(
                index='trade_date',
                columns='ts_code',
                values='close'
            )
            
            # 预处理行业成分股数据
            self.industry_members = industry_members.sort_values(['trade_date', 'ts_code'])
            
            # 计算收益率
            self.returns_pivot = self.close_pivot.pct_change()
            
            # 清理内存
            del daily_basic
            del pro
            
            logger.info("数据加载和预处理完成")
            
        except Exception as e:
            logger.error(f"数据加载失败: {str(e)}")
            import traceback
            logger.error(f"错误详情:\n{traceback.format_exc()}")
            raise

    def calculate_industry_factors_batch(self, dates):
        """批量计算行业因子"""
        try:
            # 将日期分组
            date_chunks = [dates[i:i + self.chunk_size] for i in range(0, len(dates), self.chunk_size)]
            total_chunks = len(date_chunks)
            
            all_results = []
            
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for chunk_idx, date_chunk in enumerate(date_chunks, 1):
                    chunk_start = pd.to_datetime(date_chunk[0])
                    chunk_end = pd.to_datetime(date_chunk[-1])
                    
                    # 获取该chunk需要的数据
                    chunk_turnover = self.turnover_pivot[
                        (self.turnover_pivot.index >= chunk_start - pd.Timedelta(days=self.lookback_days)) &
                        (self.turnover_pivot.index <= chunk_end)
                    ].copy()
                    
                    chunk_returns = self.returns_pivot[
                        (self.returns_pivot.index >= chunk_start - pd.Timedelta(days=self.lookback_days)) &
                        (self.returns_pivot.index <= chunk_end)
                    ].copy()
                    
                    chunk_market = self.market_index[
                        (self.market_index.index >= chunk_start - pd.Timedelta(days=self.lookback_days)) &
                        (self.market_index.index <= chunk_end)
                    ].copy()
                    
                    # 获取上个月月末日期 
                    last_month_end = (chunk_start.replace(day=1) - pd.Timedelta(days=1))
                    last_month_start = last_month_end.replace(day=1)
                    
                    # 获取不大于上个月月末的最近的成分股数据
                    chunk_members = self.industry_members[
                        (self.industry_members['trade_date'] <= last_month_end) &
                        (self.industry_members['trade_date'] >= last_month_start)
                    ].copy()
                    
                    # 提交任务到进程池
                    future = executor.submit(
                        self._process_date_chunk,
                        date_chunk,
                        chunk_turnover,
                        chunk_returns,
                        chunk_market,
                        chunk_members,
                        self.lookback_days,  # 传递lookback_days参数
                        chunk_idx,
                        total_chunks
                    )
                    futures.append(future)
                
                # 收集结果
                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="处理日期组",
                    ncols=100
                ):
                    try:
                        chunk_result = future.result()
                        if chunk_result is not None and len(chunk_result) > 0:
                            all_results.extend(chunk_result)
                    except Exception as e:
                        logger.error(f"处理日期组时出错: {str(e)}")
            
            # 合并所有结果
            if not all_results:
                logger.error("没有成功计算出任何因子数据")
                return pd.DataFrame()
            
            final_df = pd.DataFrame(all_results)
            final_df = final_df.sort_values(['trade_date', 'industry_code'])
            
            return final_df
            
        except Exception as e:
            logger.error(f"批量计算因子时出错: {str(e)}")
            return pd.DataFrame()

    def _process_date_chunk(self, dates, chunk_turnover, chunk_returns, chunk_market, chunk_members, lookback_days, chunk_idx, total_chunks):
        """处理一组日期的数据"""
        try:
            results = []
            
            # 预先获取所有有效的股票代码
            valid_stock_set = set(chunk_turnover.columns)
            
            # 预先计算每个日期的窗口范围
            date_windows = {}
            for date in dates:
                date = pd.to_datetime(date)  # 确保日期格式一致
                date_windows[date] = (
                    date - pd.Timedelta(days=lookback_days),
                    date
                )
            
            # 预先按日期对行业成分股数据进行分组
            date_grouped_members = {}
            for date in dates:
                date = pd.to_datetime(date)  # 确保日期格式一致
                members_before_date = chunk_members[chunk_members['trade_date'] <= date]
                if not members_before_date.empty:
                    # 对每个行业代码，取最新日期的数据
                    latest_date = members_before_date.groupby('l1_code')['trade_date'].transform('max')
                    latest_members = members_before_date[members_before_date['trade_date'] == latest_date]
                    date_grouped_members[date] = latest_members
            
            # 主循环处理每个日期
            for current_date in dates:
                current_date = pd.to_datetime(current_date)  # 确保日期格式一致
                if current_date not in date_grouped_members:
                    continue
                    
                current_members = date_grouped_members[current_date]
                window_start, window_end = date_windows[current_date]
                
                # 获取当前窗口的数据切片
                try:
                    window_returns = chunk_returns.loc[window_start:window_end]
                    window_turnover = chunk_turnover.loc[window_start:window_end]
                    window_market = chunk_market[window_start:window_end]
                except Exception as e:
                    logger.warning(f"获取日期 {current_date} 的窗口数据时出错: {str(e)}")
                    continue
                
                # 按行业分组处理
                for industry_code, industry_group in current_members.groupby('l1_code'):
                    try:
                        # 获取行业名称
                        industry_name = industry_group['l1_name'].iloc[0]
                        
                        # 获取该行业的所有成分股
                        industry_stocks = []
                        for stocks in industry_group['ts_code']:
                            industry_stocks.extend(stocks.split(','))
                        industry_stocks = list(set(industry_stocks))  # 去重
                        
                        # 使用预先计算的valid_stock_set进行筛选
                        valid_stocks = list(set(industry_stocks) & valid_stock_set)
                        if not valid_stocks:
                            continue
                        
                        # 使用预先获取的窗口数据
                        try:
                            industry_returns = window_returns[valid_stocks]
                            industry_turnover = window_turnover[valid_stocks]
                            
                            if industry_returns.empty or industry_turnover.empty:
                                continue
                            
                            # 计算行业整体指标
                            turnover = industry_turnover.mean().mean()  # 行业平均换手率
                            volatility = industry_returns.std().mean()  # 行业平均波动率
                            
                            # 计算行业整体贝塔
                            industry_returns_mean = industry_returns.mean(axis=1)
                            aligned_data = pd.concat([window_market, industry_returns_mean], axis=1).dropna()
                            
                            if len(aligned_data) > 10:
                                beta = calculate_beta(aligned_data)
                            else:
                                beta = np.nan
                            
                            # 添加调试日志
                            if current_date == pd.Timestamp('2025-02-27') and industry_code == '801790.SI':
                                logger.info(f"\n2025-02-27 非银金融行业数据详情：")
                                logger.info(f"成分股数量：{len(valid_stocks)}")
                                logger.info(f"成分股列表：{valid_stocks}")
                                logger.info(f"窗口期数据形状：{industry_turnover.shape}")
                                logger.info(f"每只股票的平均换手率：\n{industry_turnover.mean()}")
                                logger.info(f"行业平均换手率：{turnover}")
                            
                            if not (np.isnan(turnover) and np.isnan(volatility) and np.isnan(beta)):
                                results.append({
                                    'trade_date': current_date,
                                    'industry_code': industry_code,
                                    'industry_name': industry_name,
                                    'turnover': turnover,
                                    'volatility': volatility,
                                    'beta': beta
                                })
                                
                        except Exception as e:
                            logger.warning(f"计算行业 {industry_code} 在日期 {current_date} 的指标时出错: {str(e)}")
                            continue
                            
                    except Exception as e:
                        logger.warning(f"处理行业 {industry_code} 时出错: {str(e)}")
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"处理日期组 {chunk_idx}/{total_chunks} 时出错: {str(e)}")
            return None

    def calculate_crowd_factor(self, start_date: str, end_date: str):
        """计算整个时间段的拥挤度因子"""
        try:
            logger.info(f"开始计算从 {start_date} 到 {end_date} 的拥挤度因子...")
            
            # 转换日期格式
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            # 获取交易日历并排序
            dates = self.trade_dates[
                (self.trade_dates >= start_dt) &
                (self.trade_dates <= end_dt)
            ]
            
            if len(dates) == 0:
                logger.error("未找到指定时间范围内的交易日数据")
                return None
                
            logger.info(f"找到 {len(dates)} 个交易日")
            
            # 批量计算因子
            final_df = self.calculate_industry_factors_batch(dates)
            
            if final_df.empty:
                logger.warning("没有计算出任何有效的因子数据")
                return None
            
            # 保存原始数据（不包含标准化值）
            raw_file = self.output_dir / 'crowd_factor_raw.csv'
            final_df.to_csv(raw_file, index=False)
            logger.info(f"原始因子数据已保存至: {raw_file}")
            
            # 计算最终的拥挤度因子
            pivot_dfs = []
            normalized_dfs = []  # 存储标准化后的数据
            
            # 先创建所有透视表
            for col in ['turnover', 'volatility', 'beta']:
                try:
                    # 创建透视表
                    pivot_df = pd.pivot_table(
                        final_df,
                        values=col,
                        index='trade_date',
                        columns='industry_code',
                        aggfunc='first'
                    )
                    
                    # 确保日期索引是有序的
                    pivot_df = pivot_df.sort_index()
                    
                    # 创建一个新的DataFrame来存储标准化结果
                    normalized_df = pd.DataFrame(index=pivot_df.index, columns=pivot_df.columns)
                    
                    # 对每一列进行滚动标准化
                    for column in pivot_df.columns:
                        series = pivot_df[column].astype(float)  # 确保数据类型为float
                        rolling = series.rolling(
                            window=self.rolling_days,
                            min_periods=int(self.rolling_days/2)
                        )
                        
                        mean = rolling.mean()
                        std = rolling.std()
                        
                        # 处理标准差为0的情况
                        std = std.replace(0, np.nan)
                        
                        # 计算标准化值
                        normalized_df[column] = (series - mean) / std
                    
                    pivot_dfs.append(normalized_df)
                    
                    # 将标准化结果转换为长格式
                    norm_values = normalized_df.reset_index().melt(
                        id_vars=['trade_date'],
                        var_name='industry_code',
                        value_name=f'{col}_norm'
                    )
                    
                    normalized_dfs.append(norm_values)
                    
                except Exception as e:
                    logger.error(f"处理 {col} 时出错: {str(e)}")
                    import traceback
                    logger.error(f"错误详情:\n{traceback.format_exc()}")
                    return None
            
            if len(pivot_dfs) != 3:
                logger.error("未能成功计算所有标准化指标")
                return None
            
            # 合并所有标准化结果
            final_df_with_norms = final_df.copy()
            
            # 逐个合并标准化结果
            for norm_df in normalized_dfs:
                final_df_with_norms = pd.merge(
                    final_df_with_norms,
                    norm_df,
                    on=['trade_date', 'industry_code'],
                    how='left'
                )
            
            # 计算综合拥挤度因子
            final_df_with_norms['crowd_factor'] = final_df_with_norms[[
                'turnover_norm', 'volatility_norm', 'beta_norm'
            ]].mean(axis=1)
            
            # 删除前252天的数据（一年交易日）
            valid_dates = final_df_with_norms['trade_date'].sort_values().unique()[252:]
            final_df_with_norms = final_df_with_norms[
                final_df_with_norms['trade_date'].isin(valid_dates)
            ]
            
            # 删除任何包含空值的行
            final_df_with_norms = final_df_with_norms.dropna()
            
            # 添加计算参数
            final_df_with_norms['lookback_days'] = self.lookback_days
            final_df_with_norms['rolling_days'] = self.rolling_days
            final_df_with_norms['update_time'] = pd.Timestamp.now()
            
            # 调整列顺序
            columns_order = [
                'trade_date',           # 交易日期
                'industry_code',        # 行业代码
                'industry_name',        # 行业名称
                'crowd_factor',         # 综合拥挤度因子
                'turnover',             # 换手率原始值
                'turnover_norm',        # 换手率标准化值
                'volatility',           # 波动率原始值
                'volatility_norm',      # 波动率标准化值
                'beta',                 # 贝塔原始值
                'beta_norm',            # 贝塔标准化值
                'lookback_days',        # 回看天数
                'rolling_days',         # 滚动窗口天数
                'update_time'           # 更新时间
            ]
            final_df_with_norms = final_df_with_norms[columns_order]
            
            # 保存到数据库
            try:
                data_loader = DataLoader()
                data_loader.save_to_db(
                    final_df_with_norms,
                    'industry_crowd_factor',
                    if_exists='replace'
                )
                logger.info(f"因子数据已成功保存到数据库，共 {len(final_df_with_norms)} 条记录")
                logger.info(f"数据字段: {', '.join(columns_order)}")
                
            except Exception as e:
                logger.error(f"保存数据到数据库时出错: {str(e)}")
                import traceback
                logger.error(f"错误详情:\n{traceback.format_exc()}")
            
            logger.info(f"拥挤度因子计算完成，共处理 {len(dates)} 个交易日")
            
            return final_df_with_norms
            
        except Exception as e:
            logger.error(f"计算拥挤度因子失败: {str(e)}")
            import traceback
            logger.error(f"错误详情:\n{traceback.format_exc()}")
            return None

def main():
    """主函数"""
    try:
        # 修改开始时间为2005年
        START_DATE = '2005-01-01'
        END_DATE = '2025-03-27'
        
        logger.info(f"开始运行，计算时间范围: {START_DATE} 至 {END_DATE}")
        
        # 初始化因子计算器
        calculator = IndustryCrowdFactor()
        
        # 计算因子
        crowd_factor = calculator.calculate_crowd_factor(START_DATE, END_DATE)
        
        if crowd_factor is not None:
            logger.info("因子计算成功完成")
            logger.info(f"结果已保存在 {calculator.output_dir} 目录下")
        else:
            logger.error("因子计算失败")
        
    except Exception as e:
        logger.error(f"运行失败: {str(e)}")
        import traceback
        logger.error(f"错误详情:\n{traceback.format_exc()}")

if __name__ == "__main__":
    main() 