#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票日线数据质量检查脚本

检查stock_daily表中的数据质量，主要关注点：
1. 检查1991年以来所有交易日是否都有数据
2. 检查每个交易日的股票数量是否符合预期
3. 识别可能的数据缺失或异常
"""

import os
import sys
import asyncio
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_module.db_manager import DBManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('check_stock_daily_quality')


async def get_all_trading_dates():
    """从tushare直接获取1991年以来所有交易日"""
    from data_module.sources.tushare.api import TushareAPI
    
    # 创建 Tushare API 实例
    api_token = os.environ.get('TUSHARE_TOKEN')
    if not api_token:
        raise ValueError("未提供Tushare API令牌，请通过参数传入或设置TUSHARE_TOKEN环境变量")
    
    api = TushareAPI(api_token)
    
    # 设置日期范围参数（从1991年开始到现在）
    start_date = '19910101'  # 中国股市开始时间
    end_date = datetime.now().strftime('%Y%m%d')  # 当前日期
    
    # 调用 trade_cal 接口
    logger.info(f"从 Tushare 获取交易日历数据 ({start_date} 至 {end_date})")
    
    # 准备调用参数
    params = {
        'exchange': 'SSE',  # 上交所
        'start_date': start_date,
        'end_date': end_date,
        'is_open': '1'  # 只获取交易日
    }
    
    # 异步调用 API
    df = await api.query('trade_cal', params=params)
    
    if df is None or df.empty:
        logger.warning("未获取到交易日历数据")
        return []
    
    # 转换为日期列表
    trading_dates = df['cal_date'].tolist()
    trading_dates = [pd.to_datetime(date).date() for date in trading_dates]
    trading_dates.sort()  # 确保按日期排序
    
    logger.info(f"获取到 {len(trading_dates)} 个交易日")
    return trading_dates


async def check_daily_data_quality():
    """检查股票日线数据质量"""
    # 创建数据库连接
    connection_string = os.environ.get('DATABASE_URL', 'postgresql://postgres:wuhao123@localhost:5432/tusharedb')
    db_manager = DBManager(connection_string)
    await db_manager.connect()
    
    try:
        # 获取所有交易日
        trading_dates = await get_all_trading_dates()
        
        # 查询每个交易日的股票数量
        results = []
        for date in tqdm(trading_dates, desc="检查每日数据"):
            date_str = date.strftime('%Y-%m-%d')  # 转换为字符串格式
            query = f"""
            SELECT 
                '{date_str}' as trade_date,
                COUNT(*) as stock_count
            FROM stock_daily
            WHERE trade_date = '{date_str}'
            """
            result = await db_manager.fetch_one(query)
            results.append({
                'trade_date': date_str,
                'stock_count': result['stock_count']
            })
        
        # 转换为DataFrame进行分析
        df = pd.DataFrame(results)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date')
        
        # 计算基本统计信息
        total_days = len(df)
        days_with_data = len(df[df['stock_count'] > 0])
        days_with_no_data = len(df[df['stock_count'] == 0])
        
        # 计算每年的平均股票数量
        df['year'] = df['trade_date'].dt.year
        yearly_avg = df.groupby('year')['stock_count'].mean().round().astype(int)
        
        # 输出统计结果
        logger.info(f"总交易日数: {total_days}")
        logger.info(f"有数据的交易日: {days_with_data} ({days_with_data/total_days*100:.2f}%)")
        logger.info(f"无数据的交易日: {days_with_no_data} ({days_with_no_data/total_days*100:.2f}%)")
        
        # 输出每年的平均股票数量
        logger.info("\n每年平均股票数量:")
        for year, avg_count in yearly_avg.items():
            logger.info(f"{year}: {avg_count}")
        
        # 检查是否有异常的交易日（股票数量明显少于平均值）
        # 计算每年的标准差
        yearly_std = df.groupby('year')['stock_count'].std()
        
        # 合并年度均值和标准差
        yearly_stats = pd.DataFrame({
            'mean': yearly_avg,
            'std': yearly_std
        })
        
        # 为每行添加对应年份的均值和标准差
        df = df.merge(yearly_stats, left_on='year', right_index=True)
        
        # 计算z-score (标准分数)
        df['z_score'] = (df['stock_count'] - df['mean']) / df['std'].replace(0, 1)  # 避免除以零
        
        # 找出异常交易日 (z-score < -2，即股票数量比平均值少2个标准差)
        anomalies = df[df['z_score'] < -2].sort_values('trade_date')
        
        if len(anomalies) > 0:
            logger.info("\n发现以下异常交易日 (股票数量明显少于平均值):")
            for _, row in anomalies.iterrows():
                logger.info(f"日期: {row['trade_date'].strftime('%Y-%m-%d')}, 股票数量: {row['stock_count']}, 年均值: {row['mean']}, Z分数: {row['z_score']:.2f}")
        else:
            logger.info("\n未发现异常交易日")
        
        # 返回检查结果
        return {
            'total_days': total_days,
            'days_with_data': days_with_data,
            'days_with_no_data': days_with_no_data,
            'yearly_avg': yearly_avg.to_dict(),
            'anomalies': anomalies[['trade_date', 'stock_count', 'mean', 'z_score']].to_dict('records') if len(anomalies) > 0 else []
        }
        
    finally:
        await db_manager.close()


async def main():
    start_time = datetime.now()
    logger.info(f"开始检查股票日线数据质量...")
    
    try:
        result = await check_daily_data_quality()
        
        # 输出总结
        logger.info("\n数据质量检查总结:")
        coverage = result['days_with_data'] / result['total_days'] * 100
        logger.info(f"数据覆盖率: {coverage:.2f}%")
        
        if result['days_with_no_data'] > 0:
            logger.info(f"警告: 有 {result['days_with_no_data']} 个交易日没有数据")
        
        if len(result['anomalies']) > 0:
            logger.info(f"警告: 发现 {len(result['anomalies'])} 个异常交易日")
        
        # 计算执行时间
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"检查完成，耗时: {duration:.2f} 秒")
        
    except Exception as e:
        logger.error(f"检查失败: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
