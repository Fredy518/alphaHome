#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
市场技术特征处理任务

基于 tushare.stock_factor_pro 表，计算常用横截面技术特征，
包括动量、波动、成交活跃度、价量背离等，供择时/选股共用。

Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ..base_task import ProcessorTaskBase
from ....common.task_system import task_register
from ...operations.transforms import rolling_zscore, rolling_percentile


@task_register()
class MarketTechnicalTask(ProcessorTaskBase):
    """市场技术特征处理任务
    
    计算全市场个股的横截面技术特征聚合，包括：
    
    1. 动量特征 (Requirements 3.1):
       - 5/10/20/60日动量分布（中位数、分位数）
       - 动量强度（正动量股票比例）
       - 动量分化（动量标准差）
    
    2. 波动特征 (Requirements 3.2):
       - 20/60日实现波动率分布
       - 波动率变化（短期/长期比）
       - 高波动股票比例
    
    3. 成交活跃度 (Requirements 3.3):
       - 量比分布
       - 放量/缩量股票比例
       - 成交额集中度变化
    
    4. 价量背离 (Requirements 3.4):
       - 价涨量缩/价跌量增比例
       - 量价相关性
    """
    
    name = "market_technical"
    table_name = "processor_market_technical"
    description = "计算横截面市场技术特征"
    source_tables = ["tushare_stock_factor_pro"]
    date_column = "trade_date"
    
    # 数据血缘追踪
    primary_keys = ["trade_date"]
    
    def __init__(self, db_connection, config: Optional[Dict[str, Any]] = None):
        """初始化任务
        
        Args:
            db_connection: 数据库连接实例
            config: 配置字典，可能包含:
                - source_table: 源表名 (默认: 'tushare.stock_factor_pro')
                - result_table: 结果表名 (默认: 'processor_market_technical')
                - zscore_window: Z-Score 滚动窗口 (默认: 252)
                - percentile_window: 百分位滚动窗口 (默认: 500)
        """
        super().__init__(db_connection=db_connection)
        
        resolved_config = config or {}
        self.source_table = resolved_config.get("source_table", "tushare.stock_factor_pro")
        self.result_table = resolved_config.get("result_table", "processor_market_technical")
        self.zscore_window = resolved_config.get("zscore_window", 252)
        self.percentile_window = resolved_config.get("percentile_window", 500)
        
        # 更新 table_name 以支持配置覆盖
        self.table_name = self.result_table
        # 同步 source_tables 以便数据血缘和引擎注册保持一致
        self.source_tables = [self.source_table]


    async def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """获取横截面技术特征数据
        
        通过 SQL 查询计算横截面统计，包括动量、波动率、成交量等特征。
        
        Args:
            **kwargs: 可选参数
                - start_date: 起始日期 (格式: YYYYMMDD)
                - end_date: 结束日期 (格式: YYYYMMDD)
        
        Returns:
            包含横截面统计的 DataFrame
        
        Requirements: 3.1, 3.2, 3.3, 3.4
        """
        start_date = kwargs.get("start_date", "20100101")
        end_date = kwargs.get("end_date", "20991231")
        source_table = self.source_table
        
        self.logger.info(f"从 {source_table} 获取数据，日期范围: {start_date} - {end_date}")
        
        # SQL 查询计算横截面统计
        # 使用 CTE 分层计算，避免 PostgreSQL 别名引用问题
        query = f"""
        WITH raw_data AS (
            -- 第一层：获取原始字段
            SELECT 
                trade_date,
                ts_code,
                close_hfq,
                pct_chg,
                amount,
                volume,
                turnover_rate_f
            FROM {source_table}
            WHERE trade_date >= '{start_date}' AND trade_date <= '{end_date}'
        ),
        base AS (
            -- 第二层：计算窗口函数
            SELECT 
                trade_date,
                ts_code,
                close_hfq,
                pct_chg,
                amount,
                volume,
                turnover_rate_f,
                -- 动量：使用后复权价格计算
                (close_hfq / NULLIF(LAG(close_hfq, 5) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1) * 100 as mom_5d,
                (close_hfq / NULLIF(LAG(close_hfq, 10) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1) * 100 as mom_10d,
                (close_hfq / NULLIF(LAG(close_hfq, 20) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1) * 100 as mom_20d,
                (close_hfq / NULLIF(LAG(close_hfq, 60) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) - 1) * 100 as mom_60d,
                -- 成交量变化
                volume / NULLIF(AVG(volume) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), 0) as vol_ratio_5d,
                volume / NULLIF(AVG(volume) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING), 0) as vol_ratio_20d,
                -- 波动率（20日）
                STDDEV(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) as vol_20d,
                -- 波动率（60日）
                STDDEV(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) * SQRT(252) as vol_60d
            FROM raw_data
        )
        SELECT 
            trade_date,
            COUNT(*) as total_count,
            
            -- 动量分布 (Requirements 3.1)
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_5d) FILTER (WHERE mom_5d IS NOT NULL) as mom_5d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_10d) FILTER (WHERE mom_10d IS NOT NULL) as mom_10d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_20d) FILTER (WHERE mom_20d IS NOT NULL) as mom_20d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mom_60d) FILTER (WHERE mom_60d IS NOT NULL) as mom_60d_median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY mom_20d) FILTER (WHERE mom_20d IS NOT NULL) as mom_20d_q25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY mom_20d) FILTER (WHERE mom_20d IS NOT NULL) as mom_20d_q75,
            STDDEV(mom_20d) FILTER (WHERE mom_20d IS NOT NULL) as mom_20d_std,
            
            -- 动量强度（正动量比例）
            SUM(CASE WHEN mom_5d > 0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE mom_5d IS NOT NULL), 0) as mom_5d_pos_ratio,
            SUM(CASE WHEN mom_20d > 0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE mom_20d IS NOT NULL), 0) as mom_20d_pos_ratio,
            SUM(CASE WHEN mom_60d > 0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE mom_60d IS NOT NULL), 0) as mom_60d_pos_ratio,
            
            -- 强动量股票比例（20日涨幅>10%）
            SUM(CASE WHEN mom_20d > 10 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE mom_20d IS NOT NULL), 0) as strong_mom_ratio,
            -- 弱动量股票比例（20日跌幅>10%）
            SUM(CASE WHEN mom_20d < -10 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE mom_20d IS NOT NULL), 0) as weak_mom_ratio,
            
            -- 波动率分布 (Requirements 3.2)
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_20d) FILTER (WHERE vol_20d IS NOT NULL AND vol_20d > 0) as vol_20d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_60d) FILTER (WHERE vol_60d IS NOT NULL AND vol_60d > 0) as vol_60d_median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY vol_20d) FILTER (WHERE vol_20d IS NOT NULL AND vol_20d > 0) as vol_20d_q75,
            AVG(vol_20d) FILTER (WHERE vol_20d IS NOT NULL AND vol_20d > 0) as vol_20d_mean,
            
            -- 高波动股票比例（年化波动>50%）
            SUM(CASE WHEN vol_20d > 50 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE vol_20d IS NOT NULL), 0) as high_vol_ratio,
            -- 低波动股票比例（年化波动<20%）
            SUM(CASE WHEN vol_20d < 20 AND vol_20d > 0 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE vol_20d IS NOT NULL), 0) as low_vol_ratio,
            
            -- 量比分布 (Requirements 3.3)
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_ratio_5d) FILTER (WHERE vol_ratio_5d IS NOT NULL AND vol_ratio_5d > 0) as vol_ratio_5d_median,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_ratio_20d) FILTER (WHERE vol_ratio_20d IS NOT NULL AND vol_ratio_20d > 0) as vol_ratio_20d_median,
            
            -- 放量/缩量股票比例
            SUM(CASE WHEN vol_ratio_5d > 1.5 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) as vol_expand_ratio,
            SUM(CASE WHEN vol_ratio_5d < 0.7 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) as vol_shrink_ratio,
            
            -- 价量背离 (Requirements 3.4)
            -- 价涨量缩（pct_chg>0 且 vol_ratio<0.8）
            SUM(CASE WHEN pct_chg > 0 AND vol_ratio_5d < 0.8 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) as price_up_vol_down_ratio,
            -- 价跌量增（pct_chg<0 且 vol_ratio>1.2）
            SUM(CASE WHEN pct_chg < 0 AND vol_ratio_5d > 1.2 THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) as price_down_vol_up_ratio,
            
            -- 量价同向比例（健康市场特征）
            SUM(CASE WHEN (pct_chg > 0 AND vol_ratio_5d > 1) OR (pct_chg < 0 AND vol_ratio_5d < 1) THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*) FILTER (WHERE vol_ratio_5d IS NOT NULL), 0) as vol_price_aligned_ratio
            
        FROM base
        GROUP BY trade_date
        ORDER BY trade_date
        """
        
        try:
            rows = await self.db.fetch(query)
            
            if not rows:
                self.logger.warning("未获取到横截面技术特征数据")
                return pd.DataFrame()
            
            df = pd.DataFrame([dict(row) for row in rows])
            
            if df.empty:
                return pd.DataFrame()
            
            # 转换日期类型
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.set_index("trade_date")
            
            # 转换数值类型
            df = df.apply(pd.to_numeric, errors="coerce")
            
            self.logger.info(f"成功获取 {len(df)} 行横截面技术特征数据")
            return df
            
        except Exception as e:
            self.logger.error(f"获取横截面技术特征数据失败: {e}", exc_info=True)
            raise


    async def process_data(
        self, data: pd.DataFrame, stop_event=None, **kwargs
    ) -> Optional[pd.DataFrame]:
        """计算衍生特征
        
        基于横截面统计数据计算衍生特征，包括：
        - 动量、波动率、成交量特征的组合指标
        - 应用 rolling_zscore (252天) 标准化 (Requirements 3.5)
        - 应用 rolling_percentile (500天) 计算历史分位 (Requirements 3.6)
        
        Args:
            data: fetch_data 返回的横截面统计数据
            stop_event: 停止事件（可选）
            **kwargs: 额外参数
        
        Returns:
            包含衍生特征的 DataFrame
        
        Requirements: 3.5, 3.6
        """
        if data is None or data.empty:
            self.logger.warning("输入数据为空，跳过衍生特征计算")
            return pd.DataFrame()
        
        self.logger.info(f"开始计算衍生特征，输入数据行数: {len(data)}")
        
        features = pd.DataFrame(index=data.index)
        
        # === 动量特征 (Requirements 3.1) ===
        features["Mom_5D_Median"] = data["mom_5d_median"]
        features["Mom_10D_Median"] = data["mom_10d_median"]
        features["Mom_20D_Median"] = data["mom_20d_median"]
        features["Mom_60D_Median"] = data["mom_60d_median"]
        features["Mom_20D_Spread"] = data["mom_20d_q75"] - data["mom_20d_q25"]  # IQR
        features["Mom_20D_Dispersion"] = data["mom_20d_std"]  # 动量分化程度
        
        # 动量强度
        features["Mom_5D_Pos_Ratio"] = data["mom_5d_pos_ratio"]
        features["Mom_20D_Pos_Ratio"] = data["mom_20d_pos_ratio"]
        features["Mom_60D_Pos_Ratio"] = data["mom_60d_pos_ratio"]
        features["Strong_Mom_Ratio"] = data["strong_mom_ratio"]
        features["Weak_Mom_Ratio"] = data["weak_mom_ratio"]
        features["Mom_Strength_Diff"] = data["strong_mom_ratio"] - data["weak_mom_ratio"]
        
        # === 波动特征 (Requirements 3.2) ===
        features["Vol_20D_Median"] = data["vol_20d_median"]
        features["Vol_60D_Median"] = data["vol_60d_median"]
        features["Vol_20D_Q75"] = data["vol_20d_q75"]
        features["Vol_20D_Mean"] = data["vol_20d_mean"]
        features["High_Vol_Ratio"] = data["high_vol_ratio"]
        features["Low_Vol_Ratio"] = data["low_vol_ratio"]
        
        # 波动率变化（短期/长期）
        features["Vol_Ratio_20_60"] = data["vol_20d_median"] / data["vol_60d_median"].replace(0, np.nan)
        
        # === 成交活跃度 (Requirements 3.3) ===
        features["Vol_Ratio_5D_Median"] = data["vol_ratio_5d_median"]
        features["Vol_Ratio_20D_Median"] = data["vol_ratio_20d_median"]
        features["Vol_Expand_Ratio"] = data["vol_expand_ratio"]
        features["Vol_Shrink_Ratio"] = data["vol_shrink_ratio"]
        features["Vol_Activity_Diff"] = data["vol_expand_ratio"] - data["vol_shrink_ratio"]
        
        # === 价量背离 (Requirements 3.4) ===
        features["Price_Up_Vol_Down_Ratio"] = data["price_up_vol_down_ratio"]
        features["Price_Down_Vol_Up_Ratio"] = data["price_down_vol_up_ratio"]
        features["Vol_Price_Aligned_Ratio"] = data["vol_price_aligned_ratio"]
        # 背离强度（背离比例之和，越高越不健康）
        features["Divergence_Intensity"] = (
            data["price_up_vol_down_ratio"] + data["price_down_vol_up_ratio"]
        )
        
        # === 标准化 (Requirements 3.5) ===
        # 应用 rolling_zscore (252天窗口)
        zscore_cols = [
            "Mom_20D_Median", "Mom_20D_Pos_Ratio", "Mom_Strength_Diff",
            "Vol_20D_Median", "Vol_Ratio_20_60",
            "Vol_Activity_Diff", "Divergence_Intensity"
        ]
        for col in zscore_cols:
            if col in features.columns:
                features[f"{col}_ZScore"] = rolling_zscore(
                    features[col], window=self.zscore_window
                )
        
        # === 百分位计算 (Requirements 3.6) ===
        # 应用 rolling_percentile (500天窗口)
        pctl_cols = [
            "Mom_20D_Median", "Vol_20D_Median", "Vol_Price_Aligned_Ratio"
        ]
        for col in pctl_cols:
            if col in features.columns:
                features[f"{col}_Pctl"] = rolling_percentile(
                    features[col], window=self.percentile_window
                )
        
        # === 变化率 ===
        features["Mom_20D_Median_Chg5D"] = features["Mom_20D_Median"].diff(5)
        features["Vol_20D_Median_Chg5D"] = features["Vol_20D_Median"].diff(5)
        
        self.logger.info(f"衍生特征计算完成，输出列数: {len(features.columns)}")
        return features


    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存处理结果到数据库
        
        将计算的衍生特征保存到目标数据库表。
        
        Args:
            data: 要保存的 DataFrame
            **kwargs: 额外参数
        
        Requirements: 6.1
        """
        if data is None or data.empty:
            self.logger.warning("没有数据需要保存")
            return
        
        self.logger.info(f"准备保存 {len(data)} 行数据到 {self.result_table}")
        
        # 重置索引，将 trade_date 从索引转为列
        save_data = data.reset_index()
        
        # 确保 trade_date 列存在
        if "trade_date" not in save_data.columns:
            self.logger.error("数据中缺少 trade_date 列")
            raise ValueError("数据中缺少 trade_date 列")
        
        # 转换日期格式为字符串（YYYYMMDD）以便数据库存储
        if pd.api.types.is_datetime64_any_dtype(save_data["trade_date"]):
            save_data["trade_date"] = save_data["trade_date"].dt.strftime("%Y%m%d")
        
        try:
            # 使用 upsert 模式保存，以 trade_date 为主键
            await self.db.save_dataframe(
                save_data,
                self.result_table,
                primary_keys=[self.date_column],
                use_insert_mode=False  # 使用 upsert 模式处理重复数据
            )
            self.logger.info(f"成功保存 {len(save_data)} 行数据到 {self.result_table}")
        except Exception as e:
            self.logger.error(f"保存数据到 {self.result_table} 失败: {e}", exc_info=True)
            raise
    
    def get_task_info(self) -> Dict[str, Any]:
        """获取任务详细信息"""
        info = super().get_task_info()
        info.update({
            "source_table": self.source_table,
            "result_table": self.result_table,
            "zscore_window": self.zscore_window,
            "percentile_window": self.percentile_window,
        })
        return info
