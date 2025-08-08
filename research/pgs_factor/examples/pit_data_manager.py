#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据管理器
==============

提供Point-in-Time数据表的完整管理功能，包括自动建表、全量回填和增量更新。

主要功能：
1. 自动建表：检测并创建缺失的PIT表
2. 全量重建：重建所有PIT表数据
3. 增量更新：仅处理新增或更新的数据
4. 数据源支持：report, express, forecast
5. 智能单季化：累计数据转换为单季数据
6. 进度跟踪：详细的执行进度和错误处理

使用示例：
    # 全量重建（自动检测并创建表）
    python -m research.pgs_factor.examples.pit_data_manager --mode full --batch-size 100
    
    # 增量更新（最近30天）
    python -m research.pgs_factor.examples.pit_data_manager --mode incremental --days 30
    
    # 指定股票和数据源
    python -m research.pgs_factor.examples.pit_data_manager --mode full --stocks 000001.SZ,601020.SH --sources report,forecast
    
    # 验证数据质量
    python -m research.pgs_factor.examples.pit_data_manager --verify 601020.SH
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import pandas as pd
from pathlib import Path

from research.tools.context import ResearchContext
from research.pgs_factor.database.db_manager import PGSFactorDBManager


class PITDataManager:
    """Point-in-Time数据管理器"""
    
    def __init__(self, ctx: ResearchContext, batch_size: int = 500):
        self.ctx = ctx
        self.mgr = PGSFactorDBManager(ctx)
        self.batch_size = batch_size
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('PITDataManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def get_stock_universe(self, stocks: Optional[List[str]] = None) -> List[str]:
        """获取股票池"""
        if stocks:
            return stocks
            
        # 从所有数据源获取股票列表
        query = """
        SELECT DISTINCT ts_code FROM (
            SELECT ts_code FROM tushare.fina_income
            UNION
            SELECT ts_code FROM tushare.fina_express  
            UNION
            SELECT ts_code FROM tushare.fina_forecast
        ) t
        ORDER BY ts_code
        """
        
        df = self.ctx.query_dataframe(query)
        if df is None or df.empty:
            self.logger.warning("未找到任何股票数据")
            return []
        
        return df['ts_code'].tolist()
    
    def ensure_tables_exist(self):
        """检测并创建PIT表（如果不存在）"""
        self.logger.info("检测PIT表是否存在...")
        
        # 检测表是否存在
        check_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'pgs_factors' 
          AND table_name IN ('pit_income_quarterly', 'pit_balance_quarterly')
        """
        
        existing_tables = self.ctx.query_dataframe(check_query)
        existing_table_names = set()
        if existing_tables is not None and not existing_tables.empty:
            existing_table_names = set(existing_tables['table_name'].tolist())
        
        # 需要创建的表
        required_tables = {'pit_income_quarterly', 'pit_balance_quarterly'}
        missing_tables = required_tables - existing_table_names
        
        if not missing_tables:
            self.logger.info("所有PIT表已存在")
            return
        
        self.logger.info(f"需要创建缺失的表: {missing_tables}")
        
        # 创建schema（如果不存在）
        self.logger.info("确保schema存在...")
        self.ctx.db_manager.execute_sync("CREATE SCHEMA IF NOT EXISTS pgs_factors;")
        
        # 创建pit_income_quarterly表
        if 'pit_income_quarterly' in missing_tables:
            self.logger.info("创建 pit_income_quarterly 表...")
            create_income_sql = """
            CREATE TABLE pgs_factors.pit_income_quarterly (
                id SERIAL PRIMARY KEY,
                ts_code VARCHAR(10) NOT NULL,
                end_date DATE NOT NULL,
                ann_date DATE NOT NULL,
                data_source VARCHAR(20),
                year INT,
                quarter INT,
                n_income_attr_p FLOAT,
                n_income FLOAT,
                revenue FLOAT,
                operate_profit FLOAT,
                total_profit FLOAT,
                income_tax FLOAT,
                oper_cost FLOAT,
                total_cogs FLOAT,

                fin_exp FLOAT,
                interest_expense FLOAT,
                net_profit_mid FLOAT,
                conversion_status VARCHAR(20) DEFAULT 'SINGLE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ts_code, end_date, ann_date)
            );
            """
            self.ctx.db_manager.execute_sync(create_income_sql)
            
            # 创建索引
            self.logger.info("创建 pit_income_quarterly 索引...")
            index_sqls = [
                "CREATE INDEX IF NOT EXISTS idx_pit_income_q_stock ON pgs_factors.pit_income_quarterly(ts_code);",
                "CREATE INDEX IF NOT EXISTS idx_pit_income_q_end ON pgs_factors.pit_income_quarterly(end_date);",
                "CREATE INDEX IF NOT EXISTS idx_pit_income_q_ann ON pgs_factors.pit_income_quarterly(ann_date);"
            ]
            for sql in index_sqls:
                self.ctx.db_manager.execute_sync(sql)
        
        # 创建pit_balance_quarterly表
        if 'pit_balance_quarterly' in missing_tables:
            self.logger.info("创建 pit_balance_quarterly 表...")
            create_balance_sql = """
            CREATE TABLE pgs_factors.pit_balance_quarterly (
                id SERIAL PRIMARY KEY,
                ts_code VARCHAR(10) NOT NULL,
                end_date DATE NOT NULL,
                ann_date DATE NOT NULL,
                data_source VARCHAR(20),
                year INT,
                quarter INT,
                tot_assets FLOAT,
                tot_liab FLOAT,
                tot_equity FLOAT,
                total_cur_assets FLOAT,
                total_cur_liab FLOAT,
                inventories FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ts_code, end_date, ann_date)
            );
            """
            self.ctx.db_manager.execute_sync(create_balance_sql)
            
            # 创建索引
            self.logger.info("创建 pit_balance_quarterly 索引...")
            index_sqls = [
                "CREATE INDEX IF NOT EXISTS idx_pit_balance_q_stock ON pgs_factors.pit_balance_quarterly(ts_code);",
                "CREATE INDEX IF NOT EXISTS idx_pit_balance_q_end ON pgs_factors.pit_balance_quarterly(end_date);",
                "CREATE INDEX IF NOT EXISTS idx_pit_balance_q_ann ON pgs_factors.pit_balance_quarterly(ann_date);"
            ]
            for sql in index_sqls:
                self.ctx.db_manager.execute_sync(sql)
        
        self.logger.info("PIT表创建完成")
    
    def process_report_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理财务报告数据（report）"""
        self.logger.info("开始处理财务报告数据...")
        
        where_conditions = ["ts_code = ANY(%(stocks)s)"]
        params = {"stocks": stocks}
        
        if start_date:
            where_conditions.append("ann_date >= %(start_date)s")
            params["start_date"] = start_date
        
        where_clause = " AND ".join(where_conditions)
        
        # 处理损益表数据
        income_query = f"""
        WITH latest_income AS (
            SELECT ts_code, end_date, ann_date, 
                   n_income_attr_p, n_income, revenue, operate_profit, total_profit,
                   income_tax, oper_cost, total_cogs, fin_exp, fin_exp_int_exp,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_income
            WHERE {where_clause}
        )
        SELECT ts_code, end_date::date as end_date, ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               n_income_attr_p::numeric as n_income_attr_p,
               n_income::numeric as n_income,
               revenue::numeric as revenue,
               operate_profit::numeric as operate_profit,
               total_profit::numeric as total_profit,
               income_tax::numeric as income_tax,
               oper_cost::numeric as oper_cost,
               total_cogs::numeric as total_cogs,
               fin_exp::numeric as fin_exp,
               fin_exp_int_exp::numeric as interest_expense
        FROM latest_income
        WHERE rn = 1
        ORDER BY ts_code, end_date
        """
        
        df = self.ctx.query_dataframe(income_query, params)
        if df is None or df.empty:
            self.logger.warning("未找到财务报告数据")
            return 0
        
        # 转换为单季数据：同时处理 n_income 与 n_income_attr_p
        df_single_income = self._convert_to_single_quarter(df, 'n_income')
        df_single_attr = self._convert_to_single_quarter(df, 'n_income_attr_p')
        # 合并所需字段，确保包含conversion_status
        merge_keys = ['ts_code','end_date','ann_date','year','quarter']

        # 确保conversion_status字段被包含在合并结果中
        df_single = df_single_income.copy()  # 保留所有字段，包括conversion_status

        # 只添加n_income_attr_p_single字段
        if 'n_income_attr_p_single' in df_single_attr.columns:
            df_single = df_single.merge(
                df_single_attr[merge_keys + ['n_income_attr_p_single']],
                on=merge_keys,
                how='left',
                suffixes=('', '_attr')
            )
        
        # 检查conversion_status字段是否存在
        if 'conversion_status' not in df_single.columns:
            self.logger.warning("conversion_status字段不存在！")

        # 准备插入数据
        insert_data = pd.DataFrame({
            'ts_code': df_single['ts_code'],
            'end_date': df_single['end_date'],
            'ann_date': df_single['ann_date'],
            'data_source': 'report',
            'year': df_single['year'],
            'quarter': df_single['quarter'],
            'n_income': df_single['n_income_single'],
            'n_income_attr_p': df_single['n_income_attr_p_single'],
            'revenue': df_single.get('revenue_single', None),
            'operate_profit': df_single.get('operate_profit_single', None),
            'total_profit': df_single.get('total_profit_single', None),
            'income_tax': df_single.get('income_tax_single', None),
            'oper_cost': df_single.get('oper_cost_single', None),
            'total_cogs': df_single.get('total_cogs_single', None),
            'fin_exp': df_single.get('fin_exp_single', None),
            'interest_expense': df_single.get('interest_expense_single', None),
            'net_profit_mid': None,
            'conversion_status': df_single.get('conversion_status', 'SINGLE')
        })
        
        # 直接批量插入（数据库层面已优化）
        self.mgr.save_pit_income_quarterly(insert_data)
        total_rows = len(insert_data)
        
        # 统计conversion_status分布
        status_counts = insert_data['conversion_status'].value_counts().to_dict()
        status_summary = ', '.join([f"{k}:{v}" for k, v in status_counts.items()])

        self.logger.info(f"财务报告数据处理完成，共处理 {total_rows} 行 (状态分布: {status_summary})")
        return total_rows
    
    def process_express_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理业绩快报数据（express）"""
        self.logger.info("开始处理业绩快报数据...")
        
        where_conditions = ["ts_code = ANY(%(stocks)s)"]
        params = {"stocks": stocks}
        
        if start_date:
            where_conditions.append("ann_date >= %(start_date)s")
            params["start_date"] = start_date
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        WITH latest_express AS (
            SELECT ts_code, end_date, ann_date, n_income,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_express
            WHERE {where_clause}
        )
        SELECT ts_code, end_date::date as end_date, ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               n_income::numeric * 10000 as n_income_attr_p,
               n_income::numeric * 10000 as n_income
        FROM latest_express
        WHERE rn = 1
        ORDER BY ts_code, end_date
        """
        
        df = self.ctx.query_dataframe(query, params)
        if df is None or df.empty:
            self.logger.warning("未找到业绩快报数据")
            return 0
        
        # 转换为单季数据
        df_single = self._convert_to_single_quarter(df, 'n_income')
        
        # 准备插入数据
        insert_data = pd.DataFrame({
            'ts_code': df_single['ts_code'],
            'end_date': df_single['end_date'],
            'ann_date': df_single['ann_date'],
            'data_source': 'express',
            'year': df_single['year'],
            'quarter': df_single['quarter'],
            'n_income': df_single['n_income_single'],
            'n_income_attr_p': df_single['n_income_attr_p'],
            'net_profit_mid': None,
            'conversion_status': df_single.get('conversion_status', 'SINGLE')
        })
        
        # 直接批量插入
        self.mgr.save_pit_income_quarterly(insert_data)
        total_rows = len(insert_data)
        
        self.logger.info(f"业绩快报数据处理完成，共处理 {total_rows} 行")
        return total_rows
    
    def process_forecast_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理业绩预告数据（forecast）"""
        self.logger.info("开始处理业绩预告数据...")
        
        where_conditions = ["ts_code = ANY(%(stocks)s)"]
        params = {"stocks": stocks}
        
        if start_date:
            where_conditions.append("ann_date >= %(start_date)s")
            params["start_date"] = start_date
        
        where_clause = " AND ".join(where_conditions)
        
        # 1. 获取forecast数据 - 支持直接净利润和同比计算两种方式
        forecast_query = f"""
        WITH latest_forecast AS (
            SELECT ts_code, end_date, ann_date, 
                   net_profit_min, net_profit_max,
                   p_change_min, p_change_max, last_parent_net,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_forecast
            WHERE {where_clause}
        )
        SELECT ts_code, end_date::date as end_date, ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               CASE 
                   -- 优先使用直接净利润范围（排除NaN）
                   WHEN net_profit_min IS NOT NULL AND net_profit_max IS NOT NULL 
                        AND NOT (net_profit_min = 'NaN' OR net_profit_max = 'NaN') THEN
                       ((net_profit_min + net_profit_max)/2.0)::numeric * 10000
                   -- 当直接净利润为空或NaN但有同比数据时，通过同比计算
                   WHEN (net_profit_min IS NULL OR net_profit_max IS NULL 
                         OR net_profit_min = 'NaN' OR net_profit_max = 'NaN') 
                        AND p_change_min IS NOT NULL AND p_change_max IS NOT NULL 
                        AND last_parent_net IS NOT NULL 
                        AND NOT (p_change_min = 'NaN' OR p_change_max = 'NaN' OR last_parent_net = 'NaN') THEN
                       (last_parent_net * (1 + (p_change_min + p_change_max)/200.0))::numeric * 10000
                   ELSE NULL
               END as net_profit_mid,
               CASE 
                   WHEN net_profit_min IS NOT NULL AND net_profit_max IS NOT NULL 
                        AND NOT (net_profit_min = 'NaN' OR net_profit_max = 'NaN') THEN 'direct'
                   WHEN (net_profit_min IS NULL OR net_profit_max IS NULL 
                         OR net_profit_min = 'NaN' OR net_profit_max = 'NaN') 
                        AND p_change_min IS NOT NULL AND p_change_max IS NOT NULL 
                        AND last_parent_net IS NOT NULL 
                        AND NOT (p_change_min = 'NaN' OR p_change_max = 'NaN' OR last_parent_net = 'NaN') THEN 'calculated'
                   ELSE 'unavailable'
               END as data_source_type
        FROM latest_forecast
        WHERE rn = 1 
          AND (
              (net_profit_min IS NOT NULL AND net_profit_max IS NOT NULL 
               AND NOT (net_profit_min = 'NaN' OR net_profit_max = 'NaN')) OR
              (p_change_min IS NOT NULL AND p_change_max IS NOT NULL AND last_parent_net IS NOT NULL
               AND NOT (p_change_min = 'NaN' OR p_change_max = 'NaN' OR last_parent_net = 'NaN'))
          )
        ORDER BY ts_code, end_date
        """
        
        df = self.ctx.query_dataframe(forecast_query, params)
        if df is None or df.empty:
            self.logger.warning("未找到业绩预告数据")
            return 0
        
        # 统计数据来源分布
        source_stats = df['data_source_type'].value_counts()
        self.logger.info(f"业绩预告数据来源分布: {source_stats.to_dict()}")

        # 2. 计算forecast的单季值：
        #    单季 = 预告中值(YTD) - 年内已披露report单季累计（上一季为止）
        #    Q1 特例：单季 = 预告中值
        # 优先从PIT表中读取当年report单季值，确保与已入库口径一致
        # 注意：forecast预告的是归母净利润，应该与report的n_income_attr_p字段对比
        report_query = """
        SELECT
            ts_code,
            EXTRACT(YEAR FROM end_date)::int AS year,
            EXTRACT(QUARTER FROM end_date)::int AS quarter,
            n_income_attr_p AS n_income_attr_p_single
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = ANY(%(stocks)s)
          AND data_source = 'report'
        """
        report_df = self.ctx.query_dataframe(report_query, {"stocks": stocks})

        # 使用辅助函数生成单季forecast值（列：n_income_attr_p_single）
        merged = self._compute_forecast_single_quarter(df, report_df)

        # 3. 准备插入数据
        # 重要：forecast预告的是归母净利润，只填充n_income_attr_p字段，n_income保持为NULL
        # 其他缺失字段也使用None而不是0
        insert_data = pd.DataFrame({
            'ts_code': merged['ts_code'],
            'end_date': merged['end_date'],
            'ann_date': merged['ann_date'],
            'data_source': merged['data_source_type'].apply(lambda x: f'forecast_{x}'),
            'year': merged['year'],
            'quarter': merged['quarter'],
            'n_income': None,  # forecast数据不包含净利润，保持为NULL
            'n_income_attr_p': merged['n_income_attr_p_single'],  # 归母净利润单季值
            'revenue': None,  # forecast数据不包含营收，保持为NULL
            'operate_profit': None,  # forecast数据不包含营业利润，保持为NULL
            'total_profit': None,  # forecast数据不包含利润总额，保持为NULL
            'income_tax': None,  # forecast数据不包含所得税，保持为NULL
            'oper_cost': None,  # forecast数据不包含营业成本，保持为NULL
            'total_cogs': None,  # forecast数据不包含营业总成本，保持为NULL
            'fin_exp': None,  # forecast数据不包含财务费用，保持为NULL
            'interest_expense': None,  # forecast数据不包含利息支出，保持为NULL
            'net_profit_mid': merged['net_profit_mid'],  # 保持原始预告中值
            'conversion_status': 'FORECAST'  # forecast数据标记为预告类型
        })
        
        # 3. 直接批量插入
        self.mgr.save_pit_income_quarterly(insert_data)
        total_rows = len(insert_data)
        
        self.logger.info(f"业绩预告数据处理完成，共处理 {total_rows} 行")
        return total_rows
    
    def process_balance_data(self, stocks: List[str], start_date: Optional[str] = None) -> int:
        """处理资产负债表数据（balance）"""
        self.logger.info("开始处理资产负债表数据...")
        
        where_conditions = ["ts_code = ANY(%(stocks)s)"]
        params = {"stocks": stocks}
        
        if start_date:
            where_conditions.append("ann_date >= %(start_date)s")
            params["start_date"] = start_date
        
        where_clause = " AND ".join(where_conditions)
        
        # 处理资产负债表数据
        balance_query = f"""
        WITH latest_balance AS (
            SELECT ts_code, end_date, ann_date, 
                   total_assets, total_liab, (total_assets - total_liab) as tot_equity, 
                   total_cur_assets, total_cur_liab, inventories,
                   ROW_NUMBER() OVER (PARTITION BY ts_code, end_date ORDER BY ann_date DESC) as rn
            FROM tushare.fina_balancesheet
            WHERE {where_clause}
        )
        SELECT ts_code, end_date::date as end_date, ann_date::date as ann_date,
               EXTRACT(YEAR FROM end_date)::int as year,
               EXTRACT(QUARTER FROM end_date)::int as quarter,
               total_assets::numeric * 10000 as tot_assets,
               total_liab::numeric * 10000 as tot_liab,
               tot_equity::numeric * 10000 as tot_equity,
               total_cur_assets::numeric * 10000 as total_cur_assets,
               total_cur_liab::numeric * 10000 as total_cur_liab,
               inventories::numeric * 10000 as inventories
        FROM latest_balance
        WHERE rn = 1
        ORDER BY ts_code, end_date
        """
        
        df = self.ctx.query_dataframe(balance_query, params)
        if df is None or df.empty:
            self.logger.warning("未找到资产负债表数据")
            return 0
        
        # 准备插入数据（资产负债表是时点数据，不需要单季化）
        insert_data = pd.DataFrame({
            'ts_code': df['ts_code'],
            'end_date': df['end_date'],
            'ann_date': df['ann_date'],
            'data_source': 'report',
            'year': df['year'],
            'quarter': df['quarter'],
            'tot_assets': df['tot_assets'],
            'tot_liab': df['tot_liab'],
            'tot_equity': df['tot_equity'],
            'total_cur_assets': df.get('total_cur_assets', None),
            'total_cur_liab': df.get('total_cur_liab', None),
            'inventories': df.get('inventories', None)
        })
        
        # 直接批量插入到pit_balance_quarterly表
        self.mgr.save_pit_balance_quarterly(insert_data)
        total_rows = len(insert_data)
        
        self.logger.info(f"资产负债表数据处理完成，共处理 {total_rows} 行")
        return total_rows
    
    def _convert_to_single_quarter(self, df: pd.DataFrame, value_col: str) -> pd.DataFrame:
        """将累计数据转换为单季数据 - 增强版，处理边界情况

        新增功能：为每条记录设置conversion_status字段，标识数据的单季化状态
        """
        if df is None or df.empty:
            return df

        result = df.copy()
        result = result.sort_values(['ts_code', 'year', 'quarter'])

        # 初始化conversion_status字段
        result['conversion_status'] = 'SINGLE'  # 默认为单季值

        # 需要单季化的字段列表
        cumulative_fields = [value_col, 'revenue', 'operate_profit', 'total_profit',
                             'income_tax', 'oper_cost', 'total_cogs', 'fin_exp', 'interest_expense']

        # 用于避免重复警告的集合
        warned_combinations = set()

        # 初始化所有单季字段（在处理前初始化）
        for field in cumulative_fields:
            if field in result.columns:
                # 确保数值类型
                result[field] = pd.to_numeric(result[field], errors='coerce').fillna(0.0)
                # 初始化单季字段
                result[f'{field}_single'] = 0.0

        # 然后按股票和年份分组，设置conversion_status和计算所有字段
        grouped = result.groupby(['ts_code', 'year'])

        for (ts_code, year), group in grouped:
            group = group.sort_values('quarter')
            quarters = sorted(group['quarter'].tolist())

            # 获取当前年份的数据索引
            mask = (result['ts_code'] == ts_code) & (result['year'] == year)

            # 根据季报模式设置conversion_status（只设置一次）
            if quarters == [1]:  # 仅一季报
                result.loc[mask, 'conversion_status'] = 'SINGLE'
            elif quarters == [4]:  # 仅年报
                result.loc[mask, 'conversion_status'] = 'ANNUAL'
            elif len(quarters) == 1 and quarters[0] in [2, 3]:  # 单个季报（Q2或Q3）
                result.loc[mask, 'conversion_status'] = 'CUMULATIVE'
            elif quarters == [1, 2]:  # Q1+中报
                result.loc[mask & (result['quarter'] == 1), 'conversion_status'] = 'SINGLE'
                result.loc[mask & (result['quarter'] == 2), 'conversion_status'] = 'SINGLE'
            elif quarters == [1, 4]:  # Q1+年报
                result.loc[mask & (result['quarter'] == 1), 'conversion_status'] = 'SINGLE'
                result.loc[mask & (result['quarter'] == 4), 'conversion_status'] = 'CALCULATED'
            elif quarters == [2, 4]:  # 中报+年报
                result.loc[mask & (result['quarter'] == 2), 'conversion_status'] = 'CUMULATIVE'
                result.loc[mask & (result['quarter'] == 4), 'conversion_status'] = 'CALCULATED'
            elif quarters == [3, 4]:  # Q3+年报
                result.loc[mask & (result['quarter'] == 3), 'conversion_status'] = 'CUMULATIVE'
                result.loc[mask & (result['quarter'] == 4), 'conversion_status'] = 'SINGLE'
            elif len(quarters) >= 3:  # 完整季报或接近完整
                result.loc[mask, 'conversion_status'] = 'SINGLE'
            else:  # 其他不规则情况
                result.loc[mask, 'conversion_status'] = 'CUMULATIVE'
                # 只对每个股票-年份组合警告一次，避免重复
                warning_key = (ts_code, year)
                if warning_key not in warned_combinations:
                    self.logger.warning(f"{ts_code} {year}年季报模式异常: {quarters}，使用累计值")
                    warned_combinations.add(warning_key)

            # 同时处理所有字段的单季化（避免重复循环）
            for field in cumulative_fields:
                if field in result.columns:
                    self._process_single_field_for_group(result, group, field, quarters, mask)


        return result

    def _process_single_field_for_group(self, result: pd.DataFrame, group: pd.DataFrame,
                                       field: str, quarters: list, mask: pd.Series) -> None:
        """为单个股票-年份组合处理单个字段的单季化"""

        # 根据不同的季报模式处理
        if quarters == [1]:  # 仅一季报
            # Q1数据本身就是单季数据，直接使用
            result.loc[mask & (result['quarter'] == 1), f'{field}_single'] = \
                group[group['quarter'] == 1][field].iloc[0]

        elif quarters == [4]:  # 仅年报
            # 年报数据直接作为年度总值，无法拆分季度
            result.loc[mask & (result['quarter'] == 4), f'{field}_single'] = \
                group[group['quarter'] == 4][field].iloc[0]

        elif len(quarters) == 1 and quarters[0] in [2, 3]:  # 单个季报（Q2或Q3）
            # 单个季报数据，无法进行单季化，直接使用累计值
            quarter = quarters[0]
            result.loc[mask & (result['quarter'] == quarter), f'{field}_single'] = \
                group[group['quarter'] == quarter][field].iloc[0]

        elif quarters == [1, 2]:  # Q1+中报
            # Q1是单季数据，Q2需要用中报累计减去Q1
            q1_single = float(group[group['quarter'] == 1][field].iloc[0] or 0)
            q2_cumulative = float(group[group['quarter'] == 2][field].iloc[0] or 0)

            result.loc[mask & (result['quarter'] == 1), f'{field}_single'] = q1_single
            result.loc[mask & (result['quarter'] == 2), f'{field}_single'] = q2_cumulative - q1_single

        elif quarters == [1, 4]:  # Q1+年报
            # Q1是单季数据，Q4需要用年报累计减去Q1（实际是Q2+Q3+Q4合计）
            q1_single = float(group[group['quarter'] == 1][field].iloc[0] or 0)
            q4_cumulative = float(group[group['quarter'] == 4][field].iloc[0] or 0)

            result.loc[mask & (result['quarter'] == 1), f'{field}_single'] = q1_single
            result.loc[mask & (result['quarter'] == 4), f'{field}_single'] = q4_cumulative - q1_single

        elif quarters == [2, 4]:  # 中报+年报
            # Q2作为上半年累计，Q4作为全年累计
            q2_cumulative = float(group[group['quarter'] == 2][field].iloc[0] or 0)
            q4_cumulative = float(group[group['quarter'] == 4][field].iloc[0] or 0)

            # Q2无法拆分，只能作为上半年总值
            result.loc[mask & (result['quarter'] == 2), f'{field}_single'] = q2_cumulative
            # Q4 = 全年 - 上半年
            result.loc[mask & (result['quarter'] == 4), f'{field}_single'] = q4_cumulative - q2_cumulative

        elif quarters == [3, 4]:  # Q3+年报
            # Q3是前三季度累计，Q4需要用年报累计减去Q3累计
            q3_cumulative = float(group[group['quarter'] == 3][field].iloc[0] or 0)
            q4_cumulative = float(group[group['quarter'] == 4][field].iloc[0] or 0)

            # Q3无法拆分，保持累计值
            result.loc[mask & (result['quarter'] == 3), f'{field}_single'] = q3_cumulative
            # Q4 = 全年 - 前三季度
            result.loc[mask & (result['quarter'] == 4), f'{field}_single'] = q4_cumulative - q3_cumulative

        elif len(quarters) >= 3:  # 完整季报或接近完整
            # 标准单季化处理
            prev_cumulative = 0.0
            for quarter in quarters:
                current_cumulative = float(group[group['quarter'] == quarter][field].iloc[0] or 0)
                if quarter == 1:
                    single_value = current_cumulative
                else:
                    single_value = current_cumulative - prev_cumulative

                result.loc[mask & (result['quarter'] == quarter), f'{field}_single'] = single_value
                prev_cumulative = current_cumulative

        else:  # 其他不规则情况
            # 保守处理：直接使用累计值，标记为异常
            for quarter in quarters:
                cumulative_value = group[group['quarter'] == quarter][field].iloc[0]
                result.loc[mask & (result['quarter'] == quarter), f'{field}_single'] = cumulative_value
    
    def _compute_forecast_single_quarter(self, forecast_df: pd.DataFrame, report_df: pd.DataFrame) -> pd.DataFrame:
        """计算forecast的单季归母净利润数据

        重要概念修正：
        - forecast预告的net_profit范围指的是"归母净利润"，对应n_income_attr_p字段
        - 计算单季值时应该用forecast预告值减去report的n_income_attr_p累计值
        """
        if forecast_df is None or forecast_df.empty:
            return forecast_df

        result = forecast_df.copy()

        # 为report数据添加年内归母净利润累计和
        if report_df is not None and not report_df.empty:
            report_sorted = report_df.sort_values(['ts_code', 'year', 'quarter'])
            # 确保数值类型
            report_sorted['n_income_attr_p_single'] = pd.to_numeric(report_sorted['n_income_attr_p_single'], errors='coerce').fillna(0.0)
            report_sorted['ytd_sum_before'] = (
                report_sorted.groupby(['ts_code', 'year'])['n_income_attr_p_single']
                .cumsum() - report_sorted['n_income_attr_p_single']
            )
        else:
            report_sorted = pd.DataFrame(columns=['ts_code', 'year', 'quarter', 'ytd_sum_before'])

        # 合并数据
        merged = result.merge(
            report_sorted[['ts_code', 'year', 'quarter', 'ytd_sum_before']],
            on=['ts_code', 'year', 'quarter'],
            how='left'
        )

        merged['ytd_sum_before'] = merged['ytd_sum_before'].fillna(0.0)

        # 确保数据类型一致
        merged['net_profit_mid'] = pd.to_numeric(merged['net_profit_mid'], errors='coerce').fillna(0.0)
        merged['ytd_sum_before'] = pd.to_numeric(merged['ytd_sum_before'], errors='coerce').fillna(0.0)

        # 计算单季归母净利润值
        merged['n_income_attr_p_single'] = merged.apply(
            lambda row: (
                float(row['net_profit_mid']) if row['quarter'] == 1
                else float(row['net_profit_mid']) - float(row['ytd_sum_before'])
            ),
            axis=1
        )

        return merged
    
    def full_rebuild(self, stocks: Optional[List[str]] = None, sources: Optional[List[str]] = None):
        """全量重建PIT表"""
        self.logger.info("开始全量重建PIT表...")
        start_time = time.time()
        
        # 检测并创建表（如果不存在）
        self.ensure_tables_exist()
        
        # 获取股票池
        stock_list = self.get_stock_universe(stocks)
        self.logger.info(f"目标股票数量: {len(stock_list)}")
        
        # 确定要处理的数据源
        all_sources = ['report', 'express', 'forecast']
        target_sources = sources if sources else all_sources
        
        total_processed = 0
        
        # 分批处理股票
        for i in range(0, len(stock_list), self.batch_size):
            batch_stocks = stock_list[i:i+self.batch_size]
            self.logger.info(f"处理股票批次 {i//self.batch_size + 1}: {batch_stocks[:3]}{'...' if len(batch_stocks) > 3 else ''}")
            
            # 按数据源处理
            if 'report' in target_sources:
                total_processed += self.process_report_data(batch_stocks)
                # 处理资产负债表数据（只来自report）
                total_processed += self.process_balance_data(batch_stocks)
            
            if 'express' in target_sources:
                total_processed += self.process_express_data(batch_stocks)
            
            if 'forecast' in target_sources:
                total_processed += self.process_forecast_data(batch_stocks)
        
        elapsed = time.time() - start_time
        self.logger.info(f"全量重建完成! 总处理行数: {total_processed}, 耗时: {elapsed:.2f}秒")
    
    def incremental_update(self, days: int = 30, stocks: Optional[List[str]] = None, sources: Optional[List[str]] = None):
        """增量更新（处理最近N天的数据）"""
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        self.logger.info(f"开始增量更新（最近{days}天，起始日期: {start_date}）...")
        
        start_time = time.time()
        
        # 获取股票池
        stock_list = self.get_stock_universe(stocks)
        self.logger.info(f"目标股票数量: {len(stock_list)}")
        
        # 确定要处理的数据源
        all_sources = ['report', 'express', 'forecast']
        target_sources = sources if sources else all_sources
        
        total_processed = 0
        
        # 分批处理股票
        for i in range(0, len(stock_list), self.batch_size):
            batch_stocks = stock_list[i:i+self.batch_size]
            self.logger.info(f"处理股票批次 {i//self.batch_size + 1}: {batch_stocks[:3]}{'...' if len(batch_stocks) > 3 else ''}")
            
            # 按数据源处理
            if 'report' in target_sources:
                total_processed += self.process_report_data(batch_stocks, start_date)
                # 处理资产负债表数据（只来自report）
                total_processed += self.process_balance_data(batch_stocks, start_date)
            
            if 'express' in target_sources:
                total_processed += self.process_express_data(batch_stocks, start_date)
            
            if 'forecast' in target_sources:
                total_processed += self.process_forecast_data(batch_stocks, start_date)
        
        elapsed = time.time() - start_time
        self.logger.info(f"增量更新完成! 总处理行数: {total_processed}, 耗时: {elapsed:.2f}秒")
    
    def verify_data(self, ts_code: str) -> Dict[str, Any]:
        """验证指定股票的数据质量"""
        self.logger.info(f"验证股票 {ts_code} 的数据质量...")
        
        query = """
        SELECT data_source, COUNT(*) as count,
               MIN(end_date) as min_date, MAX(end_date) as max_date,
               COUNT(CASE WHEN n_income IS NOT NULL THEN 1 END) as n_income_count,
               COUNT(CASE WHEN net_profit_mid IS NOT NULL THEN 1 END) as forecast_count
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = %(ts_code)s
        GROUP BY data_source
        ORDER BY data_source
        """
        
        df = self.ctx.query_dataframe(query, {'ts_code': ts_code})
        
        result = {
            'ts_code': ts_code,
            'summary': df.to_dict('records') if df is not None else [],
            'sample_data': None
        }
        
        # 获取样本数据
        sample_query = """
        SELECT ts_code, end_date, ann_date, data_source, year, quarter,
               n_income, n_income_attr_p, net_profit_mid
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = %(ts_code)s
        ORDER BY end_date DESC, ann_date DESC
        LIMIT 10
        """
        
        sample_df = self.ctx.query_dataframe(sample_query, {'ts_code': ts_code})
        if sample_df is not None:
            result['sample_data'] = sample_df.to_dict('records')
        
        return result


def main():
    parser = argparse.ArgumentParser(description='PIT数据管理器')
    parser.add_argument('--mode', choices=['full', 'incremental'], 
                        help='运行模式: full=全量重建, incremental=增量更新')
    parser.add_argument('--stocks', type=str, help='指定股票列表（逗号分隔），可选')
    parser.add_argument('--sources', type=str, help='指定数据源（逗号分隔）: report,express,forecast，可选')
    parser.add_argument('--batch-size', type=int, default=100, help='批处理大小')
    parser.add_argument('--days', type=int, default=30, help='增量更新的天数（仅在incremental模式下有效）')
    parser.add_argument('--verify', type=str, help='验证指定股票的数据质量')
    
    args = parser.parse_args()
    
    # 验证模式和其他模式互斥
    if args.verify and args.mode:
        parser.error("--verify 不能与 --mode 同时使用")
    if not args.verify and not args.mode:
        parser.error("必须指定 --mode 或 --verify 参数")
    
    # 解析参数
    stocks = [s.strip() for s in args.stocks.split(',')] if args.stocks else None
    sources = [s.strip() for s in args.sources.split(',')] if args.sources else None
    
    with ResearchContext() as ctx:
        manager = PITDataManager(ctx, args.batch_size)
        
        try:
            if args.verify:
                # 验证模式
                result = manager.verify_data(args.verify)
                print(f"\n=== 数据验证结果: {args.verify} ===")
                print(f"汇总统计:")
                for item in result['summary']:
                    print(f"  {item['data_source']}: {item['count']}行 "
                          f"({item['min_date']} ~ {item['max_date']})")
                
                if result['sample_data']:
                    print(f"\n最新样本数据:")
                    for row in result['sample_data'][:5]:
                        print(f"  {row['end_date']} {row['data_source']} "
                              f"n_income={row['n_income']} mid={row['net_profit_mid']}")
                
            elif args.mode == 'full':
                # 全量重建
                manager.full_rebuild(stocks=stocks, sources=sources)
                
            elif args.mode == 'incremental':
                # 增量更新
                manager.incremental_update(days=args.days, stocks=stocks, sources=sources)
                
        except Exception as e:
            manager.logger.error(f"执行失败: {e}")
            raise


if __name__ == '__main__':
    main()
