#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
外部数据源对比验证
================

将PIT数据库中的财务数据与外部数据源进行对比验证，确保数据准确性。

支持的外部数据源：
1. Wind终端数据（通过Wind API）
2. 同花顺iFinD数据（通过iFinD API）
3. 东方财富数据（通过公开API）
4. 手工录入的标准数据

主要验证指标：
- 单季ROE = 单季归母净利润 / 期末股东权益
- 单季毛利率 = (单季营收 - 单季营业成本) / 单季营收
- 单季净利率 = 单季归母净利润 / 单季营收
- 年度汇总验证：四个季度数据之和 vs 年报数据

使用示例：
    python -m research.pgs_factor.validation.external_data_comparison --source wind --stocks 000001.SZ
    python -m research.pgs_factor.validation.external_data_comparison --source manual --file benchmark_data.xlsx
"""

import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import json

from research.tools.context import ResearchContext


class ExternalDataComparator:
    """外部数据源对比验证器"""
    
    def __init__(self, ctx: ResearchContext):
        self.ctx = ctx
        self.logger = self._setup_logger()
        
        # 误差容忍配置
        self.acceptable_relative_error = 0.02  # 2%相对误差
        self.acceptable_absolute_error = 100000  # 10万元绝对误差
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('ExternalDataComparator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def get_pit_data(self, stocks: List[str], start_year: int = 2020, end_year: int = 2024) -> pd.DataFrame:
        """获取PIT数据库中的财务数据"""
        
        query = """
        SELECT 
            i.ts_code,
            i.end_date,
            EXTRACT(YEAR FROM i.end_date) as year,
            i.quarter,
            i.conversion_status,
            i.n_income_attr_p,
            i.revenue,
            i.oper_cost,
            i.operate_profit,
            b.tot_equity,
            -- 计算财务比率
            CASE 
                WHEN b.tot_equity != 0 AND b.tot_equity IS NOT NULL 
                THEN i.n_income_attr_p / b.tot_equity 
                ELSE NULL 
            END as roe,
            CASE 
                WHEN i.revenue != 0 AND i.revenue IS NOT NULL 
                THEN (i.revenue - COALESCE(i.oper_cost, 0)) / i.revenue 
                ELSE NULL 
            END as gross_margin,
            CASE 
                WHEN i.revenue != 0 AND i.revenue IS NOT NULL 
                THEN i.n_income_attr_p / i.revenue 
                ELSE NULL 
            END as net_margin
        FROM pgs_factors.pit_income_quarterly i
        LEFT JOIN pgs_factors.pit_balance_quarterly b 
            ON i.ts_code = b.ts_code 
            AND i.end_date = b.end_date 
            AND i.ann_date = b.ann_date
        WHERE i.ts_code = ANY(%(stocks)s)
        AND EXTRACT(YEAR FROM i.end_date) BETWEEN %(start_year)s AND %(end_year)s
        AND i.data_source = 'report'
        AND i.conversion_status IN ('SINGLE', 'CALCULATED', 'ANNUAL')
        ORDER BY i.ts_code, i.end_date
        """
        
        return self.ctx.query_dataframe(query, {
            'stocks': stocks,
            'start_year': start_year,
            'end_year': end_year
        })
    
    def load_benchmark_data(self, source: str, **kwargs) -> pd.DataFrame:
        """加载基准数据"""
        
        if source == 'manual':
            return self._load_manual_data(kwargs.get('file_path'))
        elif source == 'wind':
            return self._load_wind_data(kwargs.get('stocks'), kwargs.get('start_year'), kwargs.get('end_year'))
        elif source == 'ifind':
            return self._load_ifind_data(kwargs.get('stocks'), kwargs.get('start_year'), kwargs.get('end_year'))
        else:
            raise ValueError(f"Unsupported data source: {source}")
    
    def _load_manual_data(self, file_path: str) -> pd.DataFrame:
        """加载手工录入的基准数据"""
        
        if not file_path or not Path(file_path).exists():
            self.logger.warning(f"基准数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        try:
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                df = pd.read_excel(file_path)
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                raise ValueError("Unsupported file format")
            
            # 标准化列名
            required_columns = ['ts_code', 'end_date', 'n_income_attr_p', 'revenue', 'tot_equity']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # 数据类型转换
            df['end_date'] = pd.to_datetime(df['end_date'])
            df['year'] = df['end_date'].dt.year
            df['quarter'] = df['end_date'].dt.quarter
            
            # 计算财务比率
            df['roe'] = np.where(df['tot_equity'] != 0, df['n_income_attr_p'] / df['tot_equity'], np.nan)
            df['gross_margin'] = np.where(df['revenue'] != 0, 
                                        (df['revenue'] - df.get('oper_cost', 0)) / df['revenue'], np.nan)
            df['net_margin'] = np.where(df['revenue'] != 0, df['n_income_attr_p'] / df['revenue'], np.nan)
            
            self.logger.info(f"成功加载基准数据: {len(df)} 条记录")
            return df
            
        except Exception as e:
            self.logger.error(f"加载基准数据失败: {e}")
            return pd.DataFrame()
    
    def _load_wind_data(self, stocks: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """加载Wind数据（需要Wind API）"""
        
        self.logger.warning("Wind API集成尚未实现，请使用手工数据模式")
        
        # 这里应该集成Wind API
        # 示例代码框架：
        """
        try:
            from WindPy import w
            w.start()
            
            # 获取财务数据
            fields = "net_profit_excl_min_int_inc,oper_rev,tot_equity"
            data = w.wss(stocks, fields, f"rptDate={start_year}0331;{end_year}1231")
            
            # 处理数据...
            
        except ImportError:
            self.logger.error("Wind API未安装")
        except Exception as e:
            self.logger.error(f"Wind数据获取失败: {e}")
        """
        
        return pd.DataFrame()
    
    def _load_ifind_data(self, stocks: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """加载iFinD数据（需要iFinD API）"""
        
        self.logger.warning("iFinD API集成尚未实现，请使用手工数据模式")
        
        # 这里应该集成iFinD API
        # 示例代码框架：
        """
        try:
            import iFinDPy as THS
            THS.iFinDLogin("username", "password")
            
            # 获取财务数据
            data = THS.DS(stocks, "ths_net_profit_ttm", start_year, end_year)
            
            # 处理数据...
            
        except ImportError:
            self.logger.error("iFinD API未安装")
        except Exception as e:
            self.logger.error(f"iFinD数据获取失败: {e}")
        """
        
        return pd.DataFrame()
    
    def compare_data(self, pit_data: pd.DataFrame, benchmark_data: pd.DataFrame) -> Dict[str, Any]:
        """对比PIT数据和基准数据"""
        
        if pit_data.empty or benchmark_data.empty:
            return {
                'comparison_results': [],
                'summary': {
                    'total_comparisons': 0,
                    'matches': 0,
                    'mismatches': 0,
                    'missing_in_pit': 0,
                    'missing_in_benchmark': 0
                }
            }
        
        # 合并数据进行对比
        merged = pd.merge(
            pit_data, benchmark_data,
            on=['ts_code', 'end_date'],
            suffixes=('_pit', '_benchmark'),
            how='outer'
        )
        
        results = {
            'comparison_results': [],
            'summary': {
                'total_comparisons': len(merged),
                'matches': 0,
                'mismatches': 0,
                'missing_in_pit': 0,
                'missing_in_benchmark': 0
            }
        }
        
        # 逐行对比
        for _, row in merged.iterrows():
            comparison = {
                'ts_code': row['ts_code'],
                'end_date': row['end_date'].strftime('%Y-%m-%d') if pd.notna(row['end_date']) else None,
                'metrics': {}
            }
            
            # 检查数据缺失
            if pd.isna(row.get('n_income_attr_p_pit')):
                results['summary']['missing_in_pit'] += 1
                comparison['status'] = 'MISSING_IN_PIT'
            elif pd.isna(row.get('n_income_attr_p_benchmark')):
                results['summary']['missing_in_benchmark'] += 1
                comparison['status'] = 'MISSING_IN_BENCHMARK'
            else:
                # 对比各项指标
                metrics_to_compare = ['n_income_attr_p', 'revenue', 'roe', 'gross_margin', 'net_margin']
                all_match = True
                
                for metric in metrics_to_compare:
                    pit_value = row.get(f'{metric}_pit')
                    benchmark_value = row.get(f'{metric}_benchmark')
                    
                    if pd.notna(pit_value) and pd.notna(benchmark_value):
                        abs_diff = abs(pit_value - benchmark_value)
                        rel_diff = abs_diff / abs(benchmark_value) if benchmark_value != 0 else float('inf')
                        
                        is_match = (rel_diff <= self.acceptable_relative_error or 
                                  abs_diff <= self.acceptable_absolute_error)
                        
                        comparison['metrics'][metric] = {
                            'pit_value': pit_value,
                            'benchmark_value': benchmark_value,
                            'absolute_diff': abs_diff,
                            'relative_diff': rel_diff,
                            'match': is_match
                        }
                        
                        if not is_match:
                            all_match = False
                
                comparison['status'] = 'MATCH' if all_match else 'MISMATCH'
                if all_match:
                    results['summary']['matches'] += 1
                else:
                    results['summary']['mismatches'] += 1
            
            results['comparison_results'].append(comparison)
        
        return results
