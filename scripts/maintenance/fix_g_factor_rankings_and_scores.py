#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复G因子排名和评分脚本
根据数据库中已计算的子因子结果，重新计算rank和g_score

使用方法：
python scripts/maintenance/fix_g_factor_rankings_and_scores.py --start_date 2020-01-01 --end_date 2024-12-31
"""

import sys
import os
import argparse
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import List, Optional

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.tools.context import ResearchContext
from research.pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator


class GFactorRankingFixer:
    """G因子排名和评分修复器"""
    
    def __init__(self, context: ResearchContext):
        self.context = context
        self.calculator = ProductionGFactorCalculator(context)
        self.logger = self.calculator.logger
        
        # 子因子权重配置
        self.subfactor_weights = {
            'efficiency_surprise': 0.25,
            'efficiency_momentum': 0.25,
            'revenue_momentum': 0.25,
            'profit_momentum': 0.25
        }
    
    def calculate_dynamic_rankings_and_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算动态排名和评分
        
        Args:
            df: 包含子因子的DataFrame
            
        Returns:
            包含排名和评分的DataFrame
        """
        # 1. 计算排名（只对有效值进行排名）
        df['rank_es'] = df['g_efficiency_surprise'].rank(pct=True, na_option='keep') * 100
        df['rank_em'] = df['g_efficiency_momentum'].rank(pct=True, na_option='keep') * 100
        df['rank_rm'] = df['g_revenue_momentum'].rank(pct=True, na_option='keep') * 100
        df['rank_pm'] = df['g_profit_momentum'].rank(pct=True, na_option='keep') * 100
        
        # 2. 计算动态权重G评分
        # 检查各子因子是否有有效值（非空值）
        has_es = df['g_efficiency_surprise'].notna()
        has_em = df['g_efficiency_momentum'].notna()
        has_rm = df['g_revenue_momentum'].notna()
        has_pm = df['g_profit_momentum'].notna()
        
        # 计算动态权重
        w_es = self.subfactor_weights['efficiency_surprise']
        w_em = self.subfactor_weights['efficiency_momentum']
        w_rm = self.subfactor_weights['revenue_momentum']
        w_pm = self.subfactor_weights['profit_momentum']
        
        # 初始化结果Series
        g_score = pd.Series(index=df.index, dtype=float)
        
        # 逐行计算G评分，确保正确处理空值
        for idx in df.index:
            # 获取该行的有效因子信息
            row_has_es = has_es.loc[idx]
            row_has_em = has_em.loc[idx]
            row_has_rm = has_rm.loc[idx]
            row_has_pm = has_pm.loc[idx]
            
            # 计算有效因子的权重和
            total_weight = (
                w_es * row_has_es +
                w_em * row_has_em +
                w_rm * row_has_rm +
                w_pm * row_has_pm
            )
            
            # 如果没有任何有效因子，G评分为0
            if total_weight == 0:
                g_score.loc[idx] = 0.0
                continue
            
            # 计算加权排名和
            weighted_sum = 0.0
            if row_has_es and pd.notna(df.loc[idx, 'rank_es']):
                weighted_sum += df.loc[idx, 'rank_es'] * w_es
            if row_has_em and pd.notna(df.loc[idx, 'rank_em']):
                weighted_sum += df.loc[idx, 'rank_em'] * w_em
            if row_has_rm and pd.notna(df.loc[idx, 'rank_rm']):
                weighted_sum += df.loc[idx, 'rank_rm'] * w_rm
            if row_has_pm and pd.notna(df.loc[idx, 'rank_pm']):
                weighted_sum += df.loc[idx, 'rank_pm'] * w_pm
            
            # 计算最终G评分
            g_score.loc[idx] = weighted_sum / total_weight
        
        df['g_score'] = g_score
        
        return df
    
    def get_g_factor_data_by_date(self, calc_date: str) -> pd.DataFrame:
        """
        获取指定日期的G因子数据
        
        Args:
            calc_date: 计算日期
            
        Returns:
            G因子数据DataFrame
        """
        query = """
        SELECT 
            ts_code,
            calc_date,
            g_efficiency_surprise,
            g_efficiency_momentum,
            g_revenue_momentum,
            g_profit_momentum,
            rank_es,
            rank_em,
            rank_rm,
            rank_pm,
            g_score
        FROM pgs_factors.g_factor 
        WHERE calc_date = %s
        AND (g_efficiency_surprise IS NOT NULL 
             OR g_efficiency_momentum IS NOT NULL 
             OR g_revenue_momentum IS NOT NULL 
             OR g_profit_momentum IS NOT NULL)
        ORDER BY ts_code
        """
        
        try:
            results = self.context.db_manager.fetch_sync(query, (calc_date,))
            
            if not results:
                self.logger.warning(f"未找到G因子数据: {calc_date}")
                return pd.DataFrame()
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            self.logger.info(f"获取到 {len(df)} 条G因子数据: {calc_date}")
            return df
            
        except Exception as e:
            self.logger.error(f"获取G因子数据失败 {calc_date}: {e}")
            return pd.DataFrame()
    
    def update_g_factor_rankings_and_scores(self, df: pd.DataFrame, calc_date: str) -> int:
        """
        更新G因子排名和评分
        
        Args:
            df: 包含新排名和评分的DataFrame
            calc_date: 计算日期
            
        Returns:
            成功更新的记录数
        """
        if df.empty:
            return 0
        
        try:
            # 准备更新数据
            update_data = []
            for _, row in df.iterrows():
                update_data.append((
                    row['rank_es'],
                    row['rank_em'],
                    row['rank_rm'],
                    row['rank_pm'],
                    row['g_score'],
                    row['ts_code'],
                    calc_date
                ))
            
            # 批量更新
            update_query = """
            UPDATE pgs_factors.g_factor 
            SET 
                rank_es = %s,
                rank_em = %s,
                rank_rm = %s,
                rank_pm = %s,
                g_score = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE ts_code = %s AND calc_date = %s
            """
            
            success_count = 0
            for data_tuple in update_data:
                try:
                    self.context.db_manager.execute_sync(update_query, data_tuple)
                    success_count += 1
                except Exception as e:
                    self.logger.warning(f"更新失败 {data_tuple[5]} {calc_date}: {e}")
            
            self.logger.info(f"成功更新 {success_count}/{len(update_data)} 条记录: {calc_date}")
            return success_count
            
        except Exception as e:
            self.logger.error(f"更新G因子排名和评分失败 {calc_date}: {e}")
            return 0
    
    def fix_rankings_and_scores_for_date(self, calc_date: str) -> dict:
        """
        修复指定日期的排名和评分
        
        Args:
            calc_date: 计算日期
            
        Returns:
            修复结果统计
        """
        start_time = time.time()
        
        # 1. 获取数据
        df = self.get_g_factor_data_by_date(calc_date)
        if df.empty:
            return {
                'calc_date': calc_date,
                'total_records': 0,
                'updated_records': 0,
                'processing_time': 0,
                'status': 'no_data'
            }
        
        # 2. 计算新的排名和评分
        df_fixed = self.calculate_dynamic_rankings_and_scores(df.copy())
        
        # 3. 更新数据库
        updated_count = self.update_g_factor_rankings_and_scores(df_fixed, calc_date)
        
        processing_time = time.time() - start_time
        
        return {
            'calc_date': calc_date,
            'total_records': len(df),
            'updated_records': updated_count,
            'processing_time': processing_time,
            'status': 'success' if updated_count > 0 else 'failed'
        }
    
    def fix_rankings_and_scores_batch(self, start_date: str, end_date: str) -> dict:
        """
        批量修复排名和评分
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            批量修复结果统计
        """
        self.logger.info(f"开始批量修复G因子排名和评分: {start_date} ~ {end_date}")
        
        # 获取需要修复的日期列表
        date_query = """
        SELECT DISTINCT calc_date 
        FROM pgs_factors.g_factor 
        WHERE calc_date >= %s AND calc_date <= %s
        AND (g_efficiency_surprise IS NOT NULL 
             OR g_efficiency_momentum IS NOT NULL 
             OR g_revenue_momentum IS NOT NULL 
             OR g_profit_momentum IS NOT NULL)
        ORDER BY calc_date
        """
        
        try:
            date_results = self.context.db_manager.fetch_sync(date_query, (start_date, end_date))
            
            # 调试信息
            self.logger.info(f"数据库查询结果类型: {type(date_results)}")
            self.logger.info(f"数据库查询结果: {date_results}")
            
            if not date_results:
                self.logger.warning(f"在指定日期范围内未找到G因子数据: {start_date} ~ {end_date}")
                return {
                    'status': 'no_data',
                    'total_dates': 0,
                    'total_records': 0,
                    'total_updated': 0,
                    'total_time': 0,
                    'results': []
                }
            
            # 处理查询结果
            if isinstance(date_results, list):
                if len(date_results) > 0 and isinstance(date_results[0], (list, tuple)):
                    # 结果格式: [(date1,), (date2,), ...]
                    calc_dates = [row[0] for row in date_results]
                elif len(date_results) > 0 and isinstance(date_results[0], dict):
                    # 结果格式: [{'calc_date': date1}, {'calc_date': date2}, ...]
                    calc_dates = [row['calc_date'] for row in date_results]
                else:
                    # 结果格式: [date1, date2, ...]
                    calc_dates = date_results
            else:
                self.logger.error(f"意外的查询结果格式: {type(date_results)}")
                return {'status': 'failed', 'error': f'意外的查询结果格式: {type(date_results)}'}
            
            self.logger.info(f"找到 {len(calc_dates)} 个需要修复的计算日期")
            if len(calc_dates) > 0:
                self.logger.info(f"日期范围: {min(calc_dates)} ~ {max(calc_dates)}")
            
        except Exception as e:
            self.logger.error(f"获取计算日期失败: {e}")
            return {'status': 'failed', 'error': str(e)}
        
        # 批量处理
        results = []
        total_records = 0
        total_updated = 0
        total_time = 0
        
        for i, calc_date in enumerate(calc_dates, 1):
            self.logger.info(f"处理进度: {i}/{len(calc_dates)} - {calc_date}")
            
            result = self.fix_rankings_and_scores_for_date(calc_date)
            results.append(result)
            
            total_records += result['total_records']
            total_updated += result['updated_records']
            total_time += result['processing_time']
            
            # 每处理10个日期输出一次进度
            if i % 10 == 0:
                self.logger.info(f"已处理 {i}/{len(calc_dates)} 个日期，更新 {total_updated} 条记录")
        
        return {
            'status': 'completed',
            'total_dates': len(calc_dates),
            'total_records': total_records,
            'total_updated': total_updated,
            'total_time': total_time,
            'results': results
        }


def main():
    parser = argparse.ArgumentParser(description='修复G因子排名和评分')
    parser.add_argument('--start_date', type=str, required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--single_date', type=str, help='单日修复 (YYYY-MM-DD)')
    parser.add_argument('--dry_run', action='store_true', help='试运行模式，不实际更新数据库')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.single_date:
        if args.start_date or args.end_date:
            print("❌ 单日修复模式不能同时指定日期范围")
            sys.exit(1)
    else:
        if not args.start_date or not args.end_date:
            print("❌ 批量修复模式必须指定开始和结束日期")
            sys.exit(1)
    
    print("🔧 G因子排名和评分修复工具")
    print("=" * 50)
    print(f"🕐 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.single_date:
        print(f"📅 修复模式: 单日修复")
        print(f"📅 目标日期: {args.single_date}")
    else:
        print(f"📅 修复模式: 批量修复")
        print(f"📅 日期范围: {args.start_date} ~ {args.end_date}")
    
    if args.dry_run:
        print("⚠️ 试运行模式: 不会实际更新数据库")
    
    print()
    
    # 初始化研究上下文
    try:
        context = ResearchContext()
        print("✅ 研究上下文初始化成功")
    except Exception as e:
        print(f"❌ 研究上下文初始化失败: {e}")
        sys.exit(1)
    
    # 创建修复器
    fixer = GFactorRankingFixer(context)
    
    try:
        if args.single_date:
            # 单日修复
            result = fixer.fix_rankings_and_scores_for_date(args.single_date)
            
            print(f"\n🎉 单日修复完成!")
            print(f"📅 日期: {result['calc_date']}")
            print(f"📊 总记录数: {result['total_records']}")
            print(f"✅ 更新记录数: {result['updated_records']}")
            print(f"⏰ 处理时间: {result['processing_time']:.2f}秒")
            print(f"📈 状态: {result['status']}")
            
        else:
            # 批量修复
            result = fixer.fix_rankings_and_scores_batch(args.start_date, args.end_date)
            
            print(f"\n🎉 批量修复完成!")
            print(f"📅 处理日期数: {result['total_dates']}")
            print(f"📊 总记录数: {result['total_records']}")
            print(f"✅ 更新记录数: {result['total_updated']}")
            print(f"⏰ 总处理时间: {result['total_time']:.2f}秒")
            print(f"📈 状态: {result['status']}")
            
            if result['total_records'] > 0:
                success_rate = result['total_updated'] / result['total_records'] * 100
                print(f"📊 成功率: {success_rate:.1f}%")
    
    except Exception as e:
        print(f"❌ 修复过程失败: {e}")
        sys.exit(1)
    
    print(f"\n🕐 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
