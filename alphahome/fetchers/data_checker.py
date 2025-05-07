#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
通用数据质量检查模块

提供对任何数据表的质量检查功能，无需为每个表创建特定的检查器。
"""

import asyncio
import logging
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
from matplotlib.dates import DateFormatter

from .task_factory import TaskFactory
from .tools.calendar import get_trade_cal

class DataQualityChecker:
    """通用数据质量检查器
    
    可以对任何表进行数据质量检查，不需要为每个表创建特定的检查器。
    """

    def __init__(self):
        """初始化检查器"""
        self.logger = logging.getLogger('quality_checks.data_checker')
        
    async def _discover_table_structure(self, table_name, db_manager):
        """发现表结构
        
        自动发现表的主键、日期列和其他重要特征
        
        Args:
            table_name: 表名
            db_manager: 数据库管理器实例
            
        Returns:
            dict: 表结构信息
        """
        # 查询表的列信息
        columns_query = f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        """
        columns = await db_manager.fetch(columns_query)
        
        # 查询主键信息
        pk_query = f"""
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{table_name}'::regclass
        AND    i.indisprimary
        """
        primary_keys = await db_manager.fetch(pk_query)
        primary_key_columns = [row[0] for row in primary_keys]
        
        # 自动发现日期列
        date_columns = []
        for col, dtype in columns:
            if dtype in ('date', 'timestamp', 'timestamp without time zone'):
                date_columns.append(col)
            elif col in ('trade_date', 'ann_date', 'report_date') or 'date' in col.lower():
                date_columns.append(col)
        
        return {
            'columns': {col: dtype for col, dtype in columns},
            'primary_keys': primary_key_columns,
            'date_columns': date_columns
        }
    
    async def _check_date_coverage(self, table_name, date_column, structure, db_manager, start_date, end_date):
        """检查日期覆盖率
        
        检查指定日期范围内的数据覆盖情况
        
        Args:
            table_name: 表名
            date_column: 日期列名
            structure: 表结构信息
            db_manager: 数据库管理器
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            
        Returns:
            dict: 日期覆盖检查结果
        """
        # 获取交易日历
        calendar_df = await get_trade_cal(start_date=start_date, end_date=end_date)
        trade_dates = calendar_df[calendar_df['is_open'] == 1]['cal_date'].tolist()
        
        # 查询每个交易日的记录数
        date_coverage = []
        for trade_date in trade_dates:
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {date_column} = '{trade_date}'"
            count = await db_manager.fetch_val(query)
            date_coverage.append({
                'date': trade_date,
                'record_count': count
            })
        
        # 计算覆盖率统计
        df = pd.DataFrame(date_coverage)
        total_dates = len(df)
        empty_dates = (df['record_count'] == 0).sum()
        
        return {
            'total_dates': total_dates,
            'covered_dates': total_dates - empty_dates,
            'empty_dates': empty_dates,
            'coverage_ratio': (total_dates - empty_dates) / total_dates if total_dates > 0 else 0,
            'date_details': date_coverage
        }
    
    async def _check_data_completeness(self, table_name, structure, db_manager):
        """检查数据完整性
        
        检查表中的空值情况
        
        Args:
            table_name: 表名
            structure: 表结构信息
            db_manager: 数据库管理器
            
        Returns:
            dict: 完整性检查结果
        """
        # 计算总记录数
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        total_records = await db_manager.fetch_val(count_query)
        
        # 检查每列的空值数量
        null_counts = []
        for column in structure['columns'].keys():
            null_query = f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL"
            null_count = await db_manager.fetch_val(null_query)
            null_ratio = null_count / total_records if total_records > 0 else 0
            null_counts.append({
                'column': column,
                'null_count': null_count,
                'null_ratio': null_ratio
            })
        
        return {
            'total_records': total_records,
            'column_nulls': sorted(null_counts, key=lambda x: x['null_ratio'], reverse=True)
        }
    
    async def _analyze_monthly_coverage(self, date_coverage):
        """按月分析数据覆盖情况
        
        Args:
            date_coverage: 日期覆盖检查结果
            
        Returns:
            list: 月度覆盖率分析
        """
        # 将日期转换为年月格式
        for item in date_coverage:
            date_str = item['date']
            item['year_month'] = date_str[:6]  # YYYYMM
            
        # 按月分组统计
        month_stats = {}
        for item in date_coverage:
            year_month = item['year_month']
            if year_month not in month_stats:
                month_stats[year_month] = {'total': 0, 'covered': 0}
            
            month_stats[year_month]['total'] += 1
            if item['record_count'] > 0:
                month_stats[year_month]['covered'] += 1
        
        # 计算每月覆盖率
        monthly_analysis = []
        for year_month, stats in month_stats.items():
            coverage_ratio = stats['covered'] / stats['total'] if stats['total'] > 0 else 0
            formatted_month = f"{year_month[:4]}-{year_month[4:6]}"
            monthly_analysis.append({
                'month': formatted_month,
                'total_days': stats['total'],
                'covered_days': stats['covered'],
                'coverage_ratio': coverage_ratio
            })
            
        return sorted(monthly_analysis, key=lambda x: x['month'])
    
    def _plot_date_coverage(self, date_coverage, table_name, filename=None):
        """绘制日期覆盖率图表
        
        Args:
            date_coverage: 日期覆盖检查结果
            table_name: 表名
            filename: 输出文件名（可选）
            
        Returns:
            str: 图表文件路径
        """
        if filename is None:
            filename = f"{table_name}_date_coverage.png"
            
        dates = [datetime.strptime(item['date'], '%Y%m%d') for item in date_coverage]
        counts = [item['record_count'] for item in date_coverage]
        
        plt.figure(figsize=(12, 6))
        plt.bar(dates, counts)
        plt.title(f'数据日期覆盖情况 - {table_name}')
        plt.xlabel('日期')
        plt.ylabel('记录数')
        plt.grid(True, axis='y')
        
        if max(dates) - min(dates) > timedelta(days=365*2):
            plt.gca().xaxis.set_major_formatter(DateFormatter('%Y-%m'))
        else:
            plt.gca().xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        
        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()
        
        return filename
        
    def _plot_monthly_coverage(self, monthly_analysis, table_name, filename=None):
        """绘制月度覆盖率图表
        
        Args:
            monthly_analysis: 月度覆盖率分析
            table_name: 表名
            filename: 输出文件名（可选）
            
        Returns:
            str: 图表文件路径
        """
        if filename is None:
            filename = f"{table_name}_monthly_coverage.png"
            
        months = [item['month'] for item in monthly_analysis]
        ratios = [item['coverage_ratio'] * 100 for item in monthly_analysis]  # 转为百分比
        
        plt.figure(figsize=(12, 6))
        plt.bar(months, ratios)
        plt.title(f'月度数据覆盖率 - {table_name}')
        plt.xlabel('月份')
        plt.ylabel('覆盖率 (%)')
        plt.ylim(0, 105)  # 稍微超出100%，便于查看
        plt.grid(True, axis='y')
        
        # 旋转x轴标签，避免重叠
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()
        
        return filename
    
    async def run_check(self, table_name, start_date=None, end_date=None):
        """执行数据质量检查
        
        Args:
            table_name: 要检查的表名
            start_date: 开始日期（YYYYMMDD格式，可选）
            end_date: 结束日期（YYYYMMDD格式，可选）
            
        Returns:
            dict: 检查结果
        """
        self.logger.info(f"开始对表 {table_name} 进行数据质量检查...")
        
        # 默认日期范围
        if not start_date:
            start_date = "20150101"  # 默认从2015年开始
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            
        # 初始化数据库连接
        await TaskFactory.initialize()
        db_manager = TaskFactory.get_db_manager()
        
        try:
            # 发现表结构
            self.logger.info(f"正在分析表 {table_name} 的结构...")
            structure = await self._discover_table_structure(table_name, db_manager)
            
            # 选择合适的日期列
            date_column = None
            if structure['date_columns']:
                date_column = structure['date_columns'][0]
                self.logger.info(f"自动选择日期列: {date_column}")
            
            results = {
                'table_name': table_name,
                'structure': structure,
                'start_date': start_date,
                'end_date': end_date,
                'checks': {}
            }
            
            # 检查数据完整性
            self.logger.info("检查数据完整性...")
            completeness = await self._check_data_completeness(table_name, structure, db_manager)
            results['checks']['completeness'] = completeness
            
            # 如果有日期列，检查日期覆盖率
            if date_column:
                self.logger.info(f"使用列 {date_column} 检查日期覆盖率...")
                date_coverage = await self._check_date_coverage(
                    table_name, date_column, structure, db_manager, start_date, end_date
                )
                results['checks']['date_coverage'] = date_coverage
                
                # 分析月度覆盖情况
                monthly_analysis = await self._analyze_monthly_coverage(date_coverage['date_details'])
                results['checks']['monthly_analysis'] = monthly_analysis
                
                # 生成图表
                charts = {}
                
                # 日期覆盖率图表
                date_chart = self._plot_date_coverage(
                    date_coverage['date_details'], 
                    table_name
                )
                charts['date_coverage'] = date_chart
                
                # 月度覆盖率图表
                monthly_chart = self._plot_monthly_coverage(
                    monthly_analysis,
                    table_name
                )
                charts['monthly_coverage'] = monthly_chart
                
                results['charts'] = charts
            
            # 生成摘要
            summary = {
                'total_records': completeness['total_records'],
                'columns_with_nulls': sum(1 for col in completeness['column_nulls'] if col['null_count'] > 0),
                'top_null_columns': [
                    {
                        'column': col['column'],
                        'null_ratio': col['null_ratio']
                    }
                    for col in completeness['column_nulls'][:5] if col['null_count'] > 0
                ]
            }
            
            if date_column:
                summary.update({
                    'date_coverage_ratio': date_coverage['coverage_ratio'],
                    'empty_dates': date_coverage['empty_dates'],
                    'total_dates': date_coverage['total_dates'],
                    'worst_months': sorted(
                        monthly_analysis, 
                        key=lambda x: x['coverage_ratio']
                    )[:3] if monthly_analysis else []
                })
                
            results['summary'] = summary
            
            self.logger.info("数据质量检查完成。")
            return results
            
        except Exception as e:
            self.logger.error(f"检查过程中发生错误: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'error_message': str(e)
            }
        finally:
            # 不关闭数据库连接，由调用方负责关闭
            pass 