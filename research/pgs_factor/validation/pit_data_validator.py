#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PIT数据库质量验证器
==================

对pgs_factors.pit_income_quarterly和pit_balance_quarterly表进行全面的数据质量验证，
重点验证单季化转换逻辑的正确性和conversion_status字段的可靠性。

主要功能：
1. 抽样验证：选择代表性个股进行深度验证
2. 财务比率计算：ROE、毛利率、净利率等关键指标
3. 年度汇总验证：四季度数据vs年报数据对比
4. 异常检测：识别数据质量问题并提供修复建议
5. 报告生成：输出详细的验证报告

使用示例：
    python -m research.pgs_factor.validation.pit_data_validator --mode full
    python -m research.pgs_factor.validation.pit_data_validator --stocks 000001.SZ,601318.SH --years 2022,2023
"""

import argparse
import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import json

from research.tools.context import ResearchContext


class PITDataValidator:
    """PIT数据质量验证器"""
    
    def __init__(self, ctx: ResearchContext):
        self.ctx = ctx
        self.logger = self._setup_logger()
        
        # 验证配置
        self.sample_stocks = [
            '000001.SZ',  # 平安银行 - 金融业，完整季报
            '601318.SH',  # 中国平安 - 保险业，完整季报
            '000002.SZ',  # 万科A - 房地产，中报+年报模式
            '600519.SH',  # 贵州茅台 - 消费品，完整季报
            '000858.SZ',  # 五粮液 - 消费品，不规则披露
        ]
        
        self.validation_years = [2020, 2021, 2022, 2023, 2024]
        self.acceptable_error_rate = 0.001  # 0.1%误差容忍
        self.acceptable_absolute_diff = 1000  # 1000元绝对差异容忍
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('PITDataValidator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def validate_all(self, stocks: Optional[List[str]] = None, 
                    years: Optional[List[int]] = None) -> Dict[str, Any]:
        """执行完整的数据质量验证"""
        
        stocks = stocks or self.sample_stocks
        years = years or self.validation_years
        
        self.logger.info(f"开始PIT数据质量验证...")
        self.logger.info(f"验证股票: {stocks}")
        self.logger.info(f"验证年份: {years}")
        
        results = {
            'validation_time': datetime.now().isoformat(),
            'sample_stocks': stocks,
            'validation_years': years,
            'summary': {},
            'detailed_results': {},
            'issues_found': [],
            'recommendations': []
        }
        
        # 1. 数据完整性验证
        self.logger.info("1. 执行数据完整性验证...")
        completeness_results = self._validate_data_completeness(stocks, years)
        results['detailed_results']['completeness'] = completeness_results
        
        # 2. conversion_status分布验证
        self.logger.info("2. 执行conversion_status分布验证...")
        status_results = self._validate_conversion_status(stocks, years)
        results['detailed_results']['conversion_status'] = status_results
        
        # 3. 单季化逻辑验证
        self.logger.info("3. 执行单季化逻辑验证...")
        seasonality_results = self._validate_seasonality_logic(stocks, years)
        results['detailed_results']['seasonality'] = seasonality_results
        
        # 4. 财务比率验证
        self.logger.info("4. 执行财务比率验证...")
        ratio_results = self._validate_financial_ratios(stocks, years)
        results['detailed_results']['financial_ratios'] = ratio_results
        
        # 5. 年度汇总验证
        self.logger.info("5. 执行年度汇总验证...")
        annual_results = self._validate_annual_aggregation(stocks, years)
        results['detailed_results']['annual_aggregation'] = annual_results
        
        # 6. 生成汇总报告
        results['summary'] = self._generate_summary(results['detailed_results'])
        
        self.logger.info("PIT数据质量验证完成!")
        return results
    
    def _validate_data_completeness(self, stocks: List[str], years: List[int]) -> Dict[str, Any]:
        """验证数据完整性"""
        
        query = """
        SELECT
            ts_code,
            EXTRACT(YEAR FROM end_date) as year,
            COUNT(*) as record_count,
            COUNT(DISTINCT quarter) as quarter_count,
            COUNT(DISTINCT data_source) as source_count,
            STRING_AGG(CAST(quarter AS TEXT), ',' ORDER BY quarter) as quarters,
            STRING_AGG(data_source, ',' ORDER BY data_source) as sources
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = ANY(%(stocks)s)
        AND EXTRACT(YEAR FROM end_date) = ANY(%(years)s)
        GROUP BY ts_code, EXTRACT(YEAR FROM end_date)
        ORDER BY ts_code, year
        """
        
        df = self.ctx.query_dataframe(query, {
            'stocks': stocks,
            'years': years
        })
        
        results = {
            'total_records': len(df) if df is not None else 0,
            'by_stock_year': df.to_dict('records') if df is not None else [],
            'missing_data': [],
            'irregular_patterns': []
        }
        
        if df is not None and not df.empty:
            # 检查缺失数据
            for year in years:
                for stock in stocks:
                    stock_year_data = df[(df['ts_code'] == stock) & (df['year'] == year)]
                    if stock_year_data.empty:
                        results['missing_data'].append({
                            'ts_code': stock,
                            'year': year,
                            'issue': 'No data found'
                        })
                    elif stock_year_data.iloc[0]['quarter_count'] < 2:
                        results['irregular_patterns'].append({
                            'ts_code': stock,
                            'year': year,
                            'quarters': stock_year_data.iloc[0]['quarters'],
                            'issue': 'Insufficient quarterly data'
                        })
        
        return results
    
    def _validate_conversion_status(self, stocks: List[str], years: List[int]) -> Dict[str, Any]:
        """验证conversion_status分布和合理性"""
        
        query = """
        SELECT 
            ts_code,
            EXTRACT(YEAR FROM end_date) as year,
            conversion_status,
            COUNT(*) as count,
            AVG(n_income_attr_p) as avg_n_income,
            MIN(n_income_attr_p) as min_n_income,
            MAX(n_income_attr_p) as max_n_income
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = ANY(%(stocks)s)
        AND EXTRACT(YEAR FROM end_date) = ANY(%(years)s)
        GROUP BY ts_code, EXTRACT(YEAR FROM end_date), conversion_status
        ORDER BY ts_code, year, conversion_status
        """
        
        df = self.ctx.query_dataframe(query, {
            'stocks': stocks,
            'years': years
        })
        
        results = {
            'status_distribution': {},
            'by_stock_year': df.to_dict('records') if df is not None else [],
            'anomalies': []
        }
        
        if df is not None and not df.empty:
            # 统计状态分布
            status_counts = df.groupby('conversion_status')['count'].sum().to_dict()
            total_records = sum(status_counts.values())
            
            results['status_distribution'] = {
                status: {
                    'count': count,
                    'percentage': round(count / total_records * 100, 2)
                }
                for status, count in status_counts.items()
            }
            
            # 检查异常情况
            for _, row in df.iterrows():
                # 检查CALCULATED状态是否合理
                if row['conversion_status'] == 'CALCULATED' and row['avg_n_income'] == 0:
                    results['anomalies'].append({
                        'ts_code': row['ts_code'],
                        'year': row['year'],
                        'issue': 'CALCULATED status with zero average income',
                        'status': row['conversion_status']
                    })
                
                # 检查SINGLE状态的数据质量
                if row['conversion_status'] == 'SINGLE' and abs(row['avg_n_income']) > 1e10:
                    results['anomalies'].append({
                        'ts_code': row['ts_code'],
                        'year': row['year'],
                        'issue': 'SINGLE status with extremely large values',
                        'avg_value': row['avg_n_income']
                    })
        
        return results

    def _validate_seasonality_logic(self, stocks: List[str], years: List[int]) -> Dict[str, Any]:
        """验证单季化逻辑的正确性"""

        # 获取详细的季度数据用于验证单季化逻辑
        query = """
        SELECT
            ts_code,
            end_date,
            EXTRACT(YEAR FROM end_date) as year,
            quarter,
            conversion_status,
            n_income_attr_p,
            revenue,
            operate_profit
        FROM pgs_factors.pit_income_quarterly
        WHERE ts_code = ANY(%(stocks)s)
        AND EXTRACT(YEAR FROM end_date) = ANY(%(years)s)
        AND data_source = 'report'
        ORDER BY ts_code, end_date
        """

        df = self.ctx.query_dataframe(query, {
            'stocks': stocks,
            'years': years
        })

        results = {
            'validation_cases': [],
            'logic_errors': [],
            'suspicious_patterns': []
        }

        if df is not None and not df.empty:
            # 按股票-年份分组验证
            for (ts_code, year), group in df.groupby(['ts_code', 'year']):
                group = group.sort_values('quarter')
                quarters = group['quarter'].tolist()

                case = {
                    'ts_code': ts_code,
                    'year': year,
                    'quarters': quarters,
                    'status_pattern': group['conversion_status'].tolist(),
                    'validation_result': 'PASS'
                }

                # 验证不同季报模式的逻辑
                if quarters == [4]:  # 仅年报
                    expected_status = ['ANNUAL']
                    if group['conversion_status'].tolist() != expected_status:
                        case['validation_result'] = 'FAIL'
                        results['logic_errors'].append({
                            'ts_code': ts_code,
                            'year': year,
                            'issue': f'Year-only pattern should be ANNUAL, got {group["conversion_status"].tolist()}'
                        })

                elif quarters == [2, 4]:  # 中报+年报
                    expected_status = ['CUMULATIVE', 'CALCULATED']
                    if group['conversion_status'].tolist() != expected_status:
                        case['validation_result'] = 'FAIL'
                        results['logic_errors'].append({
                            'ts_code': ts_code,
                            'year': year,
                            'issue': f'Mid+Annual pattern should be [CUMULATIVE,CALCULATED], got {group["conversion_status"].tolist()}'
                        })

                    # 验证计算逻辑：Q4应该 = 年报 - 中报
                    if len(group) == 2:
                        q2_value = group[group['quarter'] == 2]['n_income_attr_p'].iloc[0]
                        q4_value = group[group['quarter'] == 4]['n_income_attr_p'].iloc[0]

                        # 检查是否存在明显的逻辑错误（如Q4 + Q2 != 年报累计）
                        if abs(q4_value) > abs(q2_value) * 10:  # Q4单季不应该是Q2的10倍以上
                            results['suspicious_patterns'].append({
                                'ts_code': ts_code,
                                'year': year,
                                'issue': f'Q4 calculated value ({q4_value:.0f}) seems too large compared to Q2 ({q2_value:.0f})'
                            })

                elif len(quarters) >= 3:  # 完整季报
                    # 大部分应该是SINGLE
                    single_count = sum(1 for status in group['conversion_status'] if status == 'SINGLE')
                    if single_count < len(quarters) * 0.5:  # 至少一半应该是SINGLE
                        results['suspicious_patterns'].append({
                            'ts_code': ts_code,
                            'year': year,
                            'issue': f'Full quarterly data but only {single_count}/{len(quarters)} marked as SINGLE'
                        })

                results['validation_cases'].append(case)

        return results

    def _validate_financial_ratios(self, stocks: List[str], years: List[int]) -> Dict[str, Any]:
        """验证财务比率的合理性"""

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
            b.tot_equity
        FROM pgs_factors.pit_income_quarterly i
        LEFT JOIN pgs_factors.pit_balance_quarterly b
            ON i.ts_code = b.ts_code
            AND i.end_date = b.end_date
            AND i.ann_date = b.ann_date
        WHERE i.ts_code = ANY(%(stocks)s)
        AND EXTRACT(YEAR FROM i.end_date) = ANY(%(years)s)
        AND i.data_source = 'report'
        AND i.conversion_status IN ('SINGLE', 'CALCULATED')
        ORDER BY i.ts_code, i.end_date
        """

        df = self.ctx.query_dataframe(query, {
            'stocks': stocks,
            'years': years
        })

        results = {
            'ratio_analysis': [],
            'outliers': [],
            'ratio_trends': {}
        }

        if df is not None and not df.empty:
            # 计算财务比率
            df['roe'] = np.where(df['tot_equity'] != 0, df['n_income_attr_p'] / df['tot_equity'], np.nan)
            df['gross_margin'] = np.where(df['revenue'] != 0, (df['revenue'] - df['oper_cost']) / df['revenue'], np.nan)
            df['net_margin'] = np.where(df['revenue'] != 0, df['n_income_attr_p'] / df['revenue'], np.nan)

            # 按股票分析比率
            for ts_code in stocks:
                stock_data = df[df['ts_code'] == ts_code].copy()
                if stock_data.empty:
                    continue

                stock_analysis = {
                    'ts_code': ts_code,
                    'quarters_analyzed': len(stock_data),
                    'avg_roe': stock_data['roe'].mean() if not stock_data['roe'].isna().all() else None,
                    'avg_gross_margin': stock_data['gross_margin'].mean() if not stock_data['gross_margin'].isna().all() else None,
                    'avg_net_margin': stock_data['net_margin'].mean() if not stock_data['net_margin'].isna().all() else None,
                    'roe_volatility': stock_data['roe'].std() if not stock_data['roe'].isna().all() else None
                }

                results['ratio_analysis'].append(stock_analysis)

                # 检查异常值
                for _, row in stock_data.iterrows():
                    # ROE异常检查
                    if not pd.isna(row['roe']) and abs(row['roe']) > 1:  # ROE > 100%
                        results['outliers'].append({
                            'ts_code': ts_code,
                            'end_date': row['end_date'].strftime('%Y-%m-%d'),
                            'metric': 'ROE',
                            'value': row['roe'],
                            'issue': 'ROE exceeds 100%'
                        })

                    # 毛利率异常检查
                    if not pd.isna(row['gross_margin']) and (row['gross_margin'] < -0.5 or row['gross_margin'] > 1.5):
                        results['outliers'].append({
                            'ts_code': ts_code,
                            'end_date': row['end_date'].strftime('%Y-%m-%d'),
                            'metric': 'Gross Margin',
                            'value': row['gross_margin'],
                            'issue': 'Gross margin outside reasonable range (-50%, 150%)'
                        })

        return results

    def _validate_annual_aggregation(self, stocks: List[str], years: List[int]) -> Dict[str, Any]:
        """验证年度汇总数据的一致性"""

        # 获取季度数据和对应的年报数据进行对比
        query = """
        WITH quarterly_sum AS (
            SELECT
                ts_code,
                EXTRACT(YEAR FROM end_date) as year,
                SUM(CASE WHEN conversion_status IN ('SINGLE', 'CALCULATED') THEN n_income_attr_p ELSE 0 END) as q_sum_n_income,
                SUM(CASE WHEN conversion_status IN ('SINGLE', 'CALCULATED') THEN revenue ELSE 0 END) as q_sum_revenue,
                COUNT(CASE WHEN conversion_status IN ('SINGLE', 'CALCULATED') THEN 1 END) as valid_quarters
            FROM pgs_factors.pit_income_quarterly
            WHERE ts_code = ANY(%(stocks)s)
            AND EXTRACT(YEAR FROM end_date) = ANY(%(years)s)
            AND data_source = 'report'
            GROUP BY ts_code, EXTRACT(YEAR FROM end_date)
        ),
        annual_data AS (
            SELECT
                ts_code,
                EXTRACT(YEAR FROM end_date) as year,
                n_income_attr_p as annual_n_income,
                revenue as annual_revenue
            FROM pgs_factors.pit_income_quarterly
            WHERE ts_code = ANY(%(stocks)s)
            AND EXTRACT(YEAR FROM end_date) = ANY(%(years)s)
            AND quarter = 4
            AND data_source = 'report'
            AND conversion_status IN ('ANNUAL', 'CALCULATED')
        )
        SELECT
            q.ts_code,
            q.year,
            q.q_sum_n_income,
            q.q_sum_revenue,
            q.valid_quarters,
            a.annual_n_income,
            a.annual_revenue,
            ABS(q.q_sum_n_income - COALESCE(a.annual_n_income, 0)) as n_income_diff,
            ABS(q.q_sum_revenue - COALESCE(a.annual_revenue, 0)) as revenue_diff
        FROM quarterly_sum q
        LEFT JOIN annual_data a ON q.ts_code = a.ts_code AND q.year = a.year
        ORDER BY q.ts_code, q.year
        """

        df = self.ctx.query_dataframe(query, {
            'stocks': stocks,
            'years': years
        })

        results = {
            'aggregation_checks': [],
            'significant_differences': [],
            'summary_stats': {}
        }

        if df is not None and not df.empty:
            for _, row in df.iterrows():
                check = {
                    'ts_code': row['ts_code'],
                    'year': row['year'],
                    'quarterly_sum_n_income': row['q_sum_n_income'],
                    'annual_n_income': row['annual_n_income'],
                    'quarterly_sum_revenue': row['q_sum_revenue'],
                    'annual_revenue': row['annual_revenue'],
                    'valid_quarters': row['valid_quarters'],
                    'n_income_diff': row['n_income_diff'],
                    'revenue_diff': row['revenue_diff']
                }

                # 计算相对差异
                if row['annual_n_income'] and row['annual_n_income'] != 0:
                    n_income_rel_diff = abs(row['n_income_diff'] / row['annual_n_income'])
                    check['n_income_rel_diff'] = n_income_rel_diff

                    # 检查显著差异
                    if n_income_rel_diff > self.acceptable_error_rate and row['n_income_diff'] > self.acceptable_absolute_diff:
                        results['significant_differences'].append({
                            'ts_code': row['ts_code'],
                            'year': row['year'],
                            'metric': 'n_income_attr_p',
                            'quarterly_sum': row['q_sum_n_income'],
                            'annual_value': row['annual_n_income'],
                            'absolute_diff': row['n_income_diff'],
                            'relative_diff': n_income_rel_diff,
                            'issue': 'Quarterly sum does not match annual value'
                        })

                if row['annual_revenue'] and row['annual_revenue'] != 0:
                    revenue_rel_diff = abs(row['revenue_diff'] / row['annual_revenue'])
                    check['revenue_rel_diff'] = revenue_rel_diff

                    if revenue_rel_diff > self.acceptable_error_rate and row['revenue_diff'] > self.acceptable_absolute_diff:
                        results['significant_differences'].append({
                            'ts_code': row['ts_code'],
                            'year': row['year'],
                            'metric': 'revenue',
                            'quarterly_sum': row['q_sum_revenue'],
                            'annual_value': row['annual_revenue'],
                            'absolute_diff': row['revenue_diff'],
                            'relative_diff': revenue_rel_diff,
                            'issue': 'Quarterly sum does not match annual value'
                        })

                results['aggregation_checks'].append(check)

            # 汇总统计
            results['summary_stats'] = {
                'total_checks': len(df),
                'significant_differences_count': len(results['significant_differences']),
                'avg_valid_quarters': df['valid_quarters'].mean(),
                'companies_with_issues': len(set(item['ts_code'] for item in results['significant_differences']))
            }

        return results

    def _generate_summary(self, detailed_results: Dict[str, Any]) -> Dict[str, Any]:
        """生成验证结果汇总"""

        summary = {
            'overall_status': 'PASS',
            'total_issues': 0,
            'critical_issues': 0,
            'warnings': 0,
            'data_quality_score': 100.0
        }

        # 统计各类问题
        issues = []

        # 数据完整性问题
        completeness = detailed_results.get('completeness', {})
        missing_count = len(completeness.get('missing_data', []))
        irregular_count = len(completeness.get('irregular_patterns', []))

        if missing_count > 0:
            issues.append(f"{missing_count} missing data cases")
            summary['critical_issues'] += missing_count

        if irregular_count > 0:
            issues.append(f"{irregular_count} irregular data patterns")
            summary['warnings'] += irregular_count

        # conversion_status问题
        status_results = detailed_results.get('conversion_status', {})
        status_anomalies = len(status_results.get('anomalies', []))

        if status_anomalies > 0:
            issues.append(f"{status_anomalies} conversion_status anomalies")
            summary['warnings'] += status_anomalies

        # 单季化逻辑问题
        seasonality = detailed_results.get('seasonality', {})
        logic_errors = len(seasonality.get('logic_errors', []))
        suspicious_patterns = len(seasonality.get('suspicious_patterns', []))

        if logic_errors > 0:
            issues.append(f"{logic_errors} seasonality logic errors")
            summary['critical_issues'] += logic_errors

        if suspicious_patterns > 0:
            issues.append(f"{suspicious_patterns} suspicious seasonality patterns")
            summary['warnings'] += suspicious_patterns

        # 财务比率问题
        ratios = detailed_results.get('financial_ratios', {})
        ratio_outliers = len(ratios.get('outliers', []))

        if ratio_outliers > 0:
            issues.append(f"{ratio_outliers} financial ratio outliers")
            summary['warnings'] += ratio_outliers

        # 年度汇总问题
        annual = detailed_results.get('annual_aggregation', {})
        aggregation_issues = len(annual.get('significant_differences', []))

        if aggregation_issues > 0:
            issues.append(f"{aggregation_issues} annual aggregation mismatches")
            summary['critical_issues'] += aggregation_issues

        # 计算总体状态和评分
        summary['total_issues'] = summary['critical_issues'] + summary['warnings']

        if summary['critical_issues'] > 0:
            summary['overall_status'] = 'FAIL'
            summary['data_quality_score'] = max(0, 100 - summary['critical_issues'] * 10 - summary['warnings'] * 2)
        elif summary['warnings'] > 0:
            summary['overall_status'] = 'WARNING'
            summary['data_quality_score'] = max(70, 100 - summary['warnings'] * 3)

        summary['issues_summary'] = issues

        return summary

    def generate_report(self, results: Dict[str, Any], output_path: Optional[str] = None) -> str:
        """生成验证报告"""

        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("PIT数据库质量验证报告")
        report_lines.append("=" * 80)
        report_lines.append(f"验证时间: {results['validation_time']}")
        report_lines.append(f"验证股票: {', '.join(results['sample_stocks'])}")
        report_lines.append(f"验证年份: {', '.join(map(str, results['validation_years']))}")
        report_lines.append("")

        # 汇总结果
        summary = results['summary']
        report_lines.append("验证结果汇总")
        report_lines.append("-" * 40)
        report_lines.append(f"总体状态: {summary['overall_status']}")
        report_lines.append(f"数据质量评分: {summary['data_quality_score']:.1f}/100")
        report_lines.append(f"总问题数: {summary['total_issues']}")
        report_lines.append(f"  - 严重问题: {summary['critical_issues']}")
        report_lines.append(f"  - 警告: {summary['warnings']}")

        if summary['issues_summary']:
            report_lines.append("\n问题概览:")
            for issue in summary['issues_summary']:
                report_lines.append(f"  • {issue}")

        report_lines.append("")

        # 详细结果
        detailed = results['detailed_results']

        # 数据完整性
        if 'completeness' in detailed:
            comp = detailed['completeness']
            report_lines.append("1. 数据完整性验证")
            report_lines.append("-" * 40)
            report_lines.append(f"总记录数: {comp['total_records']}")

            if comp['missing_data']:
                report_lines.append("\n缺失数据:")
                for item in comp['missing_data'][:5]:  # 只显示前5个
                    report_lines.append(f"  • {item['ts_code']} {item['year']}: {item['issue']}")

            if comp['irregular_patterns']:
                report_lines.append("\n不规则模式:")
                for item in comp['irregular_patterns'][:5]:
                    report_lines.append(f"  • {item['ts_code']} {item['year']}: {item['issue']} (quarters: {item['quarters']})")

            report_lines.append("")

        # conversion_status分布
        if 'conversion_status' in detailed:
            status = detailed['conversion_status']
            report_lines.append("2. Conversion Status分布")
            report_lines.append("-" * 40)

            if status['status_distribution']:
                for status_type, stats in status['status_distribution'].items():
                    report_lines.append(f"  {status_type}: {stats['count']} ({stats['percentage']}%)")

            if status['anomalies']:
                report_lines.append("\n状态异常:")
                for item in status['anomalies'][:5]:
                    report_lines.append(f"  • {item['ts_code']} {item['year']}: {item['issue']}")

            report_lines.append("")

        # 保存报告
        report_content = "\n".join(report_lines)

        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            self.logger.info(f"验证报告已保存到: {output_path}")

        return report_content


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PIT数据库质量验证器')
    parser.add_argument('--mode', choices=['full', 'sample', 'custom'], default='sample',
                       help='验证模式: full(全量), sample(样本), custom(自定义)')
    parser.add_argument('--stocks', type=str, help='股票代码列表，逗号分隔 (如: 000001.SZ,601318.SH)')
    parser.add_argument('--years', type=str, help='验证年份列表，逗号分隔 (如: 2022,2023,2024)')
    parser.add_argument('--output', type=str, help='报告输出路径')
    parser.add_argument('--json', action='store_true', help='同时输出JSON格式结果')

    args = parser.parse_args()

    # 解析参数
    stocks = None
    if args.stocks:
        stocks = [s.strip() for s in args.stocks.split(',')]

    years = None
    if args.years:
        years = [int(y.strip()) for y in args.years.split(',')]

    # 创建验证器并执行验证
    with ResearchContext() as ctx:
        validator = PITDataValidator(ctx)

        if args.mode == 'full':
            # 全量验证 - 获取所有股票
            all_stocks_query = """
            SELECT DISTINCT ts_code
            FROM pgs_factors.pit_income_quarterly
            ORDER BY ts_code
            LIMIT 50
            """  # 限制50只股票避免过长时间
            df = ctx.query_dataframe(all_stocks_query)
            if df is not None and not df.empty:
                stocks = df['ts_code'].tolist()
            else:
                stocks = validator.sample_stocks
        elif args.mode == 'sample':
            stocks = stocks or validator.sample_stocks

        years = years or validator.validation_years

        # 执行验证
        results = validator.validate_all(stocks, years)

        # 生成报告
        output_path = args.output or f"pit_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report = validator.generate_report(results, output_path)

        # 输出到控制台
        print(report)

        # 输出JSON结果
        if args.json:
            json_path = output_path.replace('.txt', '.json')
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            print(f"\nJSON结果已保存到: {json_path}")

        # 输出验证结论
        summary = results['summary']
        print(f"\n{'='*60}")
        print(f"验证结论: {summary['overall_status']}")
        print(f"数据质量评分: {summary['data_quality_score']:.1f}/100")

        if summary['critical_issues'] > 0:
            print(f"⚠️  发现 {summary['critical_issues']} 个严重问题，需要立即修复")
        elif summary['warnings'] > 0:
            print(f"⚠️  发现 {summary['warnings']} 个警告，建议关注")
        else:
            print("✅ 数据质量良好，未发现严重问题")


if __name__ == '__main__':
    main()
