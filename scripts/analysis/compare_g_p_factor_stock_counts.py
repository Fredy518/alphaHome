#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子和P因子股票数量对比分析脚本
比较每个calc_date的股票数量是否对应，验证数据一致性

使用方法：
python scripts/analysis/compare_g_p_factor_stock_counts.py --start_year 2010 --end_year 2024
"""

import sys
import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.tools.context import ResearchContext

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def get_g_factor_stock_counts(context, start_year: int, end_year: int):
    """获取G因子各日期的股票数量"""
    print("📊 获取G因子股票数量数据...")
    
    query = """
    SELECT 
        calc_date,
        EXTRACT(YEAR FROM calc_date) as year,
        COUNT(DISTINCT ts_code) as stock_count,
        COUNT(*) as total_records,
        COUNT(CASE WHEN calculation_status = 'success' THEN 1 END) as success_records,
        COUNT(CASE WHEN calculation_status = 'failed' THEN 1 END) as failed_records
    FROM pgs_factors.g_factor 
    WHERE EXTRACT(YEAR FROM calc_date) BETWEEN %s AND %s
    GROUP BY calc_date, EXTRACT(YEAR FROM calc_date)
    ORDER BY calc_date
    """
    
    try:
        results = context.db_manager.fetch_sync(query, (start_year, end_year))
        
        if not results:
            print("❌ 未找到G因子数据")
            return None
        
        df = pd.DataFrame(results, columns=[
            'calc_date', 'year', 'stock_count', 'total_records', 
            'success_records', 'failed_records'
        ])
        
        df['calc_date'] = pd.to_datetime(df['calc_date'])
        df['factor_type'] = 'G因子'
        
        print(f"   ✅ G因子数据: {len(df)} 个日期")
        return df
        
    except Exception as e:
        print(f"❌ 获取G因子数据失败: {e}")
        return None


def get_p_factor_stock_counts(context, start_year: int, end_year: int):
    """获取P因子各日期的股票数量"""
    print("📊 获取P因子股票数量数据...")
    
    query = """
    SELECT 
        calc_date,
        EXTRACT(YEAR FROM calc_date) as year,
        COUNT(DISTINCT ts_code) as stock_count,
        COUNT(*) as total_records,
        COUNT(CASE WHEN calculation_status = 'success' THEN 1 END) as success_records,
        COUNT(CASE WHEN calculation_status = 'failed' THEN 1 END) as failed_records
    FROM pgs_factors.p_factor 
    WHERE EXTRACT(YEAR FROM calc_date) BETWEEN %s AND %s
    GROUP BY calc_date, EXTRACT(YEAR FROM calc_date)
    ORDER BY calc_date
    """
    
    try:
        results = context.db_manager.fetch_sync(query, (start_year, end_year))
        
        if not results:
            print("❌ 未找到P因子数据")
            return None
        
        df = pd.DataFrame(results, columns=[
            'calc_date', 'year', 'stock_count', 'total_records', 
            'success_records', 'failed_records'
        ])
        
        df['calc_date'] = pd.to_datetime(df['calc_date'])
        df['factor_type'] = 'P因子'
        
        print(f"   ✅ P因子数据: {len(df)} 个日期")
        return df
        
    except Exception as e:
        print(f"❌ 获取P因子数据失败: {e}")
        return None


def compare_stock_counts(g_factor_df: pd.DataFrame, p_factor_df: pd.DataFrame):
    """比较G因子和P因子的股票数量"""
    print("\n🔍 比较G因子和P因子股票数量...")
    print("=" * 60)
    
    # 合并数据
    g_factor_df = g_factor_df.rename(columns={
        'stock_count': 'g_stock_count',
        'total_records': 'g_total_records',
        'success_records': 'g_success_records',
        'failed_records': 'g_failed_records'
    })
    
    p_factor_df = p_factor_df.rename(columns={
        'stock_count': 'p_stock_count',
        'total_records': 'p_total_records',
        'success_records': 'p_success_records',
        'failed_records': 'p_failed_records'
    })
    
    # 合并数据
    merged_df = pd.merge(
        g_factor_df[['calc_date', 'year', 'g_stock_count', 'g_total_records', 'g_success_records', 'g_failed_records']],
        p_factor_df[['calc_date', 'year', 'p_stock_count', 'p_total_records', 'p_success_records', 'p_failed_records']],
        on=['calc_date', 'year'],
        how='outer'
    )
    
    # 填充缺失值
    merged_df['g_stock_count'] = merged_df['g_stock_count'].fillna(0)
    merged_df['p_stock_count'] = merged_df['p_stock_count'].fillna(0)
    merged_df['g_total_records'] = merged_df['g_total_records'].fillna(0)
    merged_df['p_total_records'] = merged_df['p_total_records'].fillna(0)
    
    # 计算差异
    merged_df['stock_count_diff'] = merged_df['g_stock_count'] - merged_df['p_stock_count']
    merged_df['stock_count_ratio'] = merged_df['g_stock_count'] / merged_df['p_stock_count'].replace(0, np.nan)
    
    # 标记匹配状态
    merged_df['is_match'] = merged_df['stock_count_diff'] == 0
    merged_df['has_g_only'] = (merged_df['g_stock_count'] > 0) & (merged_df['p_stock_count'] == 0)
    merged_df['has_p_only'] = (merged_df['g_stock_count'] == 0) & (merged_df['p_stock_count'] > 0)
    merged_df['has_both'] = (merged_df['g_stock_count'] > 0) & (merged_df['p_stock_count'] > 0)
    
    print(f"📊 数据概览:")
    print(f"   总日期数: {len(merged_df)}")
    print(f"   同时有G和P因子数据: {merged_df['has_both'].sum()} 个日期")
    print(f"   只有G因子数据: {merged_df['has_g_only'].sum()} 个日期")
    print(f"   只有P因子数据: {merged_df['has_p_only'].sum()} 个日期")
    print()
    
    return merged_df


def analyze_matches(merged_df: pd.DataFrame):
    """分析匹配情况"""
    print("📈 匹配情况分析:")
    print("-" * 50)
    
    # 只分析同时有G和P因子数据的日期
    both_data = merged_df[merged_df['has_both']].copy()
    
    if len(both_data) == 0:
        print("❌ 没有同时包含G和P因子数据的日期")
        return
    
    # 基本统计
    total_dates = len(both_data)
    perfect_matches = both_data['is_match'].sum()
    match_rate = perfect_matches / total_dates * 100
    
    print(f"📊 匹配统计:")
    print(f"   同时有数据的日期数: {total_dates}")
    print(f"   完全匹配的日期数: {perfect_matches}")
    print(f"   匹配率: {match_rate:.1f}%")
    print()
    
    # 分析不匹配的情况
    mismatches = both_data[~both_data['is_match']].copy()
    
    if len(mismatches) > 0:
        print(f"🚨 不匹配日期分析:")
        print(f"   不匹配日期数: {len(mismatches)}")
        print(f"   不匹配率: {len(mismatches)/total_dates*100:.1f}%")
        print()
        
        # 计算差异统计
        mismatches['abs_diff'] = abs(mismatches['stock_count_diff'])
        
        print(f"📊 差异统计:")
        print(f"   平均差异: {mismatches['stock_count_diff'].mean():.1f} 只")
        print(f"   最大差异: {mismatches['stock_count_diff'].max():.0f} 只")
        print(f"   最小差异: {mismatches['stock_count_diff'].min():.0f} 只")
        print(f"   平均绝对差异: {mismatches['abs_diff'].mean():.1f} 只")
        print()
        
        # 显示差异最大的日期
        print(f"🔍 差异最大的前10个日期:")
        top_mismatches = mismatches.nlargest(10, 'abs_diff')
        for _, row in top_mismatches.iterrows():
            print(f"   {row['calc_date'].strftime('%Y-%m-%d')}: G={row['g_stock_count']:.0f}, P={row['p_stock_count']:.0f}, 差异={row['stock_count_diff']:+.0f}")
        
        print()
        
        # 分析差异模式
        g_more = mismatches[mismatches['stock_count_diff'] > 0]
        p_more = mismatches[mismatches['stock_count_diff'] < 0]
        
        print(f"📊 差异方向分析:")
        print(f"   G因子股票数更多: {len(g_more)} 个日期 ({len(g_more)/len(mismatches)*100:.1f}%)")
        print(f"   P因子股票数更多: {len(p_more)} 个日期 ({len(p_more)/len(mismatches)*100:.1f}%)")
        
        if len(g_more) > 0:
            print(f"   G因子平均多: {g_more['stock_count_diff'].mean():.1f} 只")
        if len(p_more) > 0:
            print(f"   P因子平均多: {abs(p_more['stock_count_diff'].mean()):.1f} 只")
        
        return mismatches
    else:
        print("✅ 所有日期都完全匹配!")
        return pd.DataFrame()


def analyze_yearly_patterns(merged_df: pd.DataFrame):
    """分析年度模式"""
    print("\n📅 年度模式分析:")
    print("-" * 50)
    
    # 只分析同时有G和P因子数据的日期
    both_data = merged_df[merged_df['has_both']].copy()
    
    if len(both_data) == 0:
        print("❌ 没有同时包含G和P因子数据的日期")
        return
    
    yearly_stats = both_data.groupby('year').agg({
        'is_match': ['count', 'sum'],
        'stock_count_diff': ['mean', 'std', 'min', 'max'],
        'g_stock_count': 'mean',
        'p_stock_count': 'mean'
    }).round(2)
    
    yearly_stats.columns = [
        '总日期数', '匹配日期数', '平均差异', '差异标准差', '最小差异', '最大差异',
        '平均G股票数', '平均P股票数'
    ]
    
    # 计算匹配率
    yearly_stats['匹配率(%)'] = (yearly_stats['匹配日期数'] / yearly_stats['总日期数'] * 100).round(1)
    
    print("年度匹配情况:")
    print(yearly_stats)
    print()
    
    # 识别问题年份
    low_match_years = yearly_stats[yearly_stats['匹配率(%)'] < 90]
    if len(low_match_years) > 0:
        print(f"⚠️ 匹配率低于90%的年份:")
        for year, row in low_match_years.iterrows():
            print(f"   {year}年: 匹配率 {row['匹配率(%)']:.1f}%, 平均差异 {row['平均差异']:+.1f} 只")
    else:
        print("✅ 所有年份匹配率都在90%以上")
    
    return yearly_stats


def analyze_2015_specific(merged_df: pd.DataFrame):
    """专门分析2015年"""
    print("\n🔍 2015年详细分析:")
    print("=" * 50)
    
    year_2015 = merged_df[merged_df['year'] == 2015].copy()
    
    if len(year_2015) == 0:
        print("❌ 2015年无数据")
        return
    
    print(f"📊 2015年基本统计:")
    print(f"   总日期数: {len(year_2015)}")
    print(f"   同时有G和P因子数据: {year_2015['has_both'].sum()} 个日期")
    print(f"   只有G因子数据: {year_2015['has_g_only'].sum()} 个日期")
    print(f"   只有P因子数据: {year_2015['has_p_only'].sum()} 个日期")
    print()
    
    # 分析同时有数据的日期
    both_2015 = year_2015[year_2015['has_both']].copy()
    
    if len(both_2015) > 0:
        perfect_matches = both_2015['is_match'].sum()
        match_rate = perfect_matches / len(both_2015) * 100
        
        print(f"📈 2015年匹配情况:")
        print(f"   同时有数据的日期数: {len(both_2015)}")
        print(f"   完全匹配的日期数: {perfect_matches}")
        print(f"   匹配率: {match_rate:.1f}%")
        print()
        
        # 分析不匹配的日期
        mismatches_2015 = both_2015[~both_2015['is_match']].copy()
        
        if len(mismatches_2015) > 0:
            print(f"🚨 2015年不匹配日期详情:")
            for _, row in mismatches_2015.iterrows():
                diff = row['stock_count_diff']
                status = "G多" if diff > 0 else "P多"
                print(f"   {row['calc_date'].strftime('%Y-%m-%d')}: G={row['g_stock_count']:.0f}, P={row['p_stock_count']:.0f}, 差异={diff:+.0f} ({status})")
        else:
            print("✅ 2015年所有日期都完全匹配!")
        
        # 计算平均差异
        avg_diff = both_2015['stock_count_diff'].mean()
        print(f"\n📊 2015年平均差异: {avg_diff:+.1f} 只")
        
        if abs(avg_diff) > 1:
            if avg_diff > 0:
                print("⚠️ G因子平均股票数比P因子多")
            else:
                print("⚠️ P因子平均股票数比G因子多")
        else:
            print("✅ 平均差异很小，基本一致")
    
    return both_2015


def generate_visualization(merged_df: pd.DataFrame, output_dir: str = "results"):
    """生成可视化图表"""
    print(f"\n📊 生成可视化图表...")
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 只分析同时有G和P因子数据的日期
    both_data = merged_df[merged_df['has_both']].copy()
    
    if len(both_data) == 0:
        print("   ⚠️ 没有同时包含G和P因子数据的日期，跳过可视化")
        return
    
    # 设置图表样式
    plt.style.use('default')
    fig_size = (15, 10)
    
    # 1. 股票数量对比时间序列图
    plt.figure(figsize=fig_size)
    
    plt.plot(both_data['calc_date'], both_data['g_stock_count'], 
            label='G因子', color='blue', linewidth=2, alpha=0.8)
    plt.plot(both_data['calc_date'], both_data['p_stock_count'], 
            label='P因子', color='red', linewidth=2, alpha=0.8)
    
    plt.title('G因子与P因子股票数量对比', fontsize=16, fontweight='bold')
    plt.xlabel('计算日期', fontsize=12)
    plt.ylabel('股票数量', fontsize=12)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    output_file = os.path.join(output_dir, 'g_p_factor_stock_count_comparison.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"   ✅ 股票数量对比图已保存: {output_file}")
    plt.close()
    
    # 2. 差异分析图
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # 差异时间序列
    ax1.plot(both_data['calc_date'], both_data['stock_count_diff'], 
            color='green', linewidth=1, alpha=0.7)
    ax1.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    ax1.set_title('G因子与P因子股票数量差异', fontweight='bold')
    ax1.set_ylabel('差异 (G - P)')
    ax1.grid(True, alpha=0.3)
    
    # 差异分布直方图
    ax2.hist(both_data['stock_count_diff'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
    ax2.axvline(x=0, color='red', linestyle='--', alpha=0.7)
    ax2.set_title('差异分布直方图', fontweight='bold')
    ax2.set_xlabel('差异 (G - P)')
    ax2.set_ylabel('频次')
    ax2.grid(True, alpha=0.3)
    
    # 年度匹配率
    yearly_stats = both_data.groupby('year').agg({
        'is_match': ['count', 'sum']
    })
    yearly_stats.columns = ['total', 'matches']
    yearly_stats['match_rate'] = yearly_stats['matches'] / yearly_stats['total'] * 100
    
    years = yearly_stats.index
    match_rates = yearly_stats['match_rate']
    
    bars = ax3.bar(years, match_rates, alpha=0.7, color='lightgreen', edgecolor='black')
    ax3.axhline(y=100, color='red', linestyle='--', alpha=0.7, label='100%匹配')
    ax3.set_title('年度匹配率', fontweight='bold')
    ax3.set_xlabel('年份')
    ax3.set_ylabel('匹配率 (%)')
    ax3.set_ylim(0, 105)
    ax3.grid(True, alpha=0.3)
    
    # 在柱状图上显示数值
    for bar, rate in zip(bars, match_rates):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + 1,
                f'{rate:.1f}%', ha='center', va='bottom', fontsize=9)
    
    # 散点图：G vs P
    ax4.scatter(both_data['p_stock_count'], both_data['g_stock_count'], 
               alpha=0.6, color='blue', s=20)
    
    # 添加y=x参考线
    min_val = min(both_data['p_stock_count'].min(), both_data['g_stock_count'].min())
    max_val = max(both_data['p_stock_count'].max(), both_data['g_stock_count'].max())
    ax4.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.7, label='y=x')
    
    ax4.set_title('G因子 vs P因子股票数量散点图', fontweight='bold')
    ax4.set_xlabel('P因子股票数量')
    ax4.set_ylabel('G因子股票数量')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    output_file = os.path.join(output_dir, 'g_p_factor_detailed_analysis.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"   ✅ 详细分析图已保存: {output_file}")
    plt.close()


def generate_comparison_report(merged_df: pd.DataFrame, mismatches: pd.DataFrame, yearly_stats: pd.DataFrame):
    """生成对比分析报告"""
    print(f"\n📋 生成对比分析报告...")
    
    report = []
    report.append("# G因子与P因子股票数量对比分析报告")
    report.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # 总体统计
    both_data = merged_df[merged_df['has_both']].copy()
    total_dates = len(both_data)
    perfect_matches = both_data['is_match'].sum() if len(both_data) > 0 else 0
    match_rate = perfect_matches / total_dates * 100 if total_dates > 0 else 0
    
    report.append("## 总体统计")
    report.append(f"- 分析年份范围: {merged_df['year'].min()}-{merged_df['year'].max()}")
    report.append(f"- 总日期数: {len(merged_df)}")
    report.append(f"- 同时有G和P因子数据: {len(both_data)} 个日期")
    report.append(f"- 完全匹配的日期数: {perfect_matches}")
    report.append(f"- 总体匹配率: {match_rate:.1f}%")
    report.append("")
    
    # 不匹配分析
    if len(mismatches) > 0:
        report.append("## 不匹配分析")
        report.append(f"- 不匹配日期数: {len(mismatches)}")
        report.append(f"- 不匹配率: {len(mismatches)/total_dates*100:.1f}%")
        report.append(f"- 平均差异: {mismatches['stock_count_diff'].mean():.1f} 只")
        report.append(f"- 最大差异: {mismatches['stock_count_diff'].max():.0f} 只")
        report.append("")
        
        # 差异最大的日期
        report.append("### 差异最大的前10个日期")
        top_mismatches = mismatches.nlargest(10, 'abs_diff')
        for _, row in top_mismatches.iterrows():
            report.append(f"- {row['calc_date'].strftime('%Y-%m-%d')}: G={row['g_stock_count']:.0f}, P={row['p_stock_count']:.0f}, 差异={row['stock_count_diff']:+.0f}")
        report.append("")
    else:
        report.append("## 不匹配分析")
        report.append("✅ 所有日期都完全匹配!")
        report.append("")
    
    # 年度分析
    if len(yearly_stats) > 0:
        report.append("## 年度匹配情况")
        for year, row in yearly_stats.iterrows():
            report.append(f"- {year}年: 匹配率 {row['匹配率(%)']:.1f}%, 平均差异 {row['平均差异']:+.1f} 只")
        report.append("")
    
    # 结论
    report.append("## 结论")
    if match_rate >= 95:
        report.append("✅ G因子和P因子的股票数量高度一致，数据质量良好")
    elif match_rate >= 90:
        report.append("⚠️ G因子和P因子的股票数量基本一致，存在少量差异")
    else:
        report.append("❌ G因子和P因子的股票数量存在显著差异，需要进一步调查")
    
    # 保存报告
    report_content = "\n".join(report)
    report_file = "results/g_p_factor_stock_count_comparison_report.md"
    os.makedirs("results", exist_ok=True)
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"   ✅ 对比分析报告已保存: {report_file}")


def main():
    parser = argparse.ArgumentParser(description='G因子和P因子股票数量对比分析脚本')
    parser.add_argument('--start_year', type=int, default=2010, help='开始年份 (默认: 2010)')
    parser.add_argument('--end_year', type=int, default=2024, help='结束年份 (默认: 2024)')
    parser.add_argument('--focus_2015', action='store_true', help='重点关注2015年分析')
    parser.add_argument('--generate_plots', action='store_true', help='生成可视化图表')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.start_year > args.end_year:
        print(f"❌ start_year ({args.start_year}) 必须小于等于 end_year ({args.end_year})")
        sys.exit(1)
    
    print("🚀 G因子与P因子股票数量对比分析器")
    print("=" * 60)
    print(f"📅 分析年份范围: {args.start_year}-{args.end_year}")
    print(f"🎯 重点关注2015年: {'是' if args.focus_2015 else '否'}")
    print(f"📊 生成可视化图表: {'是' if args.generate_plots else '否'}")
    print(f"🕐 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 初始化上下文
    try:
        context = ResearchContext()
        print("✅ 数据库连接成功")
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        sys.exit(1)
    
    # 获取数据
    g_factor_df = get_g_factor_stock_counts(context, args.start_year, args.end_year)
    p_factor_df = get_p_factor_stock_counts(context, args.start_year, args.end_year)
    
    if g_factor_df is None or p_factor_df is None:
        print("❌ 数据获取失败")
        sys.exit(1)
    
    # 比较数据
    merged_df = compare_stock_counts(g_factor_df, p_factor_df)
    
    # 分析匹配情况
    mismatches = analyze_matches(merged_df)
    
    # 年度模式分析
    yearly_stats = analyze_yearly_patterns(merged_df)
    
    # 2015年详细分析
    if args.focus_2015:
        analyze_2015_specific(merged_df)
    
    # 生成可视化图表
    if args.generate_plots:
        generate_visualization(merged_df)
    
    # 生成对比报告
    generate_comparison_report(merged_df, mismatches, yearly_stats)
    
    print("\n✅ 对比分析完成!")


if __name__ == "__main__":
    main()
