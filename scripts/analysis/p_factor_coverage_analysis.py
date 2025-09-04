#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P因子覆盖率分析脚本
====================

目标:
- 以周五为节律，统计各日期 P 因子可计算股票数 与 A股在籍可交易股票数 的覆盖率
- 评估不同阈值(如60%/70%/80%)下，连续达到最小覆盖周数(如26周)的最早起点
- 输出CSV与控制台摘要，辅助判定可用于量化回测的起始时间窗口

依赖:
- research.tools.context.ResearchContext 提供数据库访问
- get_trading_stocks_optimized(date) 返回在籍可交易股票集合

用法示例:
  python scripts/analysis/p_factor_coverage_analysis.py \
    --start-date 2000-01-01 --end-date 2025-08-31 \
    --thresholds 0.6 0.7 0.8 --min-abs-count 1000 --min-weeks 26
"""

import sys
import argparse
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Tuple

import pandas as pd

# 将项目根目录加入路径
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from research.tools.context import ResearchContext  # noqa: E402


def generate_fridays(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    fridays: List[str] = []
    current = start
    while current.weekday() != 4:  # 周五=4
        current += timedelta(days=1)
        if current > end:
            break

    while current <= end:
        fridays.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=7)
    return fridays


def fetch_counts_for_date(ctx: ResearchContext, calc_date: str) -> Tuple[int, int]:
    """返回 (p因子股票数, 在籍可交易股票数)"""
    # P因子覆盖
    q_p = """
    SELECT COUNT(DISTINCT ts_code) AS p_count
    FROM pgs_factors.p_factor
    WHERE calc_date = %s
    """
    p_df = ctx.query_dataframe(q_p, (calc_date,))
    p_count = int(p_df.iloc[0]['p_count']) if p_df is not None and not p_df.empty else 0

    # 在籍可交易数（使用优化函数）
    q_trading = "SELECT COUNT(*) AS trading_count FROM get_trading_stocks_optimized(%s)"
    t_df = ctx.query_dataframe(q_trading, (calc_date,))
    trading_count = int(t_df.iloc[0]['trading_count']) if t_df is not None and not t_df.empty else 0

    return p_count, trading_count


def summarize_thresholds(df: pd.DataFrame, thresholds: List[float], min_abs_count: int, min_weeks: int) -> List[Dict]:
    """对不同阈值做连续周覆盖评估，返回建议列表。"""
    results = []
    for th in thresholds:
        streak = 0
        start_idx = None
        best_window = None

        for i, row in df.iterrows():
            ok = (row['trading_count'] > 0 and
                  row['p_count'] >= min_abs_count and
                  row['coverage_rate'] >= th)
            if ok:
                streak += 1
                if start_idx is None:
                    start_idx = i
                if streak >= min_weeks and best_window is None:
                    best_window = (df.loc[start_idx, 'calc_date'], row['calc_date'], streak)
            else:
                streak = 0
                start_idx = None

        results.append({
            'threshold': th,
            'min_abs_count': min_abs_count,
            'min_weeks': min_weeks,
            'earliest_window_start': best_window[0] if best_window else None,
            'earliest_window_end': best_window[1] if best_window else None,
            'earliest_streak_weeks': best_window[2] if best_window else 0
        })
    return results


def main():
    parser = argparse.ArgumentParser(description='P因子覆盖率分析')
    parser.add_argument('--start-date', required=True, type=str, help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end-date', required=True, type=str, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--thresholds', nargs='+', type=float, default=[0.6, 0.7, 0.8], help='覆盖率阈值列表')
    parser.add_argument('--min-abs-count', type=int, default=1000, help='最小绝对股票数')
    parser.add_argument('--min-weeks', type=int, default=26, help='最小连续周数')
    args = parser.parse_args()

    # 边界
    end_d = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    if end_d > date.today():
        end_d = date.today()
    end_date = end_d.strftime('%Y-%m-%d')

    # 上下文
    ctx = ResearchContext()

    dates = generate_fridays(args.start_date, end_date)
    rows = []
    for d in dates:
        try:
            p_count, trading_count = fetch_counts_for_date(ctx, d)
            coverage = (p_count / trading_count) if trading_count > 0 else 0.0
            rows.append({'calc_date': d, 'p_count': p_count, 'trading_count': trading_count, 'coverage_rate': coverage})
        except Exception as e:
            rows.append({'calc_date': d, 'p_count': 0, 'trading_count': 0, 'coverage_rate': 0.0, 'error': str(e)})

    df = pd.DataFrame(rows)
    df['calc_date'] = pd.to_datetime(df['calc_date'])

    # 导出CSV
    out_dir = Path('reports')
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_csv = out_dir / f'p_factor_coverage_{stamp}.csv'
    df.to_csv(out_csv, index=False)

    # 阈值评估
    summary = summarize_thresholds(df.sort_values('calc_date').reset_index(drop=True), args.thresholds, args.min_abs_count, args.min_weeks)

    # 控制台摘要
    print('=' * 80)
    print('P因子覆盖率分析摘要')
    print('=' * 80)
    print(f'范围: {args.start_date} ~ {end_date} | 条目: {len(df)} | 导出: {out_csv}')
    print(f'全样本覆盖均值: {df["coverage_rate"].mean():.3f} | 中位数: {df["coverage_rate"].median():.3f}')
    print(f'近一年覆盖均值: {df[df["calc_date"] >= (df["calc_date"].max() - pd.Timedelta(days=365))]["coverage_rate"].mean():.3f}')
    print('-' * 80)
    for item in summary:
        th = item['threshold']
        start = item['earliest_window_start']
        end = item['earliest_window_end']
        weeks = item['earliest_streak_weeks']
        msg = f"阈值{th:.0%}, 最小数{args.min_abs_count}, 连续{args.min_weeks}周: "
        if start is not None:
            print(msg + f"满足窗口 {start.date()} ~ {end.date()} (连续{weeks}周)")
        else:
            print(msg + "未满足连续性条件")


if __name__ == '__main__':
    main()


