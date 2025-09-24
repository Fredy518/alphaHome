#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P因子全市场一致性验证脚本
========================

随机选择一个周五，计算所有A股的P因子，验证生产级版本和研究目录版本的一致性。

使用方法：
python scripts/production/factor_calculators/p_factor/validate_full_market_consistency.py --auto_date
python scripts/production/factor_calculators/p_factor/validate_full_market_consistency.py --test_date 2024-12-13
"""

import sys
import os
import argparse
import time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
import random

# 添加项目根目录到路径  
current_file = Path(__file__)
project_root = current_file.parent.parent.parent.parent.parent
sys.path.append(str(project_root))

# 导入生产级计算器
import importlib.util
spec = importlib.util.spec_from_file_location(
    "production_p_factor_calculator", 
    Path(__file__).parent / "production_p_factor_calculator.py"
)
prod_calc_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prod_calc_module)
ProductionPFactorCalculator = prod_calc_module.ProductionPFactorCalculator

# 导入研究目录的计算器
try:
    # 导入 ResearchContext
    context_spec = importlib.util.spec_from_file_location(
        "research_context", 
        project_root / "research" / "tools" / "context.py"
    )
    context_module = importlib.util.module_from_spec(context_spec)
    context_spec.loader.exec_module(context_module)
    ResearchContext = context_module.ResearchContext
    
    # 导入研究目录的 P 因子计算器
    research_calc_spec = importlib.util.spec_from_file_location(
        "research_p_factor_calculator", 
        project_root / "research" / "pgs_factor" / "processors" / "production_p_factor_calculator.py"
    )
    research_calc_module = importlib.util.module_from_spec(research_calc_spec)
    research_calc_spec.loader.exec_module(research_calc_module)
    ResearchPFactorCalculator = research_calc_module.ProductionPFactorCalculator
    
    RESEARCH_AVAILABLE = True
    print("✅ 研究目录模块导入成功")
except Exception as e:
    print(f"⚠️ 研究目录模块不可用: {e}")
    RESEARCH_AVAILABLE = False
    ResearchContext = None
    ResearchPFactorCalculator = None


def get_random_friday(start_year: int = 2024, months_back: int = 6) -> str:
    """获取随机的周五日期
    
    Args:
        start_year: 开始年份
        months_back: 往前多少个月
    
    Returns:
        周五日期字符串
    """
    # 计算开始和结束日期
    end_date = date.today() - timedelta(days=30)  # 避免选择太近期的日期
    start_date = end_date - timedelta(days=months_back * 30)
    
    # 收集所有周五
    fridays = []
    current = start_date
    
    # 找到第一个周五
    while current.weekday() != 4:  # 4 = 周五
        current += timedelta(days=1)
    
    # 收集所有周五
    while current <= end_date:
        fridays.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=7)
    
    # 随机选择一个
    if fridays:
        selected_friday = random.choice(fridays)
        print(f"随机选择的周五: {selected_friday}")
        return selected_friday
    else:
        # 回退到固定日期
        return "2024-12-13"


def get_all_trading_stocks(test_date: str) -> list:
    """获取指定日期的所有在交易A股
    
    Args:
        test_date: 测试日期
    
    Returns:
        股票代码列表
    """
    try:
        prod_calc = ProductionPFactorCalculator()
        
        # 尝试从数据库获取
        query = """
        SELECT ts_code
        FROM tushare.stock_basic
        WHERE list_date <= %s
        AND (delist_date IS NULL OR delist_date > %s)
        AND (ts_code LIKE '%.SZ' OR ts_code LIKE '%.SH')
        ORDER BY ts_code
        """
        
        # 先尝试从数据库查询真实的A股列表
        try:
            results = prod_calc.db_manager.fetch_sync(query, (test_date, test_date))
            
            if results and len(results) > 0:
                stock_codes = [row[0] for row in results if len(row) > 0]
                if stock_codes:
                    print(f"从数据库获取到 {len(stock_codes)} 只A股")
                    return stock_codes
        except Exception as db_error:
            print(f"数据库查询失败: {db_error}")
        
        # 如果数据库查询失败，使用扩展的预定义股票列表进行测试
        print("使用扩展预定义股票列表进行全市场验证测试")
        
        # 扩展的A股代表股票池（涵盖更多行业和规模）
        extended_stocks = []
        
        # 深交所主板和中小板 (000XXX, 002XXX)
        for i in range(1, 100):  # 000001-000099
            extended_stocks.append(f"{i:06d}.SZ")
        for i in range(1, 50):   # 002001-002049  
            extended_stocks.append(f"{2000+i:06d}.SZ")
            
        # 上交所主板 (600XXX, 601XXX)
        for i in range(1, 100):  # 600001-600099
            extended_stocks.append(f"{600000+i:06d}.SH")
        for i in range(1, 50):   # 601001-601049
            extended_stocks.append(f"{601000+i:06d}.SH")
            
        # 创业板 (300XXX) 
        for i in range(1, 30):   # 300001-300029
            extended_stocks.append(f"{300000+i:06d}.SZ")
        
        print(f"扩展股票池: {len(extended_stocks)} 只股票代码")
        return extended_stocks
    
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return []


def run_production_full_calculation(test_date: str, stock_codes: list) -> pd.DataFrame:
    """运行生产级全市场P因子计算"""
    print(f"\n=== 运行生产级全市场P因子计算 ===")
    print(f"股票数量: {len(stock_codes)}")
    
    try:
        calc = ProductionPFactorCalculator()
        
        start_time = time.time()
        result = calc.calculate_p_factors_pit(test_date, stock_codes)
        end_time = time.time()
        
        print(f"生产级计算结果:")
        print(f"  成功: {result['success_count']}")
        print(f"  失败: {result['failed_count']}")
        print(f"  耗时: {end_time - start_time:.2f} 秒")
        print(f"  吞吐量: {result['success_count']/(end_time - start_time):.1f} 只/秒")
        
        if result['success_count'] == 0:
            print("❌ 生产级计算未产生任何结果")
            return pd.DataFrame()
        
        # 从数据库查询计算结果
        query = """
        SELECT ts_code, p_score, p_rank, gpa, roe_excl, roa_excl, 
               net_margin_ttm, operating_margin_ttm, roi_ttm
        FROM pgs_factors.p_factor
        WHERE calc_date = %s
        ORDER BY p_rank ASC, ts_code
        """
        
        results = calc.db_manager.fetch_sync(query, (test_date,))
        
        if results:
            columns = ['ts_code', 'p_score', 'p_rank', 'gpa', 'roe_excl', 'roa_excl',
                      'net_margin_ttm', 'operating_margin_ttm', 'roi_ttm']
            prod_df = pd.DataFrame(results, columns=columns)
            print(f"  查询到 {len(prod_df)} 条生产级结果")
            return prod_df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ 生产级计算失败: {e}")
        return pd.DataFrame()


def run_research_full_calculation(test_date: str, stock_codes: list) -> pd.DataFrame:
    """运行研究目录全市场P因子计算"""
    print(f"\n=== 运行研究目录全市场P因子计算 ===")
    
    if not RESEARCH_AVAILABLE:
        print("❌ 研究目录模块不可用")
        return pd.DataFrame()
    
    try:
        context = ResearchContext()
        calc = ResearchPFactorCalculator(context)
        
        start_time = time.time()
        result = calc.calculate_p_factors_pit(test_date, stock_codes)
        end_time = time.time()
        
        print(f"研究目录计算结果:")
        print(f"  成功: {result['success_count']}")
        print(f"  失败: {result['failed_count']}")
        print(f"  耗时: {end_time - start_time:.2f} 秒")
        print(f"  吞吐量: {result['success_count']/(end_time - start_time):.1f} 只/秒")
        
        if result['success_count'] == 0:
            print("❌ 研究目录计算未产生任何结果")
            return pd.DataFrame()
        
        # 从数据库查询计算结果
        query = """
        SELECT ts_code, p_score, p_rank, gpa, roe_excl, roa_excl,
               net_margin_ttm, operating_margin_ttm, roi_ttm
        FROM pgs_factors.p_factor
        WHERE calc_date = %s
        ORDER BY p_rank ASC, ts_code
        """
        
        research_df = context.query_dataframe(query, (test_date,))
        
        if research_df is not None and not research_df.empty:
            print(f"  查询到 {len(research_df)} 条研究目录结果")
            return research_df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ 研究目录计算失败: {e}")
        return pd.DataFrame()


def analyze_full_market_results(prod_df: pd.DataFrame, research_df: pd.DataFrame) -> dict:
    """分析全市场计算结果"""
    print(f"\n=== 全市场计算结果分析 ===")
    
    if prod_df.empty or research_df.empty:
        return {'success': False, 'error': 'One or both results are empty'}
    
    # 基础统计
    print(f"生产级结果: {len(prod_df)} 只股票")
    print(f"研究目录结果: {len(research_df)} 只股票")
    
    # 按股票代码合并
    merged = pd.merge(
        prod_df.add_suffix('_prod'), 
        research_df.add_suffix('_research'),
        left_on='ts_code_prod',
        right_on='ts_code_research',
        how='outer',
        indicator=True
    )
    
    common_stocks = len(merged[merged['_merge'] == 'both'])
    only_prod = len(merged[merged['_merge'] == 'left_only'])
    only_research = len(merged[merged['_merge'] == 'right_only'])
    
    print(f"共同股票: {common_stocks} 只")
    print(f"仅生产级有: {only_prod} 只")  
    print(f"仅研究目录有: {only_research} 只")
    
    if common_stocks == 0:
        return {'success': False, 'error': 'No common stocks found'}
    
    # 分析共同股票的差异
    common_data = merged[merged['_merge'] == 'both'].copy()
    
    # P评分统计
    prod_p_scores = pd.to_numeric(common_data['p_score_prod'], errors='coerce')
    research_p_scores = pd.to_numeric(common_data['p_score_research'], errors='coerce')
    p_score_diff = np.abs(prod_p_scores - research_p_scores)
    
    # 排名统计
    prod_ranks = pd.to_numeric(common_data['p_rank_prod'], errors='coerce')
    research_ranks = pd.to_numeric(common_data['p_rank_research'], errors='coerce')
    rank_diff = np.abs(prod_ranks - research_ranks)
    
    # 行业特殊处理股票分析
    financial_stocks_prod = common_data[pd.isna(pd.to_numeric(common_data['gpa_prod'], errors='coerce'))]['ts_code_prod'].tolist()
    financial_stocks_research = common_data[pd.isna(pd.to_numeric(common_data['gpa_research'], errors='coerce'))]['ts_code_research'].tolist()
    
    results = {
        'success': True,
        'total_stocks': {
            'production': len(prod_df),
            'research': len(research_df),
            'common': common_stocks,
            'only_production': only_prod,
            'only_research': only_research
        },
        'p_score_analysis': {
            'max_diff': float(p_score_diff.max()) if not p_score_diff.empty else None,
            'mean_diff': float(p_score_diff.mean()) if not p_score_diff.empty else None,
            'std_diff': float(p_score_diff.std()) if not p_score_diff.empty else None,
            'production_mean': float(prod_p_scores.mean()) if not prod_p_scores.empty else None,
            'research_mean': float(research_p_scores.mean()) if not research_p_scores.empty else None
        },
        'ranking_analysis': {
            'max_rank_diff': float(rank_diff.max()) if not rank_diff.empty else None,
            'mean_rank_diff': float(rank_diff.mean()) if not rank_diff.empty else None,
            'perfect_rank_match': int((rank_diff == 0).sum()) if not rank_diff.empty else 0
        },
        'special_handling': {
            'financial_stocks_prod': len(financial_stocks_prod),
            'financial_stocks_research': len(financial_stocks_research),
            'consistent_financial': len(set(financial_stocks_prod) & set(financial_stocks_research))
        }
    }
    
    # 打印详细分析
    print(f"\nP评分分析:")
    print(f"  最大差异: {results['p_score_analysis']['max_diff']:.6f}")
    print(f"  平均差异: {results['p_score_analysis']['mean_diff']:.6f}")
    print(f"  差异标准差: {results['p_score_analysis']['std_diff']:.6f}")
    print(f"  生产级平均分: {results['p_score_analysis']['production_mean']:.2f}")
    print(f"  研究目录平均分: {results['p_score_analysis']['research_mean']:.2f}")
    
    print(f"\n排名分析:")
    print(f"  最大排名差异: {results['ranking_analysis']['max_rank_diff']}")
    print(f"  平均排名差异: {results['ranking_analysis']['mean_rank_diff']:.2f}")
    print(f"  完全相同排名: {results['ranking_analysis']['perfect_rank_match']} 只")
    
    print(f"\n特殊处理分析:")
    print(f"  生产级金融股: {results['special_handling']['financial_stocks_prod']} 只")
    print(f"  研究目录金融股: {results['special_handling']['financial_stocks_research']} 只")
    print(f"  一致的金融股: {results['special_handling']['consistent_financial']} 只")
    
    # 判断整体一致性
    tolerance = 1e-6
    is_consistent = (
        results['p_score_analysis']['max_diff'] is not None and
        results['p_score_analysis']['max_diff'] < tolerance and
        results['total_stocks']['only_production'] == 0 and
        results['total_stocks']['only_research'] == 0
    )
    
    results['is_consistent'] = is_consistent
    
    if is_consistent:
        print(f"\n✅ 全市场计算结果完全一致！")
    else:
        print(f"\n⚠️ 全市场计算结果存在差异")
    
    return results


def main():
    parser = argparse.ArgumentParser(description='P因子全市场一致性验证')
    parser.add_argument('--test_date', type=str, help='指定测试日期 (YYYY-MM-DD)')
    parser.add_argument('--auto_date', action='store_true', help='自动选择随机周五')
    parser.add_argument('--max_stocks', type=int, default=None, help='限制最大股票数量（用于测试）')
    
    args = parser.parse_args()
    
    # 确定测试日期
    if args.auto_date:
        test_date = get_random_friday()
    elif args.test_date:
        test_date = args.test_date
    else:
        test_date = get_random_friday()  # 默认随机选择
    
    print("🔬 P因子全市场一致性验证")
    print("=" * 60)
    print(f"测试日期: {test_date}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 获取所有股票
    all_stocks = get_all_trading_stocks(test_date)
    if not all_stocks:
        print("❌ 未能获取股票列表")
        return
    
    # 限制股票数量（用于测试）
    if args.max_stocks and len(all_stocks) > args.max_stocks:
        all_stocks = random.sample(all_stocks, args.max_stocks)
        print(f"限制为 {args.max_stocks} 只股票进行测试")
    
    print(f"总股票数: {len(all_stocks)} 只")
    
    # 清理现有数据
    print(f"\n清理测试日期 {test_date} 的现有P因子数据...")
    try:
        prod_calc = ProductionPFactorCalculator()
        prod_calc.db_manager.execute_sync(
            "DELETE FROM pgs_factors.p_factor WHERE calc_date = %s", 
            (test_date,)
        )
        print("已清理旧数据")
    except Exception as e:
        print(f"清理数据失败: {e}")
    
    # 运行生产级计算
    prod_df = run_production_full_calculation(test_date, all_stocks)
    
    # 备份生产级结果
    prod_backup = prod_df.copy() if not prod_df.empty else pd.DataFrame()
    
    # 清理数据准备研究目录计算
    if not prod_df.empty:
        print(f"\n清理数据，准备研究目录计算...")
        try:
            prod_calc.db_manager.execute_sync(
                "DELETE FROM pgs_factors.p_factor WHERE calc_date = %s", 
                (test_date,)
            )
        except Exception as e:
            print(f"清理数据失败: {e}")
    
    # 运行研究目录计算
    research_df = run_research_full_calculation(test_date, all_stocks)
    
    # 分析结果
    if not prod_backup.empty and not research_df.empty:
        analysis = analyze_full_market_results(prod_backup, research_df)
        
        print("\n" + "=" * 60)
        print("🎯 全市场验证结果总结")
        print("=" * 60)
        
        if analysis.get('is_consistent', False):
            print("🎉 P因子全市场计算完全一致！")
            print("✅ 生产环境可以安全投入使用")
        else:
            print("⚠️ 检测到差异，需要进一步分析")
        
        print(f"\n关键指标:")
        print(f"  测试股票数: {len(all_stocks)}")
        print(f"  成功计算数: {analysis['total_stocks']['common']}")
        print(f"  P评分最大差异: {analysis['p_score_analysis']['max_diff']:.8f}")
        print(f"  排名最大差异: {analysis['ranking_analysis']['max_rank_diff']}")
        print(f"  完全相同排名比例: {analysis['ranking_analysis']['perfect_rank_match']}/{analysis['total_stocks']['common']} ({analysis['ranking_analysis']['perfect_rank_match']/max(analysis['total_stocks']['common'], 1)*100:.1f}%)")
        
    else:
        print("\n❌ 验证失败，无法获取有效的计算结果")
    
    print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
