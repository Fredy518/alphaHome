#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P因子生产环境一致性验证脚本
==========================

对比生产级P因子计算结果与研究目录下的计算结果，确保迁移过程中没有引入任何逻辑错误。

使用方法：
python scripts/production/factor_calculators/p_factor/validate_p_factor_consistency.py --test_date 2024-12-20
"""

import sys
import os
import argparse
import time
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径  
# scripts/production/factor_calculators/p_factor/validate_p_factor_consistency.py
# 需要向上4层: p_factor -> factor_calculators -> production -> scripts -> 项目根目录
current_file = Path(__file__)
project_root = current_file.parent.parent.parent.parent.parent
sys.path.append(str(project_root))

# 调试信息已验证通过，可以注释掉
# print(f"Current file: {current_file}")
# print(f"Project root: {project_root}")
# print(f"Research path exists: {(project_root / 'research').exists()}")
# print(f"Tools path exists: {(project_root / 'research' / 'tools').exists()}")
# print(f"Context file exists: {(project_root / 'research' / 'tools' / 'context.py').exists()}")

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
    # 动态导入研究目录的模块
    import importlib.util
    
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
    print("将跳过研究目录计算，仅测试生产级脚本")
    RESEARCH_AVAILABLE = False
    ResearchContext = None
    ResearchPFactorCalculator = None


def get_trading_stocks_sample(test_date: str, sample_size: int = 100) -> list:
    """获取测试用的股票样本"""
    try:
        # 使用生产级计算器获取股票列表
        prod_calc = ProductionPFactorCalculator()
        
        # 直接构造一些测试股票代码
        test_stocks = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH', '000858.SZ']
        print(f"使用预定义测试股票: {test_stocks}")
        
        # 也尝试从数据库获取
        try:
            all_stocks = prod_calc._get_trading_stock_codes(test_date)
            if all_stocks:
                print(f"从数据库获取到 {len(all_stocks)} 只股票")
                # 如果数据库有数据，优先使用数据库数据
                if len(all_stocks) >= sample_size:
                    import random
                    random.seed(42)
                    return random.sample(all_stocks, sample_size)
                else:
                    return all_stocks
        except Exception as e:
            print(f"从数据库获取股票失败: {e}")
        
        # 使用测试股票
        if len(test_stocks) > sample_size:
            import random
            random.seed(42)
            sample_stocks = random.sample(test_stocks, sample_size)
        else:
            sample_stocks = test_stocks[:sample_size]
            
        print(f"选择 {len(sample_stocks)} 只测试股票: {sample_stocks}")
        return sample_stocks
        
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return []


def run_production_calculation(test_date: str, stock_codes: list) -> pd.DataFrame:
    """运行生产级P因子计算"""
    print("\n=== 运行生产级P因子计算 ===")
    
    try:
        calc = ProductionPFactorCalculator()
        
        start_time = time.time()
        result = calc.calculate_p_factors_pit(test_date, stock_codes)
        end_time = time.time()
        
        print(f"生产级计算结果:")
        print(f"  成功: {result['success_count']}")
        print(f"  失败: {result['failed_count']}")
        print(f"  耗时: {end_time - start_time:.2f} 秒")
        
        if result['success_count'] == 0:
            print("❌ 生产级计算未产生任何结果")
            return pd.DataFrame()
        
        # 从数据库查询计算结果
        query = """
        SELECT ts_code, calc_date, p_score, p_rank, gpa, roe_excl, roa_excl
        FROM pgs_factors.p_factor
        WHERE calc_date = %s
        AND ts_code = ANY(%s)
        ORDER BY ts_code
        """
        
        results = calc.db_manager.fetch_sync(query, (test_date, stock_codes))
        
        if results:
            columns = ['ts_code', 'calc_date', 'p_score', 'p_rank', 'gpa', 'roe_excl', 'roa_excl']
            prod_df = pd.DataFrame(results, columns=columns)
        else:
            prod_df = pd.DataFrame()
        
        print(f"  从数据库查询到 {len(prod_df)} 条记录")
        return prod_df
        
    except Exception as e:
        print(f"❌ 生产级计算失败: {e}")
        return pd.DataFrame()


def run_research_calculation(test_date: str, stock_codes: list) -> pd.DataFrame:
    """运行研究目录的P因子计算"""
    print("\n=== 运行研究目录P因子计算 ===")
    
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
        
        if result['success_count'] == 0:
            print("❌ 研究目录计算未产生任何结果")
            return pd.DataFrame()
        
        # 从数据库查询计算结果
        query = """
        SELECT ts_code, calc_date, p_score, p_rank, gpa, roe_excl, roa_excl
        FROM pgs_factors.p_factor
        WHERE calc_date = %s
        AND ts_code = ANY(%s)
        ORDER BY ts_code
        """
        
        research_df = context.query_dataframe(query, (test_date, stock_codes))
        
        if research_df is not None and not research_df.empty:
            print(f"  从数据库查询到 {len(research_df)} 条记录")
            return research_df
        else:
            print("❌ 研究目录计算结果为空")
            return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ 研究目录计算失败: {e}")
        return pd.DataFrame()


def compare_results(prod_df: pd.DataFrame, research_df: pd.DataFrame) -> dict:
    """对比两个计算结果"""
    print("\n=== 对比计算结果 ===")
    
    if prod_df.empty or research_df.empty:
        return {
            'success': False,
            'error': 'One or both dataframes are empty',
            'details': {}
        }
    
    # 按ts_code合并数据
    merged = pd.merge(
        prod_df.add_suffix('_prod'), 
        research_df.add_suffix('_research'), 
        left_on='ts_code_prod', 
        right_on='ts_code_research',
        how='outer',
        indicator=True
    )
    
    comparison_results = {
        'success': True,
        'total_production': len(prod_df),
        'total_research': len(research_df),
        'common_stocks': len(merged[merged['_merge'] == 'both']),
        'only_production': len(merged[merged['_merge'] == 'left_only']),
        'only_research': len(merged[merged['_merge'] == 'right_only']),
        'details': {}
    }
    
    print(f"数据记录数对比:")
    print(f"  生产级结果: {comparison_results['total_production']} 条")
    print(f"  研究目录结果: {comparison_results['total_research']} 条")
    print(f"  共同股票: {comparison_results['common_stocks']} 只")
    print(f"  仅生产级有: {comparison_results['only_production']} 只")
    print(f"  仅研究目录有: {comparison_results['only_research']} 只")
    
    # 对于共同股票，比较数值字段
    if comparison_results['common_stocks'] > 0:
        common_data = merged[merged['_merge'] == 'both'].copy()
        
        # 比较P评分
        try:
            p_score_diff = np.abs(pd.to_numeric(common_data['p_score_prod'], errors='coerce') - 
                                 pd.to_numeric(common_data['p_score_research'], errors='coerce'))
            p_score_max_diff = p_score_diff.max()
            p_score_mean_diff = p_score_diff.mean()
        except Exception as e:
            print(f"P评分对比出错: {e}")
            p_score_max_diff = None
            p_score_mean_diff = None
        
        # 比较财务指标
        try:
            gpa_diff = np.abs(pd.to_numeric(common_data['gpa_prod'], errors='coerce') - 
                             pd.to_numeric(common_data['gpa_research'], errors='coerce'))
            roe_diff = np.abs(pd.to_numeric(common_data['roe_excl_prod'], errors='coerce') - 
                             pd.to_numeric(common_data['roe_excl_research'], errors='coerce'))
            roa_diff = np.abs(pd.to_numeric(common_data['roa_excl_prod'], errors='coerce') - 
                             pd.to_numeric(common_data['roa_excl_research'], errors='coerce'))
        except Exception as e:
            print(f"财务指标对比出错: {e}")
            gpa_diff = pd.Series([])
            roe_diff = pd.Series([])
            roa_diff = pd.Series([])
        
        comparison_results['details'] = {
            'p_score_max_diff': float(p_score_max_diff) if p_score_max_diff is not None and not pd.isna(p_score_max_diff) else None,
            'p_score_mean_diff': float(p_score_mean_diff) if p_score_mean_diff is not None and not pd.isna(p_score_mean_diff) else None,
            'gpa_max_diff': float(gpa_diff.max()) if not gpa_diff.empty and not pd.isna(gpa_diff.max()) else None,
            'roe_max_diff': float(roe_diff.max()) if not roe_diff.empty and not pd.isna(roe_diff.max()) else None,
            'roa_max_diff': float(roa_diff.max()) if not roa_diff.empty and not pd.isna(roa_diff.max()) else None,
        }
        
        print(f"\n数值对比（共同股票）:")
        print(f"  P评分最大差异: {comparison_results['details']['p_score_max_diff']:.6f}")
        print(f"  P评分平均差异: {comparison_results['details']['p_score_mean_diff']:.6f}")
        print(f"  GPA最大差异: {comparison_results['details']['gpa_max_diff']}")
        print(f"  ROE最大差异: {comparison_results['details']['roe_max_diff']}")
        print(f"  ROA最大差异: {comparison_results['details']['roa_max_diff']}")
        
        # 判断一致性
        tolerance = 1e-10  # 数值容差
        is_consistent = (
            (comparison_results['total_production'] == comparison_results['total_research']) and
            (comparison_results['only_production'] == 0) and
            (comparison_results['only_research'] == 0) and
            (comparison_results['details']['p_score_max_diff'] is None or 
             comparison_results['details']['p_score_max_diff'] < tolerance)
        )
        
        comparison_results['is_consistent'] = is_consistent
        
        if is_consistent:
            print("\n✅ 计算结果完全一致！")
        else:
            print("\n⚠️ 计算结果存在差异")
    
    return comparison_results


def main():
    parser = argparse.ArgumentParser(description='P因子生产环境一致性验证脚本')
    parser.add_argument('--test_date', type=str, required=True, 
                       help='测试日期 (YYYY-MM-DD格式)')
    parser.add_argument('--sample_size', type=int, default=50,
                       help='测试股票样本数量 (默认: 50)')
    parser.add_argument('--skip_research', action='store_true',
                       help='跳过研究目录计算（仅测试生产级）')
    
    args = parser.parse_args()
    
    print("🔬 P因子生产环境一致性验证")
    print("=" * 50)
    print(f"测试日期: {args.test_date}")
    print(f"样本数量: {args.sample_size}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 获取测试股票样本
    stock_codes = get_trading_stocks_sample(args.test_date, args.sample_size)
    if not stock_codes:
        print("❌ 未能获取测试股票样本")
        sys.exit(1)
    
    # 2. 清理现有数据（避免干扰）
    print(f"\n清理测试日期 {args.test_date} 的现有P因子数据...")
    try:
        prod_calc = ProductionPFactorCalculator()
        prod_calc.db_manager.execute_sync(
            "DELETE FROM pgs_factors.p_factor WHERE calc_date = %s", 
            (args.test_date,)
        )
        print(f"已清理测试日期的旧数据")
    except Exception as e:
        print(f"清理数据失败: {e}")
    
    # 3. 运行生产级计算
    prod_df = run_production_calculation(args.test_date, stock_codes)
    
    if args.skip_research or not RESEARCH_AVAILABLE:
        if not prod_df.empty:
            print(f"\n✅ 生产级计算成功完成，产生 {len(prod_df)} 条结果")
        else:
            print("\n❌ 生产级计算失败")
        return
    
    # 4. 保存生产级结果
    prod_backup = prod_df.copy() if not prod_df.empty else pd.DataFrame()
    
    # 5. 清理数据，准备研究目录计算
    if not prod_df.empty:
        print(f"\n保存生产级结果并清理数据...")
        try:
            prod_calc.db_manager.execute_sync(
                "DELETE FROM pgs_factors.p_factor WHERE calc_date = %s", 
                (args.test_date,)
            )
        except Exception as e:
            print(f"清理数据失败: {e}")
    
    # 6. 运行研究目录计算
    research_df = run_research_calculation(args.test_date, stock_codes)
    
    # 7. 对比结果
    if not prod_backup.empty and not research_df.empty:
        comparison = compare_results(prod_backup, research_df)
        
        print("\n" + "=" * 50)
        print("🎯 验证结果总结")
        print("=" * 50)
        
        if comparison.get('is_consistent', False):
            print("✅ P因子生产环境迁移成功！")
            print("✅ 生产级计算结果与研究目录完全一致")
        else:
            print("⚠️ 检测到差异，需要进一步检查")
            
        print(f"\n详细统计:")
        print(f"  测试股票数: {len(stock_codes)}")
        print(f"  生产级结果: {comparison['total_production']} 条")
        print(f"  研究目录结果: {comparison['total_research']} 条")
        print(f"  数据一致性: {'✅ 完全一致' if comparison.get('is_consistent', False) else '⚠️ 存在差异'}")
        
    else:
        print("\n❌ 无法完成对比，某一方计算失败")
    
    print(f"\n结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
