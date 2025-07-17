"""
database_research 模板使用示例

这个文件展示了如何快速使用研究模板进行因子研究
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from research.tools.context import ResearchContext
from research.tools.pipeline import ResearchPipeline, Step
from src.steps import LoadStockDataStep, CalculateFactorsStep
import pandas as pd


class SimpleAnalysisStep(Step):
    """简单的分析步骤示例"""
    
    def run(self, **kwargs):
        factor_data = kwargs.get('factor_data', pd.DataFrame())
        
        if factor_data.empty:
            return {'analysis': 'No data to analyze'}
        
        # 简单统计
        print("\n=== 因子数据统计 ===")
        print(f"总记录数: {len(factor_data)}")
        print(f"股票数量: {factor_data['ts_code'].nunique()}")
        print(f"日期范围: {factor_data['trade_date'].min()} 至 {factor_data['trade_date'].max()}")
        
        # 计算每只股票的平均收益率
        if 'pct_chg' in factor_data.columns:
            stock_returns = factor_data.groupby('ts_code')['pct_chg'].mean()
            print("\n=== 股票平均日收益率 ===")
            print(stock_returns.sort_values(ascending=False))
        
        # 查看最新的技术指标
        latest_date = factor_data['trade_date'].max()
        latest_indicators = factor_data[factor_data['trade_date'] == latest_date]
        
        print(f"\n=== 最新日期 {latest_date} 的技术指标 ===")
        indicator_cols = ['ts_code', 'close', 'ma_5', 'ma_20', 'rsi_14', 'macd']
        available_cols = [col for col in indicator_cols if col in latest_indicators.columns]
        print(latest_indicators[available_cols])
        
        return {'analysis_complete': True}


def example_1_basic_usage():
    """示例1: 基础用法 - 加载数据并计算因子"""
    print("\n" + "="*80)
    print("示例1: 基础用法")
    print("="*80)
    
    # 创建研究上下文
    context = ResearchContext(Path(__file__).parent)
    
    # 定义简单的流水线
    steps = [
        LoadStockDataStep(context),
        CalculateFactorsStep(context),
        SimpleAnalysisStep(context)
    ]
    
    pipeline = ResearchPipeline(steps, name="基础因子计算")
    
    # 运行流水线
    params = {
        'stock_list': ['000001.SZ', '000002.SZ'],  # 只分析两只股票
        'start_date': '2024-01-01',
        'end_date': '2024-01-31',
        'ma_windows': [5, 20],  # 只计算5日和20日均线
    }
    
    pipeline.run(params)
    
    # 清理
    context.close()


def example_2_custom_factor():
    """示例2: 添加自定义因子"""
    print("\n" + "="*80)
    print("示例2: 自定义因子计算")
    print("="*80)
    
    class CustomFactorStep(Step):
        """计算自定义因子的步骤"""
        
        def run(self, **kwargs):
            df = kwargs.get('stock_data', pd.DataFrame())
            
            if df.empty:
                return {'custom_factors': pd.DataFrame()}
            
            print("计算自定义因子...")
            
            # 按股票分组计算
            result_list = []
            for ts_code, group in df.groupby('ts_code'):
                group = group.sort_values('trade_date').copy()
                
                # 计算价格动量
                group['momentum_5'] = group['close'].pct_change(5) * 100
                group['momentum_10'] = group['close'].pct_change(10) * 100
                
                # 计算波动率
                group['volatility_5'] = group['pct_chg'].rolling(5).std()
                
                # 计算成交量变化率
                group['vol_change'] = group['vol'].pct_change()
                
                result_list.append(group)
            
            custom_factors = pd.concat(result_list, ignore_index=True)
            print(f"自定义因子计算完成，新增 4 个因子")
            
            # 显示最新的自定义因子值
            latest = custom_factors.nlargest(5, 'trade_date')[
                ['ts_code', 'trade_date', 'momentum_5', 'momentum_10', 'volatility_5']
            ]
            print("\n最新自定义因子值:")
            print(latest)
            
            return {'factor_data': custom_factors}
    
    # 创建上下文和流水线
    context = ResearchContext(Path(__file__).parent)
    
    steps = [
        LoadStockDataStep(context),
        CustomFactorStep(context),  # 使用自定义因子步骤
        SimpleAnalysisStep(context)
    ]
    
    pipeline = ResearchPipeline(steps, name="自定义因子研究")
    
    # 运行
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    params = {
        'stock_list': ['600519.SH'],  # 贵州茅台
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
    }
    
    pipeline.run(params)
    context.close()


def example_3_batch_analysis():
    """示例3: 批量股票分析"""
    print("\n" + "="*80)
    print("示例3: 批量股票分析")
    print("="*80)
    
    context = ResearchContext(Path(__file__).parent)
    
    # 获取股票列表（这里用固定列表演示）
    stock_list = [
        '000001.SZ', '000002.SZ', '000858.SZ',  # 深市
        '600000.SH', '600036.SH', '600519.SH'   # 沪市
    ]
    
    print(f"准备分析 {len(stock_list)} 只股票...")
    
    # 创建带有保存功能的流水线
    from src.steps import SaveResultsStep
    
    steps = [
        LoadStockDataStep(context),
        CalculateFactorsStep(context),
        SaveResultsStep(context)  # 保存结果
    ]
    
    pipeline = ResearchPipeline(steps, name="批量股票因子分析")
    
    params = {
        'stock_list': stock_list,
        'start_date': '2024-01-01',
        'end_date': '2024-03-31',
        'ma_windows': [5, 10, 20, 60],
        'save_to_csv': True,
        'output_dir': 'batch_output'
    }
    
    pipeline.run(params)
    
    # 显示执行摘要
    summary = pipeline.get_summary()
    print(f"\n执行完成! 耗时: {summary['execution_time']:.2f}秒")
    
    context.close()


def main():
    """运行所有示例"""
    print("\n" + "="*80)
    print("AlphaHome Research 模板使用示例")
    print("="*80)
    
    # 选择要运行的示例
    examples = {
        '1': ('基础用法', example_1_basic_usage),
        '2': ('自定义因子', example_2_custom_factor),
        '3': ('批量分析', example_3_batch_analysis)
    }
    
    print("\n可用示例:")
    for key, (name, _) in examples.items():
        print(f"  {key}. {name}")
    
    choice = input("\n请选择要运行的示例 (1-3, 或 'all' 运行全部): ").strip()
    
    if choice == 'all':
        for name, func in examples.values():
            func()
    elif choice in examples:
        examples[choice][1]()
    else:
        print("无效选择，运行默认示例...")
        example_1_basic_usage()


if __name__ == "__main__":
    main()
