"""
简化的研究工具使用示例

展示如何使用新的工具快速构建研究流水线，专注于实用性而非复杂性
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_1_basic_factor_research():
    """示例1：基础因子研究 - 最简单的使用方式"""
    print("=== 示例1：基础因子研究 ===")
    
    from .context import ResearchContext
    from .step_factory import create_pipeline_builder
    
    # 1. 创建研究上下文（自动处理配置和连接）
    with ResearchContext() as context:
        # 2. 获取一些股票进行测试
        stock_list = context.get_stock_list()[:5]  # 只取5只股票做示例
        
        # 3. 设置时间范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
        
        # 4. 构建流水线 - 链式调用，简单直观
        pipeline = (create_pipeline_builder(context)
                   .name("基础因子研究")
                   .add_data_load(
                       start_date=start_date,
                       end_date=end_date,
                       stock_list=stock_list
                   )
                   .add_data_clean()  # 清洗数据
                   .add_moving_average(windows=[5, 20])  # 计算移动平均
                   .add_rsi()  # 计算RSI
                   .add_volume_features()  # 计算成交量特征
                   .add_save_results(save_to_csv=True)
                   .build())
        
        # 5. 运行流水线
        pipeline.run()
        
        # 6. 查看结果
        summary = pipeline.get_summary()
        print(f"流水线执行完成: {summary['successful_steps']}/{summary['total_steps']} 步骤成功")


def example_2_custom_factor_with_backtest():
    """示例2：自定义因子 + 简单回测"""
    print("\n=== 示例2：自定义因子 + 简单回测 ===")
    
    from .context import ResearchContext
    from .step_factory import create_pipeline_builder
    from .pipeline import Step
    
    # 自定义一个简单的因子计算步骤
    class SimpleSignalStep(Step):
        """简单信号生成：MA5 > MA20 时买入"""
        
        def run(self, **kwargs):
            data = kwargs.get('factor_data')
            if data is None or data.empty:
                return kwargs
            
            # 生成简单的交易信号
            data['signal'] = 0
            data.loc[data['ma_5'] > data['ma_20'], 'signal'] = 1
            data.loc[data['ma_5'] < data['ma_20'], 'signal'] = -1
            
            print(f"生成信号: 买入信号 {(data['signal'] == 1).sum()} 个，卖出信号 {(data['signal'] == -1).sum()} 个")
            
            result = kwargs.copy()
            result['factor_data'] = data
            return result
    
    with ResearchContext() as context:
        stock_list = ['000001.SZ', '000002.SZ']  # 只测试2只股票
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')
        
        # 构建包含自定义步骤的流水线
        pipeline = (create_pipeline_builder(context)
                   .name("自定义因子回测")
                   .add_data_load(start_date=start_date, end_date=end_date, stock_list=stock_list)
                   .add_moving_average(windows=[5, 20])
                   .build())
        
        # 手动添加自定义步骤
        pipeline.add_step(SimpleSignalStep(context))
        
        # 添加回测步骤
        from .step_factory import create_simple_backtest_step
        backtest_step = create_simple_backtest_step(context)
        pipeline.add_step(backtest_step)
        
        # 运行流水线
        pipeline.run()
        
        # 查看回测结果
        if pipeline.results:
            last_result = pipeline.results[-1].get('output', {})
            backtest_stats = last_result.get('backtest_stats', {})
            
            print("回测结果:")
            for stock, stats in backtest_stats.items():
                print(f"  {stock}: 总收益 {stats['total_return']:.2%}, "
                      f"胜率 {stats['win_rate']:.2%}, "
                      f"夏普比率 {stats['sharpe_ratio']:.2f}")


def example_3_manual_pipeline():
    """示例3：手动构建流水线"""
    print("\n=== 示例3：手动构建流水线 ===")

    from .context import ResearchContext
    from .step_factory import (
        create_data_load_step, create_data_clean_step,
        create_moving_average_step, create_rsi_step, create_save_results_step
    )
    from .pipeline import ResearchPipeline

    with ResearchContext() as context:
        # 手动创建步骤
        steps = [
            create_data_load_step(context, '20240101', '20240331',
                                stock_list=['000001.SZ', '600519.SH']),
            create_data_clean_step(context, remove_outliers=True, outlier_std=2),
            create_moving_average_step(context, windows=[10, 30]),
            create_rsi_step(context, window=14),
            create_save_results_step(context, save_to_csv=True, output_dir='manual_output')
        ]

        # 创建流水线
        pipeline = ResearchPipeline(steps, '手动构建的研究流水线')

        # 运行流水线
        pipeline.run()

        print(f"手动构建的流水线执行完成")


def example_4_quick_exploration():
    """示例4：快速探索 - 研究人员最常用的场景"""
    print("\n=== 示例4：快速探索 ===")
    
    from .context import ResearchContext
    
    # 最简单的使用方式：使用providers数据提供层
    with ResearchContext() as context:
        # 使用data_tool获取股票数据（替代直接SQL查询）
        df = context.get_stock_data(
            symbols=['000001.SZ', '000002.SZ'],
            start_date='2024-03-01',
            end_date='2024-03-31'
        )
        print(f"查询到 {len(df)} 条数据")

        # 快速计算一些指标
        df['ma_5'] = df.groupby('ts_code')['close'].rolling(5).mean().values
        df['vol_ma'] = df.groupby('ts_code')['vol'].rolling(20).mean().values

        # 简单分析
        print("基础统计:")
        print(df.groupby('ts_code')['pct_chg'].agg(['mean', 'std', 'min', 'max']))

        # 保存结果
        output_path = Path('output') / 'quick_exploration.csv'
        output_path.parent.mkdir(exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"结果已保存到: {output_path}")


def show_available_steps():
    """显示所有可用的步骤创建函数"""
    print("\n=== 可用的步骤创建函数 ===")

    steps_info = {
        "数据处理": [
            ("create_data_load_step", "创建数据加载步骤", ["start_date", "end_date"]),
            ("create_data_clean_step", "创建数据清洗步骤", ["remove_outliers", "fill_missing"]),
        ],
        "因子计算": [
            ("create_moving_average_step", "创建移动平均线计算步骤", ["windows"]),
            ("create_rsi_step", "创建RSI计算步骤", ["window"]),
            ("create_volume_features_step", "创建成交量特征计算步骤", ["window"]),
            ("create_price_features_step", "创建价格特征计算步骤", []),
        ],
        "分析和输出": [
            ("create_simple_backtest_step", "创建简单回测步骤", ["signal_column"]),
            ("create_save_results_step", "创建结果保存步骤", ["save_to_csv", "output_dir"]),
        ]
    }

    for category, functions in steps_info.items():
        print(f"\n{category}:")
        for func_name, description, params in functions:
            print(f"  - {func_name}: {description}")
            if params:
                print(f"    常用参数: {params}")


if __name__ == '__main__':
    """运行所有示例"""
    print("AlphaHome 研究工具使用示例")
    print("=" * 50)
    
    # 显示可用步骤
    show_available_steps()
    
    # 运行示例（注释掉需要数据库连接的部分）
    print("\n注意：以下示例需要配置数据库连接才能运行")
    print("请确保 config.yml 中的数据库配置正确")
    
    # example_1_basic_factor_research()
    # example_2_custom_factor_with_backtest()
    # example_3_manual_pipeline()
    # example_4_quick_exploration()
    
    print("\n示例代码展示完成！")
    print("这些示例展示了如何:")
    print("1. 快速构建研究流水线")
    print("2. 添加自定义步骤")
    print("3. 手动构建复杂流水线")
    print("4. 进行快速数据探索")
    print("\n简化后的API更加直观易用，专注于实用性！")
