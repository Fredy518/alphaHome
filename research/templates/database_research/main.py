"""
数据库驱动的研究项目主程序

展示如何使用ResearchContext和ResearchPipeline构建完整的研究流程
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from research.tools.context import ResearchContext
from research.tools.pipeline import ResearchPipeline, Step

from src.steps import (
    LoadStockDataStep,
    CalculateFactorsStep,
    FactorICAnalysisStep,
    FactorQuantileAnalysisStep,
)


class ForwardReturnCalculationStep(Step):
    """计算未来收益率，为IC和分位数分析做准备"""

    def run(self, **kwargs) -> Dict[str, Any]:
        """执行计算"""
        import logging
        logger = logging.getLogger(self.__class__.__name__)
        logger.info("开始计算未来收益率...")
        factor_data = kwargs.get("factor_data")
        if factor_data is None or factor_data.empty:
            logger.warning("因子数据为空，跳过未来收益率计算。")
            return {}

        # 使用 assign 来创建新列，避免 SettingWithCopyWarning
        factor_data = factor_data.assign(
            forward_return=factor_data.groupby("ts_code")["close"]
            .pct_change(-5)
            .shift(5)
        )
        logger.info("未来收益率计算完成。")
        return {"factor_data": factor_data}


def create_factor_research_pipeline(context: ResearchContext) -> ResearchPipeline:
    """
    创建一个完整的研究流水线，包括数据加载、因子计算和多维度因子分析。

    Args:
        context: ResearchContext实例

    Returns:
        配置好的ResearchPipeline实例
    """
    # 1. 提取因子配置
    factor_analysis_config = context.config.get("factor_analysis", {})
    ic_factors = factor_analysis_config.get("ic_factors", [])
    quantile_factor = factor_analysis_config.get("quantile_factor", "ma_20")

    if not ic_factors:
        # 使用默认因子列表
        ic_factors = ['ma_5', 'ma_10', 'ma_20', 'ma_60', 'volume_ma_20', 'returns', 'volatility_20', 'rsi_14']
        logger.warning("配置文件中未找到因子定义，使用默认因子列表")

    # 获取研究参数配置
    research_config = context.config.get("research_params", {})

    # 2. 定义研究流水线步骤（使用配置参数）
    steps = [
        # 步骤1：加载数据
        LoadStockDataStep(context),
        # 步骤2：计算因子（参数明确传入）
        CalculateFactorsStep(
            context,
            ma_windows=research_config.get('ma_windows', [5, 10, 20, 60]),
            volume_window=research_config.get('volume_window', 20)
        ),
        # 步骤3：计算未来收益率
        ForwardReturnCalculationStep(context),
        # 步骤4：因子IC分析
        FactorICAnalysisStep(context, factor_cols=ic_factors),
        # 步骤5：因子分位数分析
        FactorQuantileAnalysisStep(context, factor_col=quantile_factor),
    ]

    # 3. 创建并返回研究流水线
    return ResearchPipeline(steps=steps, name="一站式因子研究流水线")


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """主函数"""
    print("=" * 80)
    print("AlphaHome 数据库驱动研究项目")
    print("=" * 80)
    
    # 1. 初始化研究上下文
    logger.info("初始化研究上下文...")
    project_path = Path(__file__).parent
    
    try:
        context = ResearchContext(project_path)
        logger.info("研究上下文初始化成功")
    except Exception as e:
        logger.error(f"初始化失败: {e}")
        return
    
    # 2. 从配置文件加载研究参数
    config_params = context.config.get('research_params', {})

    # 处理日期范围
    if 'start_date' in config_params and 'end_date' in config_params:
        # 使用配置文件中的固定日期
        start_date = config_params['start_date']
        end_date = config_params['end_date']
    else:
        # 使用动态日期计算
        days_back = config_params.get('days_back', 90)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')

    # 构建研究参数（配置文件 + 动态计算）
    research_params = {
        'stock_list': config_params.get('stock_list', ['000001.SZ', '000002.SZ']),
        'start_date': start_date,
        'end_date': end_date,
        'ma_windows': config_params.get('ma_windows', [5, 10, 20, 60]),
        'volume_window': config_params.get('volume_window', 20),
        'save_to_csv': config_params.get('save_to_csv', True),
        'save_to_db': config_params.get('save_to_db', False),
        'output_dir': config_params.get('output_dir', 'output')
    }
    
    logger.info(f"研究参数: {research_params}")
    
    # 3. 创建并运行流水线
    try:
        pipeline = create_factor_research_pipeline(context)
        logger.info("开始运行研究流水线...")
        
        # 运行流水线
        pipeline.run(initial_params=research_params)
        
        # 获取执行摘要
        summary = pipeline.get_summary()
        
        # 打印结果摘要
        print("\n" + "=" * 80)
        print("流水线执行摘要")
        print("=" * 80)
        print(f"总步骤数: {summary['total_steps']}")
        print(f"成功步骤: {summary['successful_steps']}")
        print(f"失败步骤: {summary['failed_steps']}")
        print(f"执行时间: {summary['execution_time']:.2f}秒")
        
        # 保存流水线结果
        result_file = project_path / 'output' / f"pipeline_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        result_file.parent.mkdir(exist_ok=True)
        pipeline.save_results(str(result_file))
        print(f"\n结果已保存至: {result_file}")
        
    except Exception as e:
        logger.error(f"流水线执行失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 清理资源
        context.close()
        logger.info("资源清理完成")
    
    print("\n" + "=" * 80)
    print("研究完成！")
    print("=" * 80)


if __name__ == "__main__":
    main()
