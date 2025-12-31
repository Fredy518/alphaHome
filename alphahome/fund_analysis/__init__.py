"""
基金分析模块 (fund_analysis)

本模块提供独立的基金绩效分析能力，可用于：
1. 分析单只基金的绩效表现
2. 分析基金组合回测结果
3. 生成绩效报告和可视化

主要组件：
- PerformanceAnalyzer: 统一入口，聚合所有分析功能
- MetricsAnalyzer: 基础绩效指标计算
- DrawdownAnalyzer: 回撤分析
- PeriodicAnalyzer: 周期性分析
- RiskAnalyzer: 风险分析
- AttributionAnalyzer: 归因分析
- ReportBuilder: 报告生成
- Visualization: 可视化（需要 matplotlib）

使用示例：
    from alphahome.fund_analysis import PerformanceAnalyzer
    
    analyzer = PerformanceAnalyzer()
    metrics = analyzer.calculate_metrics(returns, nav_series)
    
    # 生成报告
    report = analyzer.generate_report(nav_series, format='dict')
    
    # 可视化（需要 matplotlib）
    fig, ax = analyzer.plot_nav(nav_series)
"""

# 版本信息
__version__ = "1.0.0"

# 导出常量（供外部使用）
from ._constants import (
    DEFAULT_PERIODS_PER_YEAR,
    DEFAULT_RISK_FREE_RATE,
    FFILL_LIMIT,
)

# 导出 schema 定义
from ._schema import (
    METRICS_SCHEMA,
    DRAWDOWN_SCHEMA,
    PERIODIC_SCHEMA,
    RISK_SCHEMA,
    ATTRIBUTION_SCHEMA,
)

# 导出分析器类
from .metrics import MetricsAnalyzer
from .drawdown import DrawdownAnalyzer
from .periodic import PeriodicAnalyzer
from .risk import RiskAnalyzer
from .attribution import AttributionAnalyzer
from .performance import PerformanceAnalyzer
from .report import ReportBuilder

# 延迟导入 Visualization（避免 matplotlib 依赖问题）
def __getattr__(name):
    if name == "Visualization":
        from .visualization import Visualization
        return Visualization
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

# 公共 API 导出列表
__all__ = [
    # 常量
    "DEFAULT_PERIODS_PER_YEAR",
    "DEFAULT_RISK_FREE_RATE",
    "FFILL_LIMIT",
    # Schema
    "METRICS_SCHEMA",
    "DRAWDOWN_SCHEMA",
    "PERIODIC_SCHEMA",
    "RISK_SCHEMA",
    "ATTRIBUTION_SCHEMA",
    # 分析器类
    "PerformanceAnalyzer",
    "MetricsAnalyzer",
    "DrawdownAnalyzer",
    "PeriodicAnalyzer",
    "RiskAnalyzer",
    "AttributionAnalyzer",
    # 报告和可视化
    "ReportBuilder",
    "Visualization",
]
