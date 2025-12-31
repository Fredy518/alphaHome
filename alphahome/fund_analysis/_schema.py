"""
输出 Schema 定义 - 基金分析模块的输出数据结构规范

本文件定义了各分析器输出的固定 key 列表和语义约束。
这些 schema 确保输出格式的一致性，便于下游消费和测试验证。

重要说明：
- to_dict() 是唯一的 JSON 序列化出口
- 测试时校验 key 完整性与可序列化性
- 所有数值类型为 float，缺失值为 NaN（JSON 中为 null）
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypedDict, Union
from enum import Enum


# =============================================================================
# 类型定义
# =============================================================================

class MetricType(Enum):
    """指标类型枚举"""
    FLOAT = "float"      # 浮点数
    INT = "int"          # 整数
    PERCENT = "percent"  # 百分比（存储为小数，如 0.1 表示 10%）
    DATE = "date"        # 日期（字符串格式 YYYY-MM-DD）
    NULLABLE = "nullable"  # 可为 None/NaN


@dataclass
class MetricDefinition:
    """指标定义"""
    name: str           # 指标名称
    type: MetricType    # 数据类型
    nullable: bool      # 是否可为空
    description: str    # 描述说明
    unit: str = ""      # 单位（如 '%', '倍', '天'）


# =============================================================================
# METRICS_SCHEMA - 基础绩效指标输出结构
# =============================================================================

METRICS_SCHEMA: Dict[str, MetricDefinition] = {
    # === 收益指标 ===
    "cumulative_return": MetricDefinition(
        name="累计收益率",
        type=MetricType.PERCENT,
        nullable=True,
        description="期间累计收益率: (1+r1)*(1+r2)*...*(1+rn) - 1",
        unit="%"
    ),
    "annualized_return": MetricDefinition(
        name="年化收益率",
        type=MetricType.PERCENT,
        nullable=True,
        description="年化收益率 (CAGR): (1 + cum_ret) ^ (periods_per_year / n) - 1",
        unit="%"
    ),
    
    # === 风险指标 ===
    "annualized_volatility": MetricDefinition(
        name="年化波动率",
        type=MetricType.PERCENT,
        nullable=True,
        description="年化波动率: std(r) * sqrt(periods_per_year)",
        unit="%"
    ),
    "max_drawdown": MetricDefinition(
        name="最大回撤",
        type=MetricType.PERCENT,
        nullable=True,
        description="最大回撤（正数）: abs(min(underwater_curve))",
        unit="%"
    ),
    "max_drawdown_start": MetricDefinition(
        name="最大回撤开始日期",
        type=MetricType.DATE,
        nullable=True,
        description="最大回撤峰值日期",
        unit=""
    ),
    "max_drawdown_end": MetricDefinition(
        name="最大回撤结束日期",
        type=MetricType.DATE,
        nullable=True,
        description="最大回撤谷值日期",
        unit=""
    ),
    "var_95": MetricDefinition(
        name="风险价值(95%)",
        type=MetricType.PERCENT,
        nullable=True,
        description="95%置信水平下的VaR（负数收益阈值）",
        unit="%"
    ),
    "cvar_95": MetricDefinition(
        name="条件风险价值(95%)",
        type=MetricType.PERCENT,
        nullable=True,
        description="95%置信水平下的CVaR，CVaR <= VaR",
        unit="%"
    ),
    
    # === 风险调整收益指标 ===
    "sharpe_ratio": MetricDefinition(
        name="夏普比率",
        type=MetricType.FLOAT,
        nullable=True,
        description="夏普比率: (ann_ret - rf) / ann_vol",
        unit=""
    ),
    "sortino_ratio": MetricDefinition(
        name="索提诺比率",
        type=MetricType.FLOAT,
        nullable=True,
        description="索提诺比率: (ann_ret - mar) / downside_deviation",
        unit=""
    ),
    "calmar_ratio": MetricDefinition(
        name="卡玛比率",
        type=MetricType.FLOAT,
        nullable=True,
        description="卡玛比率: CAGR / |最大回撤|",
        unit=""
    ),
    "information_ratio": MetricDefinition(
        name="信息比率",
        type=MetricType.FLOAT,
        nullable=True,
        description="信息比率: mean(r_p - r_b) / std(r_p - r_b) * sqrt(periods_per_year)，需要基准",
        unit=""
    ),
    
    # === 胜率与盈亏 ===
    "win_rate": MetricDefinition(
        name="胜率",
        type=MetricType.PERCENT,
        nullable=True,
        description="胜率: count(r > 0) / count(r)，范围 [0, 1]",
        unit="%"
    ),
    "profit_loss_ratio": MetricDefinition(
        name="盈亏比",
        type=MetricType.FLOAT,
        nullable=True,
        description="盈亏比: mean(r | r > 0) / |mean(r | r < 0)|，无亏损时为 inf",
        unit="倍"
    ),
    
    # === 相对指标（需要基准）===
    "tracking_error": MetricDefinition(
        name="跟踪误差",
        type=MetricType.PERCENT,
        nullable=True,
        description="跟踪误差: std(r_p - r_b) * sqrt(periods_per_year)，需要基准",
        unit="%"
    ),
    "beta": MetricDefinition(
        name="Beta",
        type=MetricType.FLOAT,
        nullable=True,
        description="Beta: cov(r_p, r_b) / var(r_b)，需要基准",
        unit=""
    ),
    "excess_return": MetricDefinition(
        name="超额收益",
        type=MetricType.PERCENT,
        nullable=True,
        description="超额收益: 组合年化收益 - 基准年化收益，需要基准",
        unit="%"
    ),
    
    # === 统计信息 ===
    "total_days": MetricDefinition(
        name="总交易日数",
        type=MetricType.INT,
        nullable=False,
        description="收益率序列的总交易日数",
        unit="天"
    ),
}

# METRICS_SCHEMA 的 key 列表（用于验证）
METRICS_SCHEMA_KEYS: List[str] = list(METRICS_SCHEMA.keys())


# =============================================================================
# DRAWDOWN_SCHEMA - 回撤分析输出结构
# =============================================================================

@dataclass
class DrawdownPeriodSchema:
    """单个回撤周期的数据结构"""
    peak_date: Optional[str]       # 峰值日期 (YYYY-MM-DD)
    trough_date: Optional[str]     # 谷值日期 (YYYY-MM-DD)
    recovery_date: Optional[str]   # 恢复日期 (YYYY-MM-DD)，未恢复为 None
    drawdown: float                # 回撤幅度（正数，如 0.1 表示 10%）
    duration_days: int             # 峰值到谷值的交易日数
    recovery_days: Optional[int]   # 谷值到恢复的交易日数，未恢复为 None


DRAWDOWN_SCHEMA: Dict[str, MetricDefinition] = {
    "max_drawdown": MetricDefinition(
        name="最大回撤",
        type=MetricType.PERCENT,
        nullable=True,
        description="最大回撤（正数）",
        unit="%"
    ),
    "max_drawdown_start": MetricDefinition(
        name="最大回撤开始日期",
        type=MetricType.DATE,
        nullable=True,
        description="最大回撤峰值日期",
        unit=""
    ),
    "max_drawdown_end": MetricDefinition(
        name="最大回撤结束日期",
        type=MetricType.DATE,
        nullable=True,
        description="最大回撤谷值日期",
        unit=""
    ),
    "max_drawdown_recovery": MetricDefinition(
        name="最大回撤恢复日期",
        type=MetricType.DATE,
        nullable=True,
        description="最大回撤恢复日期，未恢复为 None",
        unit=""
    ),
    "avg_drawdown_duration": MetricDefinition(
        name="平均回撤持续时间",
        type=MetricType.FLOAT,
        nullable=True,
        description="已恢复回撤的平均持续时间",
        unit="天"
    ),
    "max_drawdown_duration": MetricDefinition(
        name="最大回撤持续时间",
        type=MetricType.INT,
        nullable=True,
        description="最大回撤的持续时间（峰值到谷值）",
        unit="天"
    ),
    "top_n_drawdowns": MetricDefinition(
        name="前N大回撤",
        type=MetricType.NULLABLE,
        nullable=True,
        description="前N大回撤周期列表，每个元素为 DrawdownPeriodSchema",
        unit=""
    ),
    "underwater_curve": MetricDefinition(
        name="水下曲线",
        type=MetricType.NULLABLE,
        nullable=True,
        description="水下曲线序列，值 <= 0",
        unit=""
    ),
}

# DRAWDOWN_SCHEMA 的 key 列表（用于验证）
DRAWDOWN_SCHEMA_KEYS: List[str] = list(DRAWDOWN_SCHEMA.keys())


# =============================================================================
# PERIODIC_SCHEMA - 周期性分析输出结构
# =============================================================================

PERIODIC_SCHEMA: Dict[str, MetricDefinition] = {
    "monthly_returns": MetricDefinition(
        name="月度收益矩阵",
        type=MetricType.NULLABLE,
        nullable=True,
        description="月度收益矩阵，索引=年份，列=月份(1-12)",
        unit=""
    ),
    "quarterly_returns": MetricDefinition(
        name="季度收益",
        type=MetricType.NULLABLE,
        nullable=True,
        description="季度收益序列，索引='YYYY-QN'",
        unit=""
    ),
    "yearly_returns": MetricDefinition(
        name="年度收益",
        type=MetricType.NULLABLE,
        nullable=True,
        description="年度收益序列，索引=年份",
        unit=""
    ),
    "rolling_return": MetricDefinition(
        name="滚动收益",
        type=MetricType.NULLABLE,
        nullable=True,
        description="滚动收益序列，前 window-1 个值为 NaN",
        unit=""
    ),
    "rolling_sharpe": MetricDefinition(
        name="滚动夏普比率",
        type=MetricType.NULLABLE,
        nullable=True,
        description="滚动夏普比率序列",
        unit=""
    ),
    "rolling_volatility": MetricDefinition(
        name="滚动波动率",
        type=MetricType.NULLABLE,
        nullable=True,
        description="滚动波动率序列（已年化）",
        unit=""
    ),
}

# PERIODIC_SCHEMA 的 key 列表（用于验证）
PERIODIC_SCHEMA_KEYS: List[str] = list(PERIODIC_SCHEMA.keys())


# =============================================================================
# RISK_SCHEMA - 风险分析输出结构
# =============================================================================

RISK_SCHEMA: Dict[str, MetricDefinition] = {
    "tracking_error": MetricDefinition(
        name="跟踪误差",
        type=MetricType.PERCENT,
        nullable=True,
        description="跟踪误差，需要基准",
        unit="%"
    ),
    "beta": MetricDefinition(
        name="Beta",
        type=MetricType.FLOAT,
        nullable=True,
        description="Beta系数，需要基准",
        unit=""
    ),
    "correlation_matrix": MetricDefinition(
        name="相关性矩阵",
        type=MetricType.NULLABLE,
        nullable=True,
        description="持仓收益相关性矩阵，对称且对角线为1",
        unit=""
    ),
    "hhi": MetricDefinition(
        name="赫芬达尔指数",
        type=MetricType.FLOAT,
        nullable=True,
        description="HHI集中度指标，范围 [1/n, 1]",
        unit=""
    ),
    "top_n_concentration": MetricDefinition(
        name="前N大持仓集中度",
        type=MetricType.PERCENT,
        nullable=True,
        description="前N大持仓的权重之和",
        unit="%"
    ),
    "turnover_rate": MetricDefinition(
        name="换手率",
        type=MetricType.PERCENT,
        nullable=True,
        description="年化换手率",
        unit="%"
    ),
}

# RISK_SCHEMA 的 key 列表（用于验证）
RISK_SCHEMA_KEYS: List[str] = list(RISK_SCHEMA.keys())


# =============================================================================
# ATTRIBUTION_SCHEMA - 归因分析输出结构
# =============================================================================

ATTRIBUTION_SCHEMA: Dict[str, MetricDefinition] = {
    "contribution": MetricDefinition(
        name="贡献分析",
        type=MetricType.NULLABLE,
        nullable=True,
        description="各资产贡献，sum == 组合收益",
        unit=""
    ),
    "allocation_effect": MetricDefinition(
        name="配置效应",
        type=MetricType.PERCENT,
        nullable=True,
        description="Brinson配置效应，需要基准权重",
        unit="%"
    ),
    "selection_effect": MetricDefinition(
        name="选择效应",
        type=MetricType.PERCENT,
        nullable=True,
        description="Brinson选择效应，需要基准权重",
        unit="%"
    ),
    "interaction_effect": MetricDefinition(
        name="交互效应",
        type=MetricType.PERCENT,
        nullable=True,
        description="Brinson交互效应，需要基准权重",
        unit="%"
    ),
    "total_active_return": MetricDefinition(
        name="总主动收益",
        type=MetricType.PERCENT,
        nullable=True,
        description="三效应之和 == 超额收益",
        unit="%"
    ),
}

# ATTRIBUTION_SCHEMA 的 key 列表（用于验证）
ATTRIBUTION_SCHEMA_KEYS: List[str] = list(ATTRIBUTION_SCHEMA.keys())


# =============================================================================
# 序列化辅助函数
# =============================================================================

def validate_metrics_output(output: Dict[str, Any]) -> bool:
    """
    验证指标输出是否符合 METRICS_SCHEMA
    
    Args:
        output: 待验证的输出字典
    
    Returns:
        bool: 是否通过验证
    
    Raises:
        ValueError: 当验证失败时抛出，包含具体错误信息
    """
    missing_keys = set(METRICS_SCHEMA_KEYS) - set(output.keys())
    if missing_keys:
        raise ValueError(f"输出缺少必需的 key: {missing_keys}")
    return True


def is_json_serializable(obj: Any) -> bool:
    """
    检查对象是否可 JSON 序列化
    
    Args:
        obj: 待检查的对象
    
    Returns:
        bool: 是否可序列化
    """
    import json
    import numpy as np
    import pandas as pd
    
    # 处理 numpy/pandas 类型
    if isinstance(obj, (np.integer, np.floating)):
        return True
    if isinstance(obj, np.ndarray):
        return False  # 需要先转换为 list
    if isinstance(obj, (pd.Series, pd.DataFrame)):
        return False  # 需要先转换
    if pd.isna(obj):
        return True  # NaN 会被转换为 null
    
    try:
        json.dumps(obj)
        return True
    except (TypeError, ValueError):
        return False
