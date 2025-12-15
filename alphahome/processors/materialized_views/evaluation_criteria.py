"""\
物化视图适用范围评估标准

定义了 7 个评估维度，用于判断一个任务是否适合转换为物化视图。
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field


class SuitabilityLevel(str, Enum):
    """物化视图适用度等级"""

    HIGHLY_SUITABLE = "highly_suitable"
    MODERATELY_SUITABLE = "moderately_suitable"
    NOT_SUITABLE = "not_suitable"


@dataclass
class EvaluationDimension:
    """评估维度定义"""

    name: str
    description: str
    weight: str
    criteria: str
    example_pass: str
    example_fail: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "name": self.name,
            "description": self.description,
            "weight": self.weight,
            "criteria": self.criteria,
            "example_pass": self.example_pass,
            "example_fail": self.example_fail,
        }


@dataclass
class SuitabilityScore:
    """适用度评分"""

    total_score: int
    dimension_scores: Dict[str, bool] = field(default_factory=dict)
    level: SuitabilityLevel = SuitabilityLevel.NOT_SUITABLE
    recommendation: str = "keep_as_python"
    reasoning: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_score": self.total_score,
            "dimension_scores": self.dimension_scores,
            "level": self.level.value,
            "recommendation": self.recommendation,
            "reasoning": self.reasoning,
        }


class EvaluationCriteria:
    """物化视图适用范围评估标准。"""

    DIMENSIONS: List[EvaluationDimension] = [
        EvaluationDimension(
            name="数据源简洁性",
            description="数据源越少，SQL 越简洁，越适合物化视图",
            weight="高",
            criteria="数据源 ≤ 3 个 rawdata 表",
            example_pass="从 rawdata.stock_daily 和 rawdata.stock_basic 读取（2 个表）",
            example_fail="从 5 个不同的数据源表读取数据",
        ),
        EvaluationDimension(
            name="转换逻辑复杂度",
            description="转换逻辑可用 SQL 表达，不涉及复杂业务规则",
            weight="高",
            criteria="转换逻辑可用 SQL 表达（不涉及复杂业务规则）",
            example_pass="数据对齐、格式标准化、简单聚合",
            example_fail="需要调用外部 API、复杂的机器学习模型、动态规则引擎",
        ),
        EvaluationDimension(
            name="计算复杂度",
            description="计算逻辑为标准操作，避免复杂的数学计算",
            weight="中",
            criteria="计算逻辑为标准操作（聚合、JOIN、窗口函数）",
            example_pass="SUM、AVG、ROW_NUMBER、LEAD/LAG 等标准 SQL 函数",
            example_fail="复杂的数学计算、迭代算法、条件判断链",
        ),
        EvaluationDimension(
            name="输出数据量",
            description="物化视图存储成本可控",
            weight="中",
            criteria="输出行数 < 1000 万",
            example_pass="输出 100 万行数据",
            example_fail="输出 5000 万行数据",
        ),
        EvaluationDimension(
            name="更新频率",
            description="高频更新不适合物化视图",
            weight="中",
            criteria="日更或更低频",
            example_pass="每天更新一次",
            example_fail="每分钟更新一次",
        ),
        EvaluationDimension(
            name="查询复杂度",
            description="物化视图的主要收益来自复杂查询的加速",
            weight="低",
            criteria="原始查询的执行时间 > 1 秒",
            example_pass="原始查询耗时 5 秒",
            example_fail="原始查询耗时 100 毫秒",
        ),
        EvaluationDimension(
            name="复用度",
            description="复用度高的查询优先物化",
            weight="低",
            criteria="被 2+ 个应用或报告使用",
            example_pass="被 3 个不同的应用使用",
            example_fail="仅被 1 个应用使用",
        ),
    ]

    @classmethod
    def get_dimensions(cls) -> List[EvaluationDimension]:
        return cls.DIMENSIONS

    @classmethod
    def get_dimension_by_name(cls, name: str) -> Optional[EvaluationDimension]:
        for dim in cls.DIMENSIONS:
            if dim.name == name:
                return dim
        return None

    @classmethod
    def evaluate(cls, dimension_scores: Dict[str, bool]) -> SuitabilityScore:
        valid_dimensions = {dim.name for dim in cls.DIMENSIONS}
        for dim_name in dimension_scores.keys():
            if dim_name not in valid_dimensions:
                raise ValueError(
                    f"未知的评估维度: {dim_name}。" f"有效的维度: {valid_dimensions}"
                )

        total_score = sum(1 for v in dimension_scores.values() if v)

        if total_score >= 4:
            level = SuitabilityLevel.HIGHLY_SUITABLE
            recommendation = "migrate_now"
        elif total_score >= 2:
            level = SuitabilityLevel.MODERATELY_SUITABLE
            recommendation = "migrate_later"
        else:
            level = SuitabilityLevel.NOT_SUITABLE
            recommendation = "keep_as_python"

        satisfied_dims = [name for name, satisfied in dimension_scores.items() if satisfied]
        reasoning = (
            f"满足 {total_score}/7 个维度: {', '.join(satisfied_dims) if satisfied_dims else '无'}。"
            f"建议: {recommendation}"
        )

        return SuitabilityScore(
            total_score=total_score,
            dimension_scores=dimension_scores,
            level=level,
            recommendation=recommendation,
            reasoning=reasoning,
        )

    @classmethod
    def get_evaluation_framework(cls) -> Dict[str, Any]:
        return {
            "total_dimensions": len(cls.DIMENSIONS),
            "dimensions": [dim.to_dict() for dim in cls.DIMENSIONS],
            "suitability_levels": {
                "highly_suitable": "4+ 维度满足",
                "moderately_suitable": "2-3 维度满足",
                "not_suitable": "0-1 维度满足",
            },
            "recommendations": {
                "migrate_now": "立即迁移（高适用度）",
                "migrate_later": "后续迁移（中等适用度）",
                "keep_as_python": "保持 Python 实现（低适用度）",
            },
        }
