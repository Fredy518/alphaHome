"""
物化视图评估标准的属性测试

**Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
**Validates: Requirements 1.1, 1.2, 1.3**

Property 1: 评估标准的一致性
- 对于任何维度评分组合，评估结果应该是一致的
- 满足 4+ 维度应该被标记为"高适用度"
- 满足 2-3 维度应该被标记为"中等适用度"
- 满足 0-1 维度应该被标记为"低适用度"
"""

import pytest
from hypothesis import given, strategies as st, assume
from alphahome.processors.materialized_views import (
    EvaluationCriteria,
    SuitabilityLevel,
)


class TestEvaluationCriteriaConsistency:
    """评估标准一致性测试"""
    
    def test_dimensions_count(self):
        """测试维度数量是否为 7"""
        dimensions = EvaluationCriteria.get_dimensions()
        assert len(dimensions) == 7, "应该有 7 个评估维度"
    
    def test_dimension_names_unique(self):
        """测试维度名称是否唯一"""
        dimensions = EvaluationCriteria.get_dimensions()
        names = [dim.name for dim in dimensions]
        assert len(names) == len(set(names)), "维度名称应该唯一"
    
    def test_dimension_attributes(self):
        """测试每个维度是否有必要的属性"""
        dimensions = EvaluationCriteria.get_dimensions()
        required_attrs = ["name", "description", "weight", "criteria", "example_pass", "example_fail"]
        
        for dim in dimensions:
            for attr in required_attrs:
                assert hasattr(dim, attr), f"维度 {dim.name} 缺少属性 {attr}"
                assert getattr(dim, attr), f"维度 {dim.name} 的属性 {attr} 为空"
    
    def test_dimension_weights_valid(self):
        """测试维度权重是否有效"""
        dimensions = EvaluationCriteria.get_dimensions()
        valid_weights = {"高", "中", "低"}
        
        for dim in dimensions:
            assert dim.weight in valid_weights, f"维度 {dim.name} 的权重 {dim.weight} 无效"
    
    def test_get_dimension_by_name(self):
        """测试按名称获取维度"""
        dimensions = EvaluationCriteria.get_dimensions()
        
        for dim in dimensions:
            retrieved = EvaluationCriteria.get_dimension_by_name(dim.name)
            assert retrieved is not None, f"无法获取维度 {dim.name}"
            assert retrieved.name == dim.name, f"获取的维度名称不匹配"
    
    def test_get_nonexistent_dimension(self):
        """测试获取不存在的维度"""
        result = EvaluationCriteria.get_dimension_by_name("不存在的维度")
        assert result is None, "不存在的维度应该返回 None"
    
    @given(
        st.lists(
            st.booleans(),
            min_size=7,
            max_size=7,
        )
    )
    def test_evaluation_consistency_with_random_scores(self, scores):
        """
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 1.1, 1.2, 1.3**
        
        Property 1: 对于任何维度评分组合，评估结果应该是一致的
        - 满足 4+ 维度应该被标记为"高适用度"
        - 满足 2-3 维度应该被标记为"中等适用度"
        - 满足 0-1 维度应该被标记为"低适用度"
        """
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_names = [dim.name for dim in dimensions]
        
        # 创建维度评分字典
        dimension_scores = {name: score for name, score in zip(dimension_names, scores)}
        
        # 评估
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        # 验证总分
        expected_score = sum(scores)
        assert result.total_score == expected_score, f"总分应该是 {expected_score}，但得到 {result.total_score}"
        
        # 验证适用度等级
        if expected_score >= 4:
            assert result.level == SuitabilityLevel.HIGHLY_SUITABLE, \
                f"满足 {expected_score} 个维度应该是高适用度"
            assert result.recommendation == "migrate_now", \
                f"高适用度的建议应该是 migrate_now"
        elif expected_score >= 2:
            assert result.level == SuitabilityLevel.MODERATELY_SUITABLE, \
                f"满足 {expected_score} 个维度应该是中等适用度"
            assert result.recommendation == "migrate_later", \
                f"中等适用度的建议应该是 migrate_later"
        else:
            assert result.level == SuitabilityLevel.NOT_SUITABLE, \
                f"满足 {expected_score} 个维度应该是低适用度"
            assert result.recommendation == "keep_as_python", \
                f"低适用度的建议应该是 keep_as_python"
    
    def test_evaluation_with_invalid_dimension_name(self):
        """测试使用无效维度名称进行评估"""
        dimension_scores = {
            "数据源简洁性": True,
            "无效维度": True,
        }
        
        with pytest.raises(ValueError, match="未知的评估维度"):
            EvaluationCriteria.evaluate(dimension_scores)
    
    def test_evaluation_result_to_dict(self):
        """测试评估结果转换为字典"""
        dimension_scores = {
            "数据源简洁性": True,
            "转换逻辑复杂度": True,
            "计算复杂度": True,
            "输出数据量": True,
            "更新频率": False,
            "查询复杂度": False,
            "复用度": False,
        }
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        result_dict = result.to_dict()
        
        assert "total_score" in result_dict
        assert "dimension_scores" in result_dict
        assert "level" in result_dict
        assert "recommendation" in result_dict
        assert "reasoning" in result_dict
        
        assert result_dict["total_score"] == 4
        assert result_dict["level"] == "highly_suitable"
        assert result_dict["recommendation"] == "migrate_now"
    
    def test_evaluation_framework_structure(self):
        """测试评估框架的结构"""
        framework = EvaluationCriteria.get_evaluation_framework()
        
        assert "total_dimensions" in framework
        assert "dimensions" in framework
        assert "suitability_levels" in framework
        assert "recommendations" in framework
        
        assert framework["total_dimensions"] == 7
        assert len(framework["dimensions"]) == 7
        assert len(framework["suitability_levels"]) == 3
        assert len(framework["recommendations"]) == 3
    
    def test_dimension_to_dict(self):
        """测试维度转换为字典"""
        dimensions = EvaluationCriteria.get_dimensions()
        
        for dim in dimensions:
            dim_dict = dim.to_dict()
            
            assert "name" in dim_dict
            assert "description" in dim_dict
            assert "weight" in dim_dict
            assert "criteria" in dim_dict
            assert "example_pass" in dim_dict
            assert "example_fail" in dim_dict
    
    @given(
        st.integers(min_value=0, max_value=7)
    )
    def test_evaluation_score_range(self, num_satisfied):
        """
        Property: 评估总分应该在 0-7 之间
        """
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_names = [dim.name for dim in dimensions]
        
        # 创建指定数量的满足维度
        dimension_scores = {
            name: (i < num_satisfied)
            for i, name in enumerate(dimension_names)
        }
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        assert 0 <= result.total_score <= 7, \
            f"总分应该在 0-7 之间，但得到 {result.total_score}"
        assert result.total_score == num_satisfied, \
            f"总分应该是 {num_satisfied}，但得到 {result.total_score}"
    
    def test_evaluation_idempotency(self):
        """
        Property: 对同一个维度评分进行多次评估应该得到相同的结果
        """
        dimension_scores = {
            "数据源简洁性": True,
            "转换逻辑复杂度": True,
            "计算复杂度": False,
            "输出数据量": True,
            "更新频率": False,
            "查询复杂度": True,
            "复用度": False,
        }
        
        result1 = EvaluationCriteria.evaluate(dimension_scores)
        result2 = EvaluationCriteria.evaluate(dimension_scores)
        result3 = EvaluationCriteria.evaluate(dimension_scores)
        
        assert result1.total_score == result2.total_score == result3.total_score
        assert result1.level == result2.level == result3.level
        assert result1.recommendation == result2.recommendation == result3.recommendation
    
    def test_all_dimensions_satisfied(self):
        """测试所有维度都满足的情况"""
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_scores = {dim.name: True for dim in dimensions}
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        assert result.total_score == 7
        assert result.level == SuitabilityLevel.HIGHLY_SUITABLE
        assert result.recommendation == "migrate_now"
    
    def test_no_dimensions_satisfied(self):
        """测试没有维度满足的情况"""
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_scores = {dim.name: False for dim in dimensions}
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        assert result.total_score == 0
        assert result.level == SuitabilityLevel.NOT_SUITABLE
        assert result.recommendation == "keep_as_python"
    
    def test_boundary_case_4_dimensions(self):
        """测试边界情况：恰好 4 个维度满足"""
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_scores = {
            name: (i < 4)
            for i, name in enumerate([dim.name for dim in dimensions])
        }
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        assert result.total_score == 4
        assert result.level == SuitabilityLevel.HIGHLY_SUITABLE
        assert result.recommendation == "migrate_now"
    
    def test_boundary_case_3_dimensions(self):
        """测试边界情况：恰好 3 个维度满足"""
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_scores = {
            name: (i < 3)
            for i, name in enumerate([dim.name for dim in dimensions])
        }
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        assert result.total_score == 3
        assert result.level == SuitabilityLevel.MODERATELY_SUITABLE
        assert result.recommendation == "migrate_later"
    
    def test_boundary_case_2_dimensions(self):
        """测试边界情况：恰好 2 个维度满足"""
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_scores = {
            name: (i < 2)
            for i, name in enumerate([dim.name for dim in dimensions])
        }
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        assert result.total_score == 2
        assert result.level == SuitabilityLevel.MODERATELY_SUITABLE
        assert result.recommendation == "migrate_later"
    
    def test_boundary_case_1_dimension(self):
        """测试边界情况：恰好 1 个维度满足"""
        dimensions = EvaluationCriteria.get_dimensions()
        dimension_scores = {
            name: (i < 1)
            for i, name in enumerate([dim.name for dim in dimensions])
        }
        
        result = EvaluationCriteria.evaluate(dimension_scores)
        
        assert result.total_score == 1
        assert result.level == SuitabilityLevel.NOT_SUITABLE
        assert result.recommendation == "keep_as_python"
