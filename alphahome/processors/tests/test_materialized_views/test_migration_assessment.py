"""
任务迁移评估的属性测试

**Feature: materialized-views-system, Property 2: Migration assessment consistency**
**Validates: Requirements 2.1, 2.2**

Property 2: 迁移评估结果的一致性
- 对于任何任务的维度评分组合，评估结果应该是一致的
- 相同的维度评分应该产生相同的迁移建议
- 迁移工作量估计应该与任务复杂度相关
- 评估表应该包含所有必需的列
"""

import pytest
from hypothesis import given, strategies as st, assume, settings, HealthCheck
from typing import Dict, Any, List
import pandas as pd


class MigrationAssessment:
    """迁移评估类"""
    
    REQUIRED_COLUMNS = [
        "task_name",
        "data_sources",
        "transformation_logic",
        "output_rows",
        "update_frequency",
        "suitability_score",
        "recommendation",
        "migration_effort",
    ]
    
    VALID_RECOMMENDATIONS = ["migrate_now", "migrate_later", "keep_as_python"]
    
    @staticmethod
    def validate_assessment_row(row: Dict[str, Any]) -> bool:
        """验证评估行是否有效"""
        # 检查所有必需列
        for col in MigrationAssessment.REQUIRED_COLUMNS:
            if col not in row:
                return False
            if row[col] is None or row[col] == "":
                return False
        
        # 检查适用度评分范围
        score = row["suitability_score"]
        if not isinstance(score, (int, str)):
            return False
        
        # 如果是字符串格式 "X/7"，解析它
        if isinstance(score, str):
            try:
                parts = score.split("/")
                if len(parts) != 2:
                    return False
                num = int(parts[0])
                denom = int(parts[1])
                if num < 0 or num > denom or denom != 7:
                    return False
            except (ValueError, IndexError):
                return False
        else:
            if score < 0 or score > 7:
                return False
        
        # 检查建议是否有效
        if row["recommendation"] not in MigrationAssessment.VALID_RECOMMENDATIONS:
            return False
        
        # 检查工作量估计
        effort = row["migration_effort"]
        if effort != "N/A":
            # 应该是 "X days" 或 "X-Y days" 的格式
            if not ("day" in str(effort).lower()):
                return False
        
        return True
    
    @staticmethod
    def validate_assessment_table(df: pd.DataFrame) -> bool:
        """验证评估表是否有效"""
        # 检查所有必需列
        for col in MigrationAssessment.REQUIRED_COLUMNS:
            if col not in df.columns:
                return False
        
        # 检查每一行
        for _, row in df.iterrows():
            if not MigrationAssessment.validate_assessment_row(row.to_dict()):
                return False
        
        return True
    
    @staticmethod
    def parse_suitability_score(score: Any) -> int:
        """解析适用度评分"""
        if isinstance(score, int):
            return score
        elif isinstance(score, str):
            try:
                parts = score.split("/")
                return int(parts[0])
            except (ValueError, IndexError):
                raise ValueError(f"无法解析适用度评分: {score}")
        else:
            raise ValueError(f"无效的适用度评分类型: {type(score)}")
    
    @staticmethod
    def get_recommendation_from_score(score: int) -> str:
        """根据评分获取建议"""
        if score >= 4:
            return "migrate_now"
        elif score >= 2:
            return "migrate_later"
        else:
            return "keep_as_python"
    
    @staticmethod
    def estimate_effort_from_complexity(
        transformation_complexity: str,
        data_sources_count: int,
        output_rows: int,
    ) -> str:
        """根据复杂度估计工作量"""
        # 基础工作量：2 天
        base_effort = 2
        
        # 根据转换逻辑复杂度调整
        complexity_multiplier = {
            "simple": 1.0,
            "medium": 1.5,
            "complex": 2.0,
        }
        multiplier = complexity_multiplier.get(transformation_complexity.lower(), 1.0)
        
        # 根据数据源数量调整
        source_adjustment = (data_sources_count - 1) * 0.5
        
        # 根据输出数据量调整
        if output_rows > 1_000_000:
            volume_adjustment = 1.0
        elif output_rows > 100_000:
            volume_adjustment = 0.5
        else:
            volume_adjustment = 0.0
        
        total_effort = base_effort * multiplier + source_adjustment + volume_adjustment
        
        # 四舍五入到 0.5 天
        total_effort = round(total_effort * 2) / 2
        
        if total_effort == int(total_effort):
            return f"{int(total_effort)} days"
        else:
            return f"{total_effort} days"


class TestMigrationAssessmentConsistency:
    """迁移评估一致性测试"""
    
    def test_assessment_row_validation(self):
        """测试评估行验证"""
        valid_row = {
            "task_name": "index_valuation",
            "data_sources": 2,
            "transformation_logic": "simple",
            "output_rows": 1000,
            "update_frequency": "daily",
            "suitability_score": "7/7",
            "recommendation": "migrate_now",
            "migration_effort": "2-3 days",
        }
        
        assert MigrationAssessment.validate_assessment_row(valid_row)
    
    def test_assessment_row_missing_column(self):
        """测试缺少列的评估行"""
        invalid_row = {
            "task_name": "index_valuation",
            "data_sources": 2,
            # 缺少其他列
        }
        
        assert not MigrationAssessment.validate_assessment_row(invalid_row)
    
    def test_assessment_row_invalid_recommendation(self):
        """测试无效建议的评估行"""
        invalid_row = {
            "task_name": "index_valuation",
            "data_sources": 2,
            "transformation_logic": "simple",
            "output_rows": 1000,
            "update_frequency": "daily",
            "suitability_score": "7/7",
            "recommendation": "invalid_recommendation",
            "migration_effort": "2-3 days",
        }
        
        assert not MigrationAssessment.validate_assessment_row(invalid_row)
    
    def test_assessment_row_invalid_score_format(self):
        """测试无效评分格式的评估行"""
        invalid_row = {
            "task_name": "index_valuation",
            "data_sources": 2,
            "transformation_logic": "simple",
            "output_rows": 1000,
            "update_frequency": "daily",
            "suitability_score": "invalid",
            "recommendation": "migrate_now",
            "migration_effort": "2-3 days",
        }
        
        assert not MigrationAssessment.validate_assessment_row(invalid_row)
    
    def test_parse_suitability_score_integer(self):
        """测试解析整数评分"""
        score = MigrationAssessment.parse_suitability_score(7)
        assert score == 7
    
    def test_parse_suitability_score_string(self):
        """测试解析字符串评分"""
        score = MigrationAssessment.parse_suitability_score("7/7")
        assert score == 7
    
    def test_parse_suitability_score_invalid(self):
        """测试解析无效评分"""
        with pytest.raises(ValueError):
            MigrationAssessment.parse_suitability_score("invalid")
    
    @given(
        st.integers(min_value=0, max_value=7)
    )
    def test_recommendation_consistency_with_score(self, score):
        """
        **Feature: materialized-views-system, Property 2: Migration assessment consistency**
        **Validates: Requirements 2.1, 2.2**
        
        Property 2: 相同的评分应该产生相同的建议
        - 评分 >= 4 应该推荐 migrate_now
        - 评分 2-3 应该推荐 migrate_later
        - 评分 0-1 应该推荐 keep_as_python
        """
        recommendation = MigrationAssessment.get_recommendation_from_score(score)
        
        if score >= 4:
            assert recommendation == "migrate_now", \
                f"评分 {score} 应该推荐 migrate_now"
        elif score >= 2:
            assert recommendation == "migrate_later", \
                f"评分 {score} 应该推荐 migrate_later"
        else:
            assert recommendation == "keep_as_python", \
                f"评分 {score} 应该推荐 keep_as_python"
    
    @given(
        st.integers(min_value=0, max_value=7)
    )
    def test_recommendation_idempotency(self, score):
        """
        Property: 对同一个评分进行多次推荐应该得到相同的结果
        """
        rec1 = MigrationAssessment.get_recommendation_from_score(score)
        rec2 = MigrationAssessment.get_recommendation_from_score(score)
        rec3 = MigrationAssessment.get_recommendation_from_score(score)
        
        assert rec1 == rec2 == rec3, \
            f"相同的评分应该产生相同的建议"
    
    def test_effort_estimation_simple_task(self):
        """测试简单任务的工作量估计"""
        effort = MigrationAssessment.estimate_effort_from_complexity(
            transformation_complexity="simple",
            data_sources_count=1,
            output_rows=1000,
        )
        
        # 简单任务应该是 2 天
        assert "2" in effort or "2.0" in effort
    
    def test_effort_estimation_complex_task(self):
        """测试复杂任务的工作量估计"""
        effort = MigrationAssessment.estimate_effort_from_complexity(
            transformation_complexity="complex",
            data_sources_count=3,
            output_rows=1_000_000,
        )
        
        # 复杂任务应该需要更多时间
        # 基础 2 天 * 2.0 (复杂) + 1.0 (2 个额外数据源) + 1.0 (大数据量) = 6 天
        assert "5" in effort or "6" in effort or "5.5" in effort
    
    def test_effort_estimation_consistency(self):
        """测试工作量估计的一致性"""
        effort1 = MigrationAssessment.estimate_effort_from_complexity(
            transformation_complexity="medium",
            data_sources_count=2,
            output_rows=100_000,
        )
        effort2 = MigrationAssessment.estimate_effort_from_complexity(
            transformation_complexity="medium",
            data_sources_count=2,
            output_rows=100_000,
        )
        
        assert effort1 == effort2, \
            f"相同的参数应该产生相同的工作量估计"
    
    def test_assessment_table_structure(self):
        """测试评估表结构"""
        data = {
            "task_name": ["index_valuation", "market_technical"],
            "data_sources": [2, 1],
            "transformation_logic": ["simple", "complex"],
            "output_rows": [1000, 250],
            "update_frequency": ["daily", "daily"],
            "suitability_score": ["7/7", "7/7"],
            "recommendation": ["migrate_now", "migrate_now"],
            "migration_effort": ["2-3 days", "3-4 days"],
        }
        
        df = pd.DataFrame(data)
        assert MigrationAssessment.validate_assessment_table(df)
    
    def test_assessment_table_missing_column(self):
        """测试缺少列的评估表"""
        data = {
            "task_name": ["index_valuation"],
            "data_sources": [2],
            # 缺少其他列
        }
        
        df = pd.DataFrame(data)
        assert not MigrationAssessment.validate_assessment_table(df)
    
    def test_assessment_table_invalid_row(self):
        """测试包含无效行的评估表"""
        data = {
            "task_name": ["index_valuation", "market_technical"],
            "data_sources": [2, 1],
            "transformation_logic": ["simple", "complex"],
            "output_rows": [1000, 250],
            "update_frequency": ["daily", "daily"],
            "suitability_score": ["7/7", "invalid"],  # 无效评分
            "recommendation": ["migrate_now", "migrate_now"],
            "migration_effort": ["2-3 days", "3-4 days"],
        }
        
        df = pd.DataFrame(data)
        assert not MigrationAssessment.validate_assessment_table(df)
    
    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(
        st.lists(
            st.tuples(
                st.just("task"),  # task_name
                st.integers(min_value=1, max_value=5),  # data_sources
                st.sampled_from(["simple", "medium", "complex"]),  # transformation_logic
                st.sampled_from([100, 1000, 100_000, 1_000_000]),  # output_rows
                st.sampled_from(["daily", "weekly", "monthly"]),  # update_frequency
                st.integers(min_value=0, max_value=7),  # suitability_score
            ),
            min_size=1,
            max_size=3,
        )
    )
    def test_assessment_consistency_with_random_data(self, task_data):
        """
        **Feature: materialized-views-system, Property 2: Migration assessment consistency**
        **Validates: Requirements 2.1, 2.2**
        
        Property 2: 评估结果应该与维度评分一致
        - 对于任何任务数据，评估结果应该是一致的
        - 建议应该与评分相关
        - 工作量估计应该与复杂度相关
        """
        rows = []
        for idx, (
            task_name,
            data_sources,
            transformation_logic,
            output_rows,
            update_frequency,
            suitability_score,
        ) in enumerate(task_data):
            recommendation = MigrationAssessment.get_recommendation_from_score(
                suitability_score
            )
            effort = MigrationAssessment.estimate_effort_from_complexity(
                transformation_logic,
                data_sources,
                output_rows,
            )
            
            row = {
                "task_name": f"{task_name}_{idx}",
                "data_sources": data_sources,
                "transformation_logic": transformation_logic,
                "output_rows": output_rows,
                "update_frequency": update_frequency,
                "suitability_score": f"{suitability_score}/7",
                "recommendation": recommendation,
                "migration_effort": effort,
            }
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # 验证表结构
        assert MigrationAssessment.validate_assessment_table(df)
        
        # 验证建议与评分的一致性
        for _, row in df.iterrows():
            score = MigrationAssessment.parse_suitability_score(
                row["suitability_score"]
            )
            expected_recommendation = (
                MigrationAssessment.get_recommendation_from_score(score)
            )
            assert row["recommendation"] == expected_recommendation, \
                f"任务 {row['task_name']} 的建议应该与评分一致"
    
    def test_boundary_case_score_4(self):
        """测试边界情况：评分 4"""
        recommendation = MigrationAssessment.get_recommendation_from_score(4)
        assert recommendation == "migrate_now"
    
    def test_boundary_case_score_3(self):
        """测试边界情况：评分 3"""
        recommendation = MigrationAssessment.get_recommendation_from_score(3)
        assert recommendation == "migrate_later"
    
    def test_boundary_case_score_2(self):
        """测试边界情况：评分 2"""
        recommendation = MigrationAssessment.get_recommendation_from_score(2)
        assert recommendation == "migrate_later"
    
    def test_boundary_case_score_1(self):
        """测试边界情况：评分 1"""
        recommendation = MigrationAssessment.get_recommendation_from_score(1)
        assert recommendation == "keep_as_python"
    
    def test_all_valid_recommendations(self):
        """测试所有有效的建议"""
        for recommendation in MigrationAssessment.VALID_RECOMMENDATIONS:
            # 应该能够创建包含此建议的有效行
            row = {
                "task_name": "test_task",
                "data_sources": 2,
                "transformation_logic": "simple",
                "output_rows": 1000,
                "update_frequency": "daily",
                "suitability_score": "7/7",
                "recommendation": recommendation,
                "migration_effort": "2 days",
            }
            
            assert MigrationAssessment.validate_assessment_row(row)
    
    def test_effort_estimation_with_large_output(self):
        """测试大数据量的工作量估计"""
        effort = MigrationAssessment.estimate_effort_from_complexity(
            transformation_complexity="simple",
            data_sources_count=1,
            output_rows=10_000_000,
        )
        
        # 大数据量应该增加工作量
        assert "3" in effort or "2.5" in effort or "3.0" in effort
    
    def test_effort_estimation_with_many_sources(self):
        """测试多数据源的工作量估计"""
        effort = MigrationAssessment.estimate_effort_from_complexity(
            transformation_complexity="simple",
            data_sources_count=5,
            output_rows=1000,
        )
        
        # 多数据源应该增加工作量
        # 基础 2 天 + 2.0 (4 个额外数据源) = 4 天
        assert "4" in effort or "4.0" in effort
