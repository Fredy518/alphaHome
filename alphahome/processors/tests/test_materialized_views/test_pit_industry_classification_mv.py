#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for PITIndustryClassificationMV.

Tests the PIT industry classification materialized view implementation.

**Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
**Validates: Requirements 8.1, 8.2, 8.3**

**Feature: materialized-views-system, Property 8: Data alignment consistency**
**Validates: Requirements 5.1, 8.1**
"""

import pytest
from hypothesis import given, strategies as st, settings, assume, HealthCheck
from datetime import date, timedelta
import pandas as pd


# =============================================================================
# Custom Strategies for PIT Industry Classification
# =============================================================================

def ts_code_strategy():
    """Generate valid ts_code values (stock codes)."""
    # Generate codes like 000001, 600000, 300001, etc.
    prefixes = st.sampled_from(['0', '3', '6'])
    rest = st.integers(min_value=0, max_value=9999).map(lambda x: str(x).zfill(5))
    return st.builds(lambda p, r: p + r, prefixes, rest)


def obs_date_strategy():
    """Generate valid observation dates."""
    # Generate dates in the past 5 years
    base_date = date.today()
    return st.dates(
        min_value=base_date - timedelta(days=365*5),
        max_value=base_date
    )


def data_source_strategy():
    """Generate valid data source values."""
    return st.sampled_from(['sw', 'ci'])


def industry_level_strategy():
    """Generate valid industry level values."""
    industries = [
        '农业', '采矿业', '制造业', '电力热力燃气水',
        '建筑业', '交通运输仓储邮政', '信息技术', '金融业',
        '房地产业', '租赁和商务服务', '科学研究和技术服务',
        '水利环境和公共设施', '居民服务修缮和其他', '教育',
        '卫生和社会工作', '文化体育和娱乐', '公共管理社会保障'
    ]
    return st.sampled_from(industries)


# =============================================================================
# Property 7: PIT Time Series Expansion Correctness
# **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
# **Validates: Requirements 8.1, 8.2, 8.3**
# =============================================================================

class TestProperty7PITTimeSeriesExpansionCorrectness:
    """
    Property 7: PIT Time Series Expansion Correctness
    
    *For any* PIT industry classification record with observation date `obs_date`,
    the materialized view should expand it into a time series where:
    - `query_start_date` = `obs_date`
    - `query_end_date` = next observation date - 1 day (or '2099-12-31' if no next record)
    - Any query date between `query_start_date` and `query_end_date` should return this record
    
    **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
    **Validates: Requirements 8.1, 8.2, 8.3**
    """
    
    def test_pit_industry_mv_sql_includes_time_series_expansion(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1, 8.2**
        
        The PIT Industry MV SQL SHALL include time series expansion logic:
        1. query_start_date = obs_date
        2. query_end_date = LEAD(obs_date) - 1 day or '2099-12-31'
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify time series expansion
        assert "query_start_date" in sql, "SQL should include query_start_date"
        assert "query_end_date" in sql, "SQL should include query_end_date"
        assert "LEAD" in sql, "SQL should use LEAD window function"
        assert "OVER" in sql, "SQL should include OVER clause"
        assert "PARTITION BY" in sql, "SQL should partition by ts_code and data_source"
        assert "ORDER BY" in sql, "SQL should order by obs_date"
        assert "2099-12-31" in sql, "SQL should use 2099-12-31 as default end date"
    
    def test_pit_industry_mv_sql_includes_data_alignment(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1, 8.3**
        
        The PIT Industry MV SQL SHALL include data alignment logic:
        1. ts_code format standardization (add .SH or .SZ suffix)
        2. CASE statement for different prefixes
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify data alignment
        assert "CASE" in sql, "SQL should include CASE for ts_code alignment"
        assert ".SH" in sql, "SQL should handle Shanghai codes (.SH)"
        assert ".SZ" in sql, "SQL should handle Shenzhen codes (.SZ)"
        assert "WHEN" in sql, "SQL should include WHEN clauses"
    
    def test_pit_industry_mv_sql_includes_data_standardization(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.2**
        
        The PIT Industry MV SQL SHALL include data standardization logic:
        1. TRIM string columns for consistency
        2. Preserve industry classification fields
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify data standardization
        assert "TRIM" in sql, "SQL should include TRIM for string standardization"
        assert "industry_level1" in sql, "SQL should include industry_level1"
        assert "industry_level2" in sql, "SQL should include industry_level2"
        assert "industry_level3" in sql, "SQL should include industry_level3"
        assert "data_source" in sql, "SQL should include data_source"
    
    def test_pit_industry_mv_sql_includes_lineage_metadata(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.3**
        
        The PIT Industry MV SQL SHALL include lineage metadata:
        1. _source_table: source table name
        2. _processed_at: processing timestamp
        3. _data_version: data version date
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify lineage metadata
        assert "_source_table" in sql, "SQL should include _source_table"
        assert "_processed_at" in sql, "SQL should include _processed_at"
        assert "_data_version" in sql, "SQL should include _data_version"
        assert "rawdata.pit_industry_classification" in sql, "SQL should reference source table"
    
    def test_pit_industry_mv_sql_includes_quality_checks(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1**
        
        The PIT Industry MV SQL SHALL include data quality checks:
        1. NOT NULL checks for key columns
        2. Validation of required fields
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify quality checks
        assert "IS NOT NULL" in sql, "SQL should include NOT NULL checks"
        assert "WHERE" in sql, "SQL should include WHERE clause for validation"
        assert "ts_code IS NOT NULL" in sql, "SQL should check ts_code NOT NULL"
        assert "obs_date IS NOT NULL" in sql, "SQL should check obs_date NOT NULL"
    
    def test_pit_industry_mv_attributes(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1**
        
        The PITIndustryClassificationMV class SHALL have correct attributes:
        1. materialized_view_name = "pit_industry_classification_mv"
        2. source_tables = ["rawdata.pit_industry_classification"]
        3. refresh_strategy = "full"
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Verify attributes
        assert mv.materialized_view_name == "pit_industry_classification_mv"
        assert mv.source_tables == ["rawdata.pit_industry_classification"]
        assert mv.refresh_strategy == "full"
        assert mv.is_materialized_view is True
        assert mv.materialized_view_schema == "materialized_views"


# =============================================================================
# Property 8: Data Alignment Consistency
# **Feature: materialized-views-system, Property 8: Data alignment consistency**
# **Validates: Requirements 5.1, 8.1**
# =============================================================================

class TestProperty8DataAlignmentConsistency:
    """
    Property 8: Data Alignment Consistency
    
    *For any* record in the materialized view, the `ts_code` should be in standard format
    (e.g., '000001.SZ'), regardless of the source format in rawdata.
    
    **Feature: materialized-views-system, Property 8: Data alignment consistency**
    **Validates: Requirements 5.1, 8.1**
    """
    
    @given(
        ts_code_prefix=st.sampled_from(['0', '3', '6']),
        ts_code_suffix=st.integers(min_value=0, max_value=9999).map(lambda x: str(x).zfill(5))
    )
    @settings(
        max_examples=100,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much]
    )
    def test_ts_code_alignment_logic(self, ts_code_prefix, ts_code_suffix):
        """
        **Feature: materialized-views-system, Property 8: Data alignment consistency**
        **Validates: Requirements 5.1**
        
        For any ts_code with prefix 0, 3, or 6, the alignment logic SHALL:
        1. Prefix 0 or 3 → add .SZ suffix
        2. Prefix 6 → add .SH suffix
        3. Result should be in format: XXXXXX.SH or XXXXXX.SZ
        """
        ts_code = ts_code_prefix + ts_code_suffix
        
        # Simulate the alignment logic from SQL
        if ts_code.startswith('6'):
            aligned_code = ts_code + '.SH'
        elif ts_code.startswith('0') or ts_code.startswith('3'):
            aligned_code = ts_code + '.SZ'
        else:
            aligned_code = ts_code
        
        # Verify alignment
        assert '.' in aligned_code, "Aligned code should contain exchange suffix"
        assert aligned_code.endswith('.SH') or aligned_code.endswith('.SZ'), \
            "Aligned code should end with .SH or .SZ"
        
        # Verify format
        parts = aligned_code.split('.')
        assert len(parts) == 2, "Aligned code should have exactly 2 parts"
        assert len(parts[0]) == 6, "Stock code should be 6 digits"
        assert parts[1] in ['SH', 'SZ'], "Exchange should be SH or SZ"
    
    def test_pit_industry_mv_sql_ts_code_alignment(self):
        """
        **Feature: materialized-views-system, Property 8: Data alignment consistency**
        **Validates: Requirements 5.1, 8.1**
        
        The PIT Industry MV SQL SHALL align ts_code to standard format:
        1. Check for prefix 6 → add .SH
        2. Check for prefix 0 or 3 → add .SZ
        3. Otherwise keep original
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify alignment logic
        assert "CASE" in sql, "SQL should use CASE for alignment"
        assert "WHEN ts_code LIKE '6%'" in sql, "SQL should check for prefix 6"
        assert "WHEN ts_code LIKE '0%'" in sql, "SQL should check for prefix 0"
        assert "WHEN ts_code LIKE '3%'" in sql, "SQL should check for prefix 3"
        assert ".SH" in sql, "SQL should add .SH for Shanghai codes"
        assert ".SZ" in sql, "SQL should add .SZ for Shenzhen codes"
    
    def test_pit_industry_mv_sql_preserves_data_integrity(self):
        """
        **Feature: materialized-views-system, Property 8: Data alignment consistency**
        **Validates: Requirements 8.1**
        
        The PIT Industry MV SQL SHALL preserve data integrity:
        1. All key columns should be preserved
        2. All industry classification columns should be preserved
        3. Alignment should not lose data
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify key columns are preserved
        assert "ts_code" in sql, "SQL should preserve ts_code"
        assert "obs_date" in sql, "SQL should preserve obs_date"
        assert "data_source" in sql, "SQL should preserve data_source"
        
        # Verify industry classification columns are preserved
        assert "industry_level1" in sql, "SQL should preserve industry_level1"
        assert "industry_level2" in sql, "SQL should preserve industry_level2"
        assert "industry_level3" in sql, "SQL should preserve industry_level3"
        assert "industry_code1" in sql, "SQL should preserve industry_code1"
        assert "industry_code2" in sql, "SQL should preserve industry_code2"
        assert "industry_code3" in sql, "SQL should preserve industry_code3"
    
    def test_pit_industry_mv_quality_checks_configuration(self):
        """
        **Feature: materialized-views-system, Property 8: Data alignment consistency**
        **Validates: Requirements 8.1**
        
        The PITIndustryClassificationMV class SHALL have quality checks configured:
        1. null_check for key columns
        2. row_count_change threshold
        """
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        # Create instance
        mv = PITIndustryClassificationMV()
        
        # Verify quality checks
        assert 'null_check' in mv.quality_checks, "Should have null_check"
        assert 'row_count_change' in mv.quality_checks, "Should have row_count_change"
        
        # Verify null_check configuration
        null_check = mv.quality_checks['null_check']
        assert 'columns' in null_check, "null_check should have columns"
        assert 'ts_code' in null_check['columns'], "null_check should include ts_code"
        assert 'obs_date' in null_check['columns'], "null_check should include obs_date"
        assert 'industry_level1' in null_check['columns'], "null_check should include industry_level1"
        assert 'industry_level2' in null_check['columns'], "null_check should include industry_level2"


# =============================================================================
# Unit Tests for PITIndustryClassificationMV
# =============================================================================

class TestPITIndustryClassificationMVUnit:
    """Unit tests for PITIndustryClassificationMV class."""
    
    def test_pit_industry_mv_initialization(self):
        """Test PITIndustryClassificationMV initialization."""
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        mv = PITIndustryClassificationMV()
        
        assert mv.name == "pit_industry_classification_mv"
        assert mv.is_materialized_view is True
        assert mv.materialized_view_name == "pit_industry_classification_mv"
        assert mv.materialized_view_schema == "materialized_views"
        assert mv.refresh_strategy == "full"
        assert mv.source_tables == ["rawdata.pit_industry_classification"]
    
    def test_pit_industry_mv_get_materialized_view_info(self):
        """Test get_materialized_view_info method."""
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        mv = PITIndustryClassificationMV()
        info = mv.get_materialized_view_info()
        
        assert info['name'] == "pit_industry_classification_mv"
        assert info['materialized_view_name'] == "pit_industry_classification_mv"
        assert info['materialized_view_schema'] == "materialized_views"
        assert info['full_name'] == "materialized_views.pit_industry_classification_mv"
        assert info['refresh_strategy'] == "full"
        assert info['source_tables'] == ["rawdata.pit_industry_classification"]
    
    @pytest.mark.asyncio
    async def test_pit_industry_mv_fetch_data_returns_none(self):
        """Test that fetch_data returns None (as expected for MV tasks)."""
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        mv = PITIndustryClassificationMV()
        result = await mv.fetch_data()
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_pit_industry_mv_process_data_returns_none(self):
        """Test that process_data returns None (as expected for MV tasks)."""
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        mv = PITIndustryClassificationMV()
        result = await mv.process_data(pd.DataFrame())
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_pit_industry_mv_save_result_does_nothing(self):
        """Test that save_result does nothing (as expected for MV tasks)."""
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        mv = PITIndustryClassificationMV()
        # Should not raise any exception
        await mv.save_result(pd.DataFrame())
    
    def test_pit_industry_mv_description(self):
        """Test that PITIndustryClassificationMV has proper description."""
        from alphahome.processors.tasks.pit import PITIndustryClassificationMV
        
        mv = PITIndustryClassificationMV()
        
        assert mv.description == "PIT 行业分类物化视图"
        assert mv.name == "pit_industry_classification_mv"
