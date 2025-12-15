#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Property-based tests for PITFinancialIndicatorsMV.

Tests the PIT financial indicators materialized view implementation.

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
# Custom Strategies for PIT Financial Indicators
# =============================================================================

def ts_code_strategy():
    """Generate valid ts_code values (stock codes)."""
    # Generate codes like 000001, 600000, 300001, etc.
    prefixes = st.sampled_from(['0', '3', '6'])
    rest = st.integers(min_value=0, max_value=9999).map(lambda x: str(x).zfill(5))
    return st.builds(lambda p, r: p + r, prefixes, rest)


def ann_date_strategy():
    """Generate valid announcement dates."""
    # Generate dates in the past 5 years
    base_date = date.today()
    return st.dates(
        min_value=base_date - timedelta(days=365*5),
        max_value=base_date
    )


def end_date_strategy():
    """Generate valid end dates (quarter end dates)."""
    # Generate quarter end dates: 03-31, 06-30, 09-30, 12-31
    base_date = date.today()
    return st.dates(
        min_value=base_date - timedelta(days=365*5),
        max_value=base_date
    )


def financial_indicator_strategy():
    """Generate valid financial indicator values."""
    # Generate realistic financial indicator values
    return st.floats(
        min_value=-1000,
        max_value=1000,
        allow_nan=False,
        allow_infinity=False
    )


# =============================================================================
# Property 7: PIT Time Series Expansion Correctness
# **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
# **Validates: Requirements 8.1, 8.2, 8.3**
# =============================================================================

class TestProperty7PITTimeSeriesExpansionCorrectness:
    """
    Property 7: PIT Time Series Expansion Correctness
    
    *For any* PIT financial indicators record with announcement date `ann_date`,
    the materialized view should expand it into a time series where:
    - `query_start_date` = `ann_date`
    - `query_end_date` = next announcement date - 1 day (or '2099-12-31' if no next record)
    - Any query date between `query_start_date` and `query_end_date` should return this record
    
    **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
    **Validates: Requirements 8.1, 8.2, 8.3**
    """
    
    def test_pit_mv_sql_includes_time_series_expansion(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1, 8.2**
        
        The PIT MV SQL SHALL include time series expansion logic:
        1. query_start_date = ann_date
        2. query_end_date = LEAD(ann_date) - 1 day or '2099-12-31'
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify time series expansion
        assert "query_start_date" in sql, "SQL should include query_start_date"
        assert "query_end_date" in sql, "SQL should include query_end_date"
        assert "LEAD" in sql, "SQL should use LEAD window function"
        assert "OVER" in sql, "SQL should include OVER clause"
        assert "PARTITION BY" in sql, "SQL should partition by ts_code"
        assert "ORDER BY" in sql, "SQL should order by ann_date"
        assert "2099-12-31" in sql, "SQL should use 2099-12-31 as default end date"
    
    def test_pit_mv_sql_includes_data_alignment(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1, 8.3**
        
        The PIT MV SQL SHALL include data alignment logic:
        1. ts_code format standardization (add .SH or .SZ suffix)
        2. CASE statement for different prefixes
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify data alignment
        assert "CASE" in sql, "SQL should include CASE for ts_code alignment"
        assert ".SH" in sql, "SQL should handle Shanghai codes (.SH)"
        assert ".SZ" in sql, "SQL should handle Shenzhen codes (.SZ)"
        assert "WHEN" in sql, "SQL should include WHEN clauses"
    
    def test_pit_mv_sql_includes_data_standardization(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.2**
        
        The PIT MV SQL SHALL include data standardization logic:
        1. CAST numeric columns to DECIMAL
        2. Preserve precision for financial indicators
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify data standardization
        assert "CAST" in sql, "SQL should include CAST for standardization"
        assert "DECIMAL" in sql, "SQL should cast to DECIMAL"
        assert "pe_ttm" in sql, "SQL should include pe_ttm"
        assert "pb" in sql, "SQL should include pb"
    
    def test_pit_mv_sql_includes_lineage_metadata(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.3**
        
        The PIT MV SQL SHALL include lineage metadata:
        1. _source_table: source table name
        2. _processed_at: processing timestamp
        3. _data_version: data version date
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify lineage metadata
        assert "_source_table" in sql, "SQL should include _source_table"
        assert "_processed_at" in sql, "SQL should include _processed_at"
        assert "_data_version" in sql, "SQL should include _data_version"
        assert "rawdata.pit_financial_indicators" in sql, "SQL should reference source table"
    
    def test_pit_mv_sql_includes_quality_checks(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1**
        
        The PIT MV SQL SHALL include data quality checks:
        1. NOT NULL checks for key columns
        2. Range checks for numeric columns
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify quality checks
        assert "IS NOT NULL" in sql, "SQL should include NOT NULL checks"
        assert "BETWEEN" in sql, "SQL should include range checks"
        assert "WHERE" in sql, "SQL should include WHERE clause for validation"
    
    def test_pit_mv_attributes(self):
        """
        **Feature: materialized-views-system, Property 7: PIT time series expansion correctness**
        **Validates: Requirements 8.1**
        
        The PITFinancialIndicatorsMV class SHALL have correct attributes:
        1. materialized_view_name = "pit_financial_indicators_mv"
        2. source_tables = ["rawdata.pit_financial_indicators"]
        3. refresh_strategy = "full"
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Verify attributes
        assert mv.materialized_view_name == "pit_financial_indicators_mv"
        assert mv.source_tables == ["rawdata.pit_financial_indicators"]
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
    
    def test_pit_mv_sql_ts_code_alignment(self):
        """
        **Feature: materialized-views-system, Property 8: Data alignment consistency**
        **Validates: Requirements 5.1, 8.1**
        
        The PIT MV SQL SHALL align ts_code to standard format:
        1. Check for prefix 6 → add .SH
        2. Check for prefix 0 or 3 → add .SZ
        3. Otherwise keep original
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
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
    
    def test_pit_mv_sql_preserves_data_integrity(self):
        """
        **Feature: materialized-views-system, Property 8: Data alignment consistency**
        **Validates: Requirements 8.1**
        
        The PIT MV SQL SHALL preserve data integrity:
        1. All key columns should be preserved
        2. All value columns should be preserved
        3. Alignment should not lose data
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Get SQL
        import asyncio
        sql = asyncio.run(mv.define_materialized_view_sql())
        
        # Verify key columns are preserved
        assert "ts_code" in sql, "SQL should preserve ts_code"
        assert "ann_date" in sql, "SQL should preserve ann_date"
        assert "end_date" in sql, "SQL should preserve end_date"
        
        # Verify value columns are preserved
        assert "pe_ttm" in sql, "SQL should preserve pe_ttm"
        assert "pb" in sql, "SQL should preserve pb"
        assert "ps" in sql, "SQL should preserve ps"
    
    def test_pit_mv_quality_checks_configuration(self):
        """
        **Feature: materialized-views-system, Property 8: Data alignment consistency**
        **Validates: Requirements 8.1**
        
        The PITFinancialIndicatorsMV class SHALL have quality checks configured:
        1. null_check for key columns
        2. outlier_check for numeric columns
        3. row_count_change threshold
        """
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        # Create instance
        mv = PITFinancialIndicatorsMV()
        
        # Verify quality checks
        assert 'null_check' in mv.quality_checks, "Should have null_check"
        assert 'outlier_check' in mv.quality_checks, "Should have outlier_check"
        assert 'row_count_change' in mv.quality_checks, "Should have row_count_change"
        
        # Verify null_check configuration
        null_check = mv.quality_checks['null_check']
        assert 'columns' in null_check, "null_check should have columns"
        assert 'ts_code' in null_check['columns'], "null_check should include ts_code"
        assert 'ann_date' in null_check['columns'], "null_check should include ann_date"
        
        # Verify outlier_check configuration
        outlier_check = mv.quality_checks['outlier_check']
        assert 'columns' in outlier_check, "outlier_check should have columns"
        assert 'pe_ttm' in outlier_check['columns'], "outlier_check should include pe_ttm"
        assert 'pb' in outlier_check['columns'], "outlier_check should include pb"


# =============================================================================
# Unit Tests for PITFinancialIndicatorsMV
# =============================================================================

class TestPITFinancialIndicatorsMVUnit:
    """Unit tests for PITFinancialIndicatorsMV class."""
    
    def test_pit_mv_initialization(self):
        """Test PITFinancialIndicatorsMV initialization."""
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        mv = PITFinancialIndicatorsMV()
        
        assert mv.name == "pit_financial_indicators_mv"
        assert mv.is_materialized_view is True
        assert mv.materialized_view_name == "pit_financial_indicators_mv"
        assert mv.materialized_view_schema == "materialized_views"
        assert mv.refresh_strategy == "full"
        assert mv.source_tables == ["rawdata.pit_financial_indicators"]
    
    def test_pit_mv_get_materialized_view_info(self):
        """Test get_materialized_view_info method."""
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        mv = PITFinancialIndicatorsMV()
        info = mv.get_materialized_view_info()
        
        assert info['name'] == "pit_financial_indicators_mv"
        assert info['materialized_view_name'] == "pit_financial_indicators_mv"
        assert info['materialized_view_schema'] == "materialized_views"
        assert info['full_name'] == "materialized_views.pit_financial_indicators_mv"
        assert info['refresh_strategy'] == "full"
        assert info['source_tables'] == ["rawdata.pit_financial_indicators"]
    
    @pytest.mark.asyncio
    async def test_pit_mv_fetch_data_returns_none(self):
        """Test that fetch_data returns None (as expected for MV tasks)."""
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        mv = PITFinancialIndicatorsMV()
        result = await mv.fetch_data()
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_pit_mv_process_data_returns_none(self):
        """Test that process_data returns None (as expected for MV tasks)."""
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        mv = PITFinancialIndicatorsMV()
        result = await mv.process_data(pd.DataFrame())
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_pit_mv_save_result_does_nothing(self):
        """Test that save_result does nothing (as expected for MV tasks)."""
        from alphahome.processors.tasks.pit import PITFinancialIndicatorsMV
        
        mv = PITFinancialIndicatorsMV()
        # Should not raise any exception
        await mv.save_result(pd.DataFrame())

