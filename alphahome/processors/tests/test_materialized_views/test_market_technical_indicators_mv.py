#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests for MarketTechnicalIndicatorsMV.

Tests the market technical indicators materialized view implementation.

**Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
**Validates: Requirements 8.1, 8.2, 8.3**
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
from datetime import date, timedelta


class TestMarketTechnicalIndicatorsMVClass:
    """Test MarketTechnicalIndicatorsMV class initialization and properties."""
    
    def test_market_technical_indicators_mv_initialization(self):
        """
        Test that MarketTechnicalIndicatorsMV initializes correctly.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        # Create instance
        mv = MarketTechnicalIndicatorsMV()
        
        # Verify basic properties
        assert mv.name == "market_technical_indicators_mv"
        assert mv.materialized_view_name == "market_technical_indicators_mv"
        assert mv.materialized_view_schema == "materialized_views"
        assert mv.is_materialized_view is True
        assert mv.refresh_strategy == "full"
        assert mv.source_tables == ["rawdata.market_technical"]
    
    def test_market_technical_indicators_mv_quality_checks_configured(self):
        """
        Test that quality checks are properly configured.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        
        # Verify quality checks configuration
        assert "null_check" in mv.quality_checks
        assert "outlier_check" in mv.quality_checks
        assert "row_count_change" in mv.quality_checks
        
        # Verify null_check configuration
        null_check = mv.quality_checks["null_check"]
        assert "columns" in null_check
        assert "threshold" in null_check
        assert null_check["threshold"] == 0.05
        
        # Verify outlier_check configuration
        outlier_check = mv.quality_checks["outlier_check"]
        assert outlier_check["method"] == "iqr"
        assert outlier_check["threshold"] == 3.0
        
        # Verify row_count_change configuration
        row_count_check = mv.quality_checks["row_count_change"]
        assert row_count_check["threshold"] == 0.3


class TestMarketTechnicalIndicatorsMVSQL:
    """Test SQL generation for market technical indicators MV."""
    
    @pytest.mark.asyncio
    async def test_define_materialized_view_sql_returns_valid_sql(self):
        """
        Test that define_materialized_view_sql returns valid SQL.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify SQL is a string
        assert isinstance(sql, str)
        assert len(sql) > 0
        
        # Verify SQL contains key components
        assert "CREATE MATERIALIZED VIEW" in sql
        assert "materialized_views.market_technical_indicators_mv" in sql
        assert "rawdata.market_technical" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_data_alignment(self):
        """
        Test that SQL includes data alignment logic.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify data alignment logic
        assert "ts_code_std" in sql or "ts_code" in sql
        assert ".SH" in sql or ".SZ" in sql
        assert "CASE" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_aggregation_calculations(self):
        """
        Test that SQL includes aggregation calculations.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify aggregation calculations
        assert "PERCENTILE_CONT" in sql
        assert "AVG(" in sql
        assert "STDDEV" in sql
        assert "COUNT" in sql
        assert "SUM(" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_data_validation(self):
        """
        Test that SQL includes data validation logic.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify data validation logic
        assert "WHERE" in sql
        assert "IS NOT NULL" in sql
        assert "> 0" in sql or ">= 0" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_lineage_metadata(self):
        """
        Test that SQL includes lineage metadata.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify lineage metadata
        assert "_source_table" in sql
        assert "_processed_at" in sql
        assert "_data_version" in sql
        assert "NOW()" in sql
        assert "CURRENT_DATE" in sql


class TestMarketTechnicalIndicatorsMVIntegration:
    """Integration tests for market technical indicators MV."""
    
    @pytest.mark.asyncio
    async def test_fetch_data_returns_none(self):
        """
        Test that fetch_data returns None (MV tasks don't fetch data).
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        result = await mv.fetch_data()
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_process_data_returns_none(self):
        """
        Test that process_data returns None (MV tasks don't process data).
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        result = await mv.process_data(pd.DataFrame())
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_save_result_does_nothing(self):
        """
        Test that save_result does nothing (MV tasks don't save data).
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        
        # Should not raise any exception
        await mv.save_result(pd.DataFrame())
    
    def test_get_materialized_view_info(self):
        """
        Test that get_materialized_view_info returns correct information.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        info = mv.get_materialized_view_info()
        
        # Verify info structure
        assert "name" in info
        assert "materialized_view_name" in info
        assert "materialized_view_schema" in info
        assert "full_name" in info
        assert "refresh_strategy" in info
        assert "source_tables" in info
        assert "quality_checks" in info
        
        # Verify info values
        assert info["name"] == "market_technical_indicators_mv"
        assert info["materialized_view_name"] == "market_technical_indicators_mv"
        assert info["full_name"] == "materialized_views.market_technical_indicators_mv"
        assert info["refresh_strategy"] == "full"
        assert info["source_tables"] == ["rawdata.market_technical"]


class TestMarketTechnicalIndicatorsMVDataQuality:
    """Test data quality checks for market technical indicators MV."""
    
    def test_quality_checks_include_null_check(self):
        """
        Test that quality checks include null value detection.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        
        assert "null_check" in mv.quality_checks
        null_check = mv.quality_checks["null_check"]
        assert "columns" in null_check
        assert "trade_date" in null_check["columns"]
        assert "close" in null_check["columns"]
        assert "vol" in null_check["columns"]
    
    def test_quality_checks_include_outlier_check(self):
        """
        Test that quality checks include outlier detection.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        
        assert "outlier_check" in mv.quality_checks
        outlier_check = mv.quality_checks["outlier_check"]
        assert outlier_check["method"] == "iqr"
        assert "columns" in outlier_check
    
    def test_quality_checks_include_row_count_change(self):
        """
        Test that quality checks include row count change detection.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        
        assert "row_count_change" in mv.quality_checks
        row_count_check = mv.quality_checks["row_count_change"]
        assert "threshold" in row_count_check
        assert row_count_check["threshold"] == 0.3


class TestMarketTechnicalIndicatorsMVSQLColumns:
    """Test that SQL generates all required columns."""
    
    @pytest.mark.asyncio
    async def test_sql_includes_price_statistics_columns(self):
        """
        Test that SQL includes price statistics columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify price statistics columns
        assert "close_median" in sql
        assert "close_q25" in sql
        assert "close_q75" in sql
        assert "close_mean" in sql
        assert "close_std" in sql
        assert "close_min" in sql
        assert "close_max" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_volume_statistics_columns(self):
        """
        Test that SQL includes volume statistics columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify volume statistics columns
        assert "vol_median" in sql
        assert "vol_q25" in sql
        assert "vol_q75" in sql
        assert "vol_mean" in sql
        assert "vol_std" in sql
        assert "vol_total" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_turnover_rate_statistics_columns(self):
        """
        Test that SQL includes turnover rate statistics columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify turnover rate statistics columns
        assert "turnover_rate_median" in sql
        assert "turnover_rate_q25" in sql
        assert "turnover_rate_q75" in sql
        assert "turnover_rate_mean" in sql
        assert "turnover_rate_std" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_ratio_columns(self):
        """
        Test that SQL includes ratio columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import MarketTechnicalIndicatorsMV
        
        mv = MarketTechnicalIndicatorsMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify ratio columns
        assert "high_price_ratio" in sql
        assert "high_vol_ratio" in sql
        assert "high_turnover_ratio" in sql
