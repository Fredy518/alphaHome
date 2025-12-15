#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Unit tests for SectorAggregationMV.

Tests the sector aggregation materialized view implementation.

**Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
**Validates: Requirements 8.1, 8.2, 8.3**
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
from datetime import date, timedelta


class TestSectorAggregationMVClass:
    """Test SectorAggregationMV class initialization and properties."""
    
    def test_sector_aggregation_mv_initialization(self):
        """
        Test that SectorAggregationMV initializes correctly.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        # Create instance
        mv = SectorAggregationMV()
        
        # Verify basic properties
        assert mv.name == "sector_aggregation_mv"
        assert mv.materialized_view_name == "sector_aggregation_mv"
        assert mv.materialized_view_schema == "materialized_views"
        assert mv.is_materialized_view is True
        assert mv.refresh_strategy == "full"
        assert mv.source_tables == [
            "rawdata.industry_classification",
            "rawdata.stock_daily"
        ]
    
    def test_sector_aggregation_mv_quality_checks_configured(self):
        """
        Test that quality checks are properly configured.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        
        # Verify quality checks configuration
        assert "null_check" in mv.quality_checks
        assert "outlier_check" in mv.quality_checks
        assert "row_count_change" in mv.quality_checks
        
        # Verify null_check configuration
        null_check = mv.quality_checks["null_check"]
        assert "columns" in null_check
        assert "threshold" in null_check
        assert null_check["threshold"] == 0.05
        assert "trade_date" in null_check["columns"]
        assert "industry_code" in null_check["columns"]
        assert "stock_count" in null_check["columns"]
        assert "close_median" in null_check["columns"]
        
        # Verify outlier_check configuration
        outlier_check = mv.quality_checks["outlier_check"]
        assert outlier_check["method"] == "iqr"
        assert outlier_check["threshold"] == 3.0
        assert "close_median" in outlier_check["columns"]
        assert "close_mean" in outlier_check["columns"]
        
        # Verify row_count_change configuration
        row_count_check = mv.quality_checks["row_count_change"]
        assert row_count_check["threshold"] == 0.3


class TestSectorAggregationMVSQL:
    """Test SQL generation for sector aggregation MV."""
    
    @pytest.mark.asyncio
    async def test_define_materialized_view_sql_returns_valid_sql(self):
        """
        Test that define_materialized_view_sql returns valid SQL.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify SQL is a string
        assert isinstance(sql, str)
        assert len(sql) > 0
        
        # Verify SQL contains key components
        assert "CREATE MATERIALIZED VIEW" in sql
        assert "materialized_views.sector_aggregation_mv" in sql
        assert "rawdata.stock_daily" in sql
        assert "rawdata.industry_classification" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_data_alignment(self):
        """
        Test that SQL includes data alignment logic.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify data alignment logic for ts_code
        assert "ts_code_std" in sql or "ts_code" in sql
        assert ".SH" in sql or ".SZ" in sql
        assert "CASE" in sql
        
        # Verify data alignment logic for trade_date
        assert "trade_date" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_industry_grouping(self):
        """
        Test that SQL includes industry grouping logic.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify industry grouping
        assert "industry_code" in sql
        assert "industry_name" in sql
        assert "GROUP BY" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_cross_sectional_statistics(self):
        """
        Test that SQL includes cross-sectional statistics.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify cross-sectional statistics
        assert "PERCENTILE_CONT" in sql
        assert "AVG(" in sql
        assert "STDDEV" in sql
        assert "COUNT(DISTINCT" in sql
        assert "SUM(" in sql
        assert "MIN(" in sql
        assert "MAX(" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_data_validation(self):
        """
        Test that SQL includes data validation logic.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
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
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify lineage metadata
        assert "_source_tables" in sql
        assert "_processed_at" in sql
        assert "_data_version" in sql
        assert "NOW()" in sql
        assert "CURRENT_DATE" in sql


class TestSectorAggregationMVIntegration:
    """Integration tests for sector aggregation MV."""
    
    @pytest.mark.asyncio
    async def test_fetch_data_returns_none(self):
        """
        Test that fetch_data returns None (MV tasks don't fetch data).
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        result = await mv.fetch_data()
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_process_data_returns_none(self):
        """
        Test that process_data returns None (MV tasks don't process data).
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        result = await mv.process_data(pd.DataFrame())
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_save_result_does_nothing(self):
        """
        Test that save_result does nothing (MV tasks don't save data).
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        
        # Should not raise any exception
        await mv.save_result(pd.DataFrame())
    
    def test_get_materialized_view_info(self):
        """
        Test that get_materialized_view_info returns correct information.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
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
        assert info["name"] == "sector_aggregation_mv"
        assert info["materialized_view_name"] == "sector_aggregation_mv"
        assert info["full_name"] == "materialized_views.sector_aggregation_mv"
        assert info["refresh_strategy"] == "full"
        assert len(info["source_tables"]) == 2


class TestSectorAggregationMVDataQuality:
    """Test data quality checks for sector aggregation MV."""
    
    def test_quality_checks_include_null_check(self):
        """
        Test that quality checks include null value detection.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        
        assert "null_check" in mv.quality_checks
        null_check = mv.quality_checks["null_check"]
        assert "columns" in null_check
        assert "trade_date" in null_check["columns"]
        assert "industry_code" in null_check["columns"]
        assert "stock_count" in null_check["columns"]
        assert "close_median" in null_check["columns"]
    
    def test_quality_checks_include_outlier_check(self):
        """
        Test that quality checks include outlier detection.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        
        assert "outlier_check" in mv.quality_checks
        outlier_check = mv.quality_checks["outlier_check"]
        assert outlier_check["method"] == "iqr"
        assert "columns" in outlier_check
        assert "close_median" in outlier_check["columns"]
        assert "close_mean" in outlier_check["columns"]
    
    def test_quality_checks_include_row_count_change(self):
        """
        Test that quality checks include row count change detection.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.3**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        
        assert "row_count_change" in mv.quality_checks
        row_count_check = mv.quality_checks["row_count_change"]
        assert "threshold" in row_count_check
        assert row_count_check["threshold"] == 0.3


class TestSectorAggregationMVSQLColumns:
    """Test that SQL generates all required columns."""
    
    @pytest.mark.asyncio
    async def test_sql_includes_price_statistics_columns(self):
        """
        Test that SQL includes price statistics columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
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
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
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
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify turnover rate statistics columns
        assert "turnover_rate_median" in sql
        assert "turnover_rate_q25" in sql
        assert "turnover_rate_q75" in sql
        assert "turnover_rate_mean" in sql
        assert "turnover_rate_std" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_amount_statistics_columns(self):
        """
        Test that SQL includes amount statistics columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify amount statistics columns
        assert "amount_median" in sql
        assert "amount_mean" in sql
        assert "amount_total" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_ratio_columns(self):
        """
        Test that SQL includes ratio columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify ratio columns
        assert "high_price_ratio" in sql
        assert "high_vol_ratio" in sql
        assert "high_turnover_ratio" in sql
    
    @pytest.mark.asyncio
    async def test_sql_includes_key_columns(self):
        """
        Test that SQL includes key columns for grouping.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify key columns
        assert "trade_date" in sql
        assert "industry_code" in sql
        assert "industry_name" in sql
        assert "stock_count" in sql


class TestSectorAggregationMVDataTypes:
    """Test that SQL uses correct data types."""
    
    @pytest.mark.asyncio
    async def test_sql_uses_decimal_for_price_columns(self):
        """
        Test that SQL uses DECIMAL type for price columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify DECIMAL type for price columns
        assert "CAST(close_median AS DECIMAL" in sql
        assert "CAST(close_mean AS DECIMAL" in sql
        assert "CAST(close_std AS DECIMAL" in sql
    
    @pytest.mark.asyncio
    async def test_sql_uses_integer_for_stock_count(self):
        """
        Test that SQL uses INTEGER type for stock_count.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify INTEGER type for stock_count
        assert "CAST(stock_count AS INTEGER)" in sql
    
    @pytest.mark.asyncio
    async def test_sql_uses_decimal_for_ratio_columns(self):
        """
        Test that SQL uses DECIMAL type for ratio columns.
        
        **Feature: materialized-views-system, Property 1: Evaluation criteria consistency**
        **Validates: Requirements 8.1, 8.2**
        """
        from alphahome.processors.tasks.market import SectorAggregationMV
        
        mv = SectorAggregationMV()
        sql = await mv.define_materialized_view_sql()
        
        # Verify DECIMAL type for ratio columns
        assert "CAST(high_price_ratio AS DECIMAL" in sql
        assert "CAST(high_vol_ratio AS DECIMAL" in sql
        assert "CAST(high_turnover_ratio AS DECIMAL" in sql