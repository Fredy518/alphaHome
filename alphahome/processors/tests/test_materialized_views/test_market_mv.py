#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Consolidated tests for market data materialized views.

This module consolidates tests for:
- MarketTechnicalIndicatorsMV
- SectorAggregationMV

**Feature: materialized-views-system**
**Validates: Requirements 8.1, 8.2, 8.3**
"""

# Import all tests from market technical indicators
from alphahome.processors.tests.test_materialized_views.test_market_technical_indicators_mv import (
    TestMarketTechnicalIndicatorsMVClass,
    TestMarketTechnicalIndicatorsMVSQL,
    TestMarketTechnicalIndicatorsMVIntegration,
    TestMarketTechnicalIndicatorsMVDataQuality,
    TestMarketTechnicalIndicatorsMVSQLColumns,
)

# Import all tests from sector aggregation
from alphahome.processors.tests.test_materialized_views.test_sector_aggregation_mv import (
    TestSectorAggregationMVClass,
    TestSectorAggregationMVSQL,
    TestSectorAggregationMVIntegration,
    TestSectorAggregationMVDataQuality,
    TestSectorAggregationMVSQLColumns,
    TestSectorAggregationMVDataTypes,
)

__all__ = [
    # Market Technical Indicators MV Tests
    "TestMarketTechnicalIndicatorsMVClass",
    "TestMarketTechnicalIndicatorsMVSQL",
    "TestMarketTechnicalIndicatorsMVIntegration",
    "TestMarketTechnicalIndicatorsMVDataQuality",
    "TestMarketTechnicalIndicatorsMVSQLColumns",
    # Sector Aggregation MV Tests
    "TestSectorAggregationMVClass",
    "TestSectorAggregationMVSQL",
    "TestSectorAggregationMVIntegration",
    "TestSectorAggregationMVDataQuality",
    "TestSectorAggregationMVSQLColumns",
    "TestSectorAggregationMVDataTypes",
]
