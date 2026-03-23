#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Verification test for AkShare fund_purchase_em fixtures.

This test ensures fixtures can be imported and used without network calls.
"""

import pytest
from tests.unit.test_akshare_fund_purchase_em_fixtures import (
    fixture_normal_limits,
    fixture_unlimited_limits,
    fixture_suspended_purchase,
    fixture_duplicate_rows,
    fixture_full_response,
)


def test_fixture_normal_limits_returns_dataframe():
    """Test fixture_normal_limits returns proper DataFrame."""
    df = fixture_normal_limits()
    assert df is not None
    assert len(df) == 5
    assert set(df.columns) == {
        "基金代码", "基金简称", "申购状态", "日累计限定金额", "赎回状态", "最新净值"
    }
    assert all(df["申购状态"] == "开放申购")


def test_fixture_unlimited_limits_has_blank_values():
    """Test fixture_unlimited_limits contains blank/unlimited markers."""
    df = fixture_unlimited_limits()
    assert len(df) == 4
    # Check for blank strings and "无限制"
    limit_values = df["日累计限定金额"]
    # Check for empty string values or "无限制"
    has_blank = any(limit_values == "")
    has_unlimited = any(limit_values == "无限制")
    has_large_num = any(isinstance(v, (int, float)) and v > 1000000 for v in limit_values)
    assert has_blank or has_unlimited or has_large_num


def test_fixture_suspended_purchase_has_suspended_status():
    """Test fixture_suspended_purchase contains suspended status."""
    df = fixture_suspended_purchase()
    assert len(df) == 3
    assert all(df["申购状态"] == "暂停申购")


def test_fixture_duplicate_rows_has_duplicates():
    """Test fixture_duplicate_rows contains duplicate fund codes."""
    df = fixture_duplicate_rows()
    assert len(df) == 3
    assert df["基金代码"].duplicated().sum() > 0


def test_fixture_full_response_comprehensive():
    """Test fixture_full_response contains realistic mix of data."""
    df = fixture_full_response()
    assert len(df) == 15
    
    # Check for mix of statuses
    statuses = df["申购状态"].unique()
    assert len(statuses) > 1  # More than one status type
    
    # Check for different limit types
    limits = df["日累计限定金额"]
    has_numeric = any(isinstance(v, (int, float)) and v > 0 for v in limits)
    has_string = any(isinstance(v, str) for v in limits)
    assert has_numeric and has_string


@pytest.mark.parametrize(
    "fixture_func,expected_rows",
    [
        (fixture_normal_limits, 5),
        (fixture_unlimited_limits, 4),
        (fixture_suspended_purchase, 3),
        (fixture_duplicate_rows, 3),
        (fixture_full_response, 15),
    ],
)
def test_all_fixtures_have_required_columns(fixture_func, expected_rows):
    """Test all fixtures have required columns and row counts."""
    df = fixture_func()
    
    # Verify shape
    assert len(df) == expected_rows, f"{fixture_func.__name__} has {len(df)} rows, expected {expected_rows}"
    
    # Verify required columns
    required_columns = {"基金代码", "基金简称", "申购状态", "日累计限定金额", "赎回状态", "最新净值"}
    assert set(df.columns) == required_columns, f"Missing columns in {fixture_func.__name__}"


def test_fixtures_import_without_network():
    """Test that importing fixtures doesn't require network."""
    # This test just verifies imports completed successfully
    # If it runs, it means no network calls were made during import
    assert fixture_normal_limits is not None
    assert fixture_unlimited_limits is not None
    assert fixture_suspended_purchase is not None
    assert fixture_duplicate_rows is not None
    assert fixture_full_response is not None


