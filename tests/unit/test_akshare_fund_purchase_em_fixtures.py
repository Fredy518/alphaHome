#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Reusable mocked test fixtures for AkShare fund_purchase_em response shapes.

These fixtures return pandas DataFrames with realistic AkShare API response structure
without making any network calls.
"""

from datetime import datetime
from typing import List

import pandas as pd
import pytest


def fixture_normal_limits() -> pd.DataFrame:
    """
    Fixture for normal numeric daily cumulative limits.
    
    Returns:
        DataFrame with 5 funds, varying limit amounts (1000, 10000, 100000)
        Status: "开放申购" (Open for Purchase)
    """
    return pd.DataFrame({
        "基金代码": ["110022", "110023", "110024", "110025", "110026"],
        "基金简称": ["易方达消费", "易方达医疗", "易方达科技", "易方达新兴", "易方达价值"],
        "申购状态": ["开放申购", "开放申购", "开放申购", "开放申购", "开放申购"],
        "日累计限定金额": [1000.0, 10000.0, 100000.0, 5000.0, 50000.0],
        "赎回状态": ["开放赎回", "开放赎回", "开放赎回", "开放赎回", "开放赎回"],
        "最新净值": [3.2456, 5.1234, 2.8901, 1.5678, 4.2345],
    })


def fixture_unlimited_limits() -> pd.DataFrame:
    """
    Fixture for unlimited/blank limit cases.
    
    Returns:
        DataFrame with 4 funds having blank or unlimited purchase limits
        (empty string, "无限制", or very large number)
    """
    return pd.DataFrame({
        "基金代码": ["000001", "000002", "000003", "000004"],
        "基金简称": ["华夏成长", "华夏红利", "华夏回报", "华夏精选"],
        "申购状态": ["开放申购", "开放申购", "开放申购", "开放申购"],
        "日累计限定金额": ["", "无限制", 999999999.0, ""],
        "赎回状态": ["开放赎回", "开放赎回", "开放赎回", "开放赎回"],
        "最新净值": [8.5432, 3.2109, 2.1987, 1.8765],
    })


def fixture_suspended_purchase() -> pd.DataFrame:
    """
    Fixture for suspended purchase status.
    
    Returns:
        DataFrame with 3 funds in suspended purchase state
        Limit value may be present but irrelevant
    """
    return pd.DataFrame({
        "基金代码": ["163402", "163403", "163404"],
        "基金简称": ["兴全趋势投资", "兴全社会责任", "兴全绿色投资"],
        "申购状态": ["暂停申购", "暂停申购", "暂停申购"],
        "日累计限定金额": [10000.0, 5000.0, ""],
        "赎回状态": ["开放赎回", "开放赎回", "开放赎回"],
        "最新净值": [2.5432, 3.9876, 1.2345],
    })


def fixture_duplicate_rows() -> pd.DataFrame:
    """
    Fixture for duplicate fund codes (edge case).
    
    Returns:
        DataFrame with same fund code appearing twice with different data
        Useful for testing deduplication and merge logic
    """
    return pd.DataFrame({
        "基金代码": ["519674", "519674", "519675"],
        "基金简称": ["银河创新成长", "银河创新成长", "银河领先"],
        "申购状态": ["开放申购", "限制大额申购", "开放申购"],
        "日累计限定金额": [100000.0, 50000.0, 200000.0],
        "赎回状态": ["开放赎回", "开放赎回", "开放赎回"],
        "最新净值": [4.5678, 4.5678, 3.2109],
    })


def fixture_full_response() -> pd.DataFrame:
    """
    Fixture for a full realistic API response.
    
    Returns:
        DataFrame with 15 funds representing a typical AkShare fund_purchase_em
        response, with mix of different statuses, limit types, and fund variations
    """
    return pd.DataFrame({
        "基金代码": [
            "110022", "110023", "163402", "163403", "000001",
            "000002", "519674", "519675", "398011", "398012",
            "470018", "470019", "690005", "690006", "320022",
        ],
        "基金简称": [
            "易方达消费", "易方达医疗", "兴全趋势投资", "兴全社会责任", "华夏成长",
            "华夏红利", "银河创新成长", "银河领先", "普通话指数", "量化对冲",
            "汇添富均衡", "汇添富回报", "南方中证", "南方创业", "诺安成长",
        ],
        "申购状态": [
            "开放申购", "开放申购", "暂停申购", "暂停申购", "开放申购",
            "开放申购", "限制大额申购", "开放申购", "开放申购", "开放申购",
            "开放申购", "暂停申购", "开放申购", "开放申购", "限制大额申购",
        ],
        "日累计限定金额": [
            1000.0, 10000.0, 5000.0, 50000.0, "",
            "无限制", 100000.0, 200000.0, 25000.0, 75000.0,
            500.0, 10000.0, "", 999999999.0, 5000.0,
        ],
        "赎回状态": [
            "开放赎回", "开放赎回", "开放赎回", "开放赎回", "开放赎回",
            "开放赎回", "开放赎回", "开放赎回", "开放赎回", "暂停赎回",
            "开放赎回", "开放赎回", "开放赎回", "开放赎回", "开放赎回",
        ],
        "最新净值": [
            3.2456, 5.1234, 2.5432, 3.9876, 8.5432,
            3.2109, 4.5678, 3.2109, 2.1987, 1.8765,
            3.4567, 4.5678, 2.3456, 1.2345, 5.6789,
        ],
    })


@pytest.fixture
def akshare_normal_limits() -> pd.DataFrame:
    """pytest fixture: normal numeric limits"""
    return fixture_normal_limits()


@pytest.fixture
def akshare_unlimited_limits() -> pd.DataFrame:
    """pytest fixture: unlimited/blank limits"""
    return fixture_unlimited_limits()


@pytest.fixture
def akshare_suspended_purchase() -> pd.DataFrame:
    """pytest fixture: suspended purchase status"""
    return fixture_suspended_purchase()


@pytest.fixture
def akshare_duplicate_rows() -> pd.DataFrame:
    """pytest fixture: duplicate fund codes"""
    return fixture_duplicate_rows()


@pytest.fixture
def akshare_full_response() -> pd.DataFrame:
    """pytest fixture: full realistic API response"""
    return fixture_full_response()


@pytest.fixture(params=[
    "fixture_normal_limits",
    "fixture_unlimited_limits",
    "fixture_suspended_purchase",
])
def akshare_all_variants(request) -> pd.DataFrame:
    """
    pytest fixture: parametrized fixture covering all main variants.
    
    Use with @pytest.mark.parametrize or access via request.getfixturevalue()
    """
    fixture_name = request.param
    fixture_func = globals().get(fixture_name)
    if fixture_func is None:
        raise ValueError(f"Unknown fixture: {fixture_name}")
    return fixture_func()


__all__ = [
    "fixture_normal_limits",
    "fixture_unlimited_limits",
    "fixture_suspended_purchase",
    "fixture_duplicate_rows",
    "fixture_full_response",
    "akshare_normal_limits",
    "akshare_unlimited_limits",
    "akshare_suspended_purchase",
    "akshare_duplicate_rows",
    "akshare_full_response",
    "akshare_all_variants",
]
