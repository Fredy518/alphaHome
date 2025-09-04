#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子计算器单元测试 (v1.1 - 简化权重系统)

测试覆盖：
1. YoY差序列构建 (52周)
2. 效率惊喜因子计算 (简化权重)
3. 数据源信息保留
4. 缺失数据处理
"""

import pandas as pd
import numpy as np

from research.pgs_factor.processors.production_g_factor_calculator import ProductionGFactorCalculator


class DummyContext:
    """模拟数据库上下文"""
    def __init__(self):
        self.db_manager = None


def build_weekly_group(num_weeks: int = 120, start_date: str = "2020-01-03",
                       base: float = 0.0, step: float = 1.0,
                       add_variation: bool = False, data_source: str = 'express') -> pd.DataFrame:
    """构建测试用周频数据序列

    Args:
        num_weeks: 序列长度（周数）
        start_date: 开始日期
        base: 基准值
        step: 步长
        add_variation: 是否添加随机变化
        data_source: 数据源类型

    Returns:
        测试数据DataFrame
    """
    dates = pd.to_datetime(pd.date_range(start=start_date, periods=num_weeks, freq='7D'))
    idx = np.arange(num_weeks, dtype=float)

    if add_variation:
        # 引入一些确定性的变化，避免YoY差序列方差为0
        variation = (idx % 13) / 13.0
    else:
        variation = 0.0

    p_score = base + step * idx + variation

    df = pd.DataFrame({
        'ts_code': ['TEST'] * num_weeks,
        'calc_date': dates,
        'p_score': p_score,
        'data_source': [data_source] * num_weeks,
        'ann_date': dates
    })
    return df


def test_build_yoy_delta_series_52w_constant_step():
    """测试52周YoY差序列构建 - 常数步长"""
    ctx = DummyContext()
    calc = ProductionGFactorCalculator(ctx, {})

    group = build_weekly_group(num_weeks=120, step=1.0, add_variation=False)
    deltas = calc._build_yoy_delta_series_52w(group)

    # 52周间隔（364天），线性步长为1，则YoY差应接近52
    assert len(deltas) > 50
    assert all(abs(d - 52.0) < 1e-6 for d in deltas), f"unexpected deltas range: {min(deltas)}~{max(deltas)}"


def test_calculate_efficiency_surprise_fallback_when_std_zero():
    """测试效率惊喜计算 - 标准差为0时的回退逻辑"""
    ctx = DummyContext()
    calc = ProductionGFactorCalculator(ctx, {})

    group = build_weekly_group(num_weeks=120, step=1.0, add_variation=False)
    latest = group.iloc[-1]
    yoy = calc._get_yoy_p_factor_data(group, latest['calc_date'])

    # 因为YoY差序列常数（52），std=0，逻辑会回退为 delta * 1.0
    delta = float(latest['p_score']) - float(yoy['p_score'])
    result = calc._calculate_efficiency_surprise(group, latest, yoy)

    assert abs(result - delta * 1.0) < 1e-6


def test_calculate_efficiency_surprise_with_variation():
    """测试效率惊喜计算 - 有变化的序列"""
    ctx = DummyContext()
    calc = ProductionGFactorCalculator(ctx, {})

    group = build_weekly_group(num_weeks=120, step=1.0, add_variation=True)
    latest = group.iloc[-1]
    yoy = calc._get_yoy_p_factor_data(group, latest['calc_date'])

    delta = float(latest['p_score']) - float(yoy['p_score'])
    deltas = calc._build_yoy_delta_series_52w(group)
    std_delta = np.std(deltas) if len(deltas) > 1 else 0.0

    res = calc._calculate_efficiency_surprise(group, latest, yoy)

    if std_delta > 0:
        expect = (delta / std_delta) * 1.0
        assert abs(res - expect) < 1e-6
    else:
        expect = delta * 1.0
        assert abs(res - expect) < 1e-6


def test_calculate_efficiency_surprise_when_yoy_missing_returns_nan():
    """测试效率惊喜计算 - 缺失YoY数据时返回NaN"""
    ctx = DummyContext()
    calc = ProductionGFactorCalculator(ctx, {})

    # 构造极短序列，使得找不到同比点
    group = build_weekly_group(num_weeks=10, step=1.0, add_variation=False)
    latest = group.iloc[-1]
    yoy = calc._get_yoy_p_factor_data(group, latest['calc_date'])

    assert yoy is None
    res = calc._calculate_efficiency_surprise(group, latest, yoy)
    assert np.isnan(res)


def test_data_source_information_preserved():
    """测试数据源信息被正确保留"""
    ctx = DummyContext()
    calc = ProductionGFactorCalculator(ctx, {})

    # 创建包含不同数据源的测试数据
    group = build_weekly_group(num_weeks=120, data_source='forecast')
    latest = group.iloc[-1]

    # 计算子因子（虽然权重统一为1.0，但数据源信息应该保留）
    g_factors = calc._calculate_g_subfactors(group, latest, latest['calc_date'])

    assert g_factors is not None
    assert g_factors['data_source'] == 'forecast'
    assert g_factors['data_timeliness_weight'] == 1.0


def test_unified_weight_system():
    """测试统一权重系统"""
    ctx = DummyContext()
    calc = ProductionGFactorCalculator(ctx, {})

    # 验证权重统一为1.0
    assert calc.timeliness_weights == {'express': 1.0, 'forecast': 1.0, 'report': 1.0}

    # 测试不同数据源的因子计算结果应该相同（因为权重统一）
    group_express = build_weekly_group(num_weeks=120, data_source='express')
    group_forecast = build_weekly_group(num_weeks=120, data_source='forecast')

    latest_express = group_express.iloc[-1]
    latest_forecast = group_forecast.iloc[-1]

    yoy_express = calc._get_yoy_p_factor_data(group_express, latest_express['calc_date'])
    yoy_forecast = calc._get_yoy_p_factor_data(group_forecast, latest_forecast['calc_date'])

    # 因子计算结果应该相同（因为权重统一）
    result_express = calc._calculate_efficiency_surprise(group_express, latest_express, yoy_express)
    result_forecast = calc._calculate_efficiency_surprise(group_forecast, latest_forecast, yoy_forecast)

    # 由于数据完全相同，结果应该相同
    assert abs(result_express - result_forecast) < 1e-10


