#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AlphaHome 回测模块测试用例

测试回测框架的基本功能，包括数据适配器、配置管理等。
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, date

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alphahome.backtest.config.backtest_config import BacktestConfig, ConfigTemplates
from alphahome.backtest.adapters.asset_finder import AlphaHomeAssetFinder
from alphahome.backtest.adapters.bar_reader import AlphaHomeBarReader


class TestBacktestConfig:
    """测试回测配置类"""
    
    def test_default_config_creation(self):
        """测试默认配置创建"""
        config = BacktestConfig()
        
        assert config.start_date == "2020-01-01"
        assert config.end_date == "2023-12-31"
        assert config.initial_capital == 100000.0
        assert config.data_frequency == "daily"
        assert config.trading_calendar == "XSHG"
    
    def test_config_validation(self):
        """测试配置验证"""
        # 有效配置
        config = BacktestConfig(
            start_date="2022-01-01",
            end_date="2022-12-31",
            initial_capital=50000.0
        )
        assert config.validate() == True
        
        # 无效配置：负初始资金
        config_invalid = BacktestConfig(initial_capital=-1000)
        assert config_invalid.validate() == False
        
        # 无效配置：开始日期晚于结束日期
        with pytest.raises(ValueError):
            BacktestConfig(start_date="2023-01-01", end_date="2022-01-01")
    
    def test_config_from_dict(self):
        """测试从字典创建配置"""
        config_dict = {
            "start_date": "2021-01-01",
            "end_date": "2021-12-31",
            "initial_capital": 200000.0,
            "strategy_name": "test_strategy"
        }
        
        config = BacktestConfig.from_dict(config_dict)
        assert config.start_date == "2021-01-01"
        assert config.initial_capital == 200000.0
        assert config.strategy_name == "test_strategy"
    
    def test_config_templates(self):
        """测试配置模板"""
        # 测试双均线策略模板
        dual_ma_config = ConfigTemplates.dual_moving_average()
        assert dual_ma_config.strategy_name == "dual_moving_average"
        assert "short_window" in dual_ma_config.strategy_params
        assert "long_window" in dual_ma_config.strategy_params
        
        # 测试买入持有策略模板
        buy_hold_config = ConfigTemplates.buy_and_hold()
        assert buy_hold_config.strategy_name == "buy_and_hold"
        assert "stocks" in buy_hold_config.strategy_params
    
    def test_config_date_range(self):
        """测试日期范围获取"""
        config = BacktestConfig(
            start_date="2022-06-01",
            end_date="2022-08-31"
        )
        
        start, end = config.get_date_range()
        assert isinstance(start, pd.Timestamp)
        assert isinstance(end, pd.Timestamp)
        assert start < end
    
    def test_config_zipline_params(self):
        """测试 zipline 参数创建"""
        config = BacktestConfig(
            start_date="2022-01-01",
            end_date="2022-12-31",
            initial_capital=100000.0
        )
        
        params = config.create_zipline_params()
        assert 'start' in params
        assert 'end' in params
        assert 'capital_base' in params
        assert params['capital_base'] == 100000.0


class TestAssetFinder:
    """测试资产查找器"""
    
    @pytest.fixture
    def mock_db_manager(self):
        """模拟数据库管理器"""
        db_manager = Mock()
        db_manager.fetch = AsyncMock(return_value=[
            {
                'ts_code': '000001.SZ',
                'symbol': '000001',
                'name': '平安银行',
                'list_date': date(1991, 4, 3),
                'delist_date': None,
                'exchange': 'SZSE',
                'market': '主板'
            },
            {
                'ts_code': '000002.SZ',
                'symbol': '000002',
                'name': '万科A',
                'list_date': date(1991, 1, 29),
                'delist_date': None,
                'exchange': 'SZSE',
                'market': '主板'
            }
        ])
        return db_manager
    
    def test_asset_finder_creation(self, mock_db_manager):
        """测试资产查找器创建"""
        finder = AlphaHomeAssetFinder(mock_db_manager)
        assert finder.db_manager == mock_db_manager
        assert finder.stock_table == "tushare_stock_basic"
    
    def test_ts_code_to_sid(self, mock_db_manager):
        """测试代码到SID的转换"""
        finder = AlphaHomeAssetFinder(mock_db_manager)
        
        sid1 = finder._ts_code_to_sid('000001.SZ')
        sid2 = finder._ts_code_to_sid('000002.SZ')
        
        assert isinstance(sid1, int)
        assert isinstance(sid2, int)
        assert sid1 != sid2  # 不同代码应该有不同的SID
        
        # 相同代码应该有相同的SID
        sid1_again = finder._ts_code_to_sid('000001.SZ')
        assert sid1 == sid1_again


class TestBarReader:
    """测试价格数据读取器"""
    
    @pytest.fixture
    def mock_db_manager(self):
        """模拟数据库管理器"""
        db_manager = Mock()
        db_manager.fetch = AsyncMock(return_value=[
            {
                'ts_code': '000001.SZ',
                'trade_date': date(2023, 1, 3),
                'open': 12.50,
                'high': 12.80,
                'low': 12.40,
                'close': 12.70,
                'volume': 1000000
            },
            {
                'ts_code': '000001.SZ',
                'trade_date': date(2023, 1, 4),
                'open': 12.70,
                'high': 12.90,
                'low': 12.60,
                'close': 12.85,
                'volume': 1200000
            }
        ])
        db_manager.fetch_val = AsyncMock(return_value=date(2023, 12, 29))
        return db_manager
    
    def test_bar_reader_creation(self, mock_db_manager):
        """测试价格数据读取器创建"""
        with patch('alphahome.backtest.adapters.bar_reader.get_calendar'):
            reader = AlphaHomeBarReader(mock_db_manager)
            assert reader.db_manager == mock_db_manager
            assert reader.table_name == "tushare_stock_daily"
    
    def test_asset_to_ts_code(self, mock_db_manager):
        """测试资产到代码的转换"""
        with patch('alphahome.backtest.adapters.bar_reader.get_calendar'):
            reader = AlphaHomeBarReader(mock_db_manager)
            
            # 模拟资产对象
            mock_asset = Mock()
            mock_asset.symbol = '000001.SZ'
            
            ts_code = reader._asset_to_ts_code(mock_asset)
            assert ts_code == '000001.SZ'


class TestIntegration:
    """集成测试"""
    
    def test_config_and_strategy_integration(self):
        """测试配置与策略的集成"""
        # 创建配置
        config = ConfigTemplates.dual_moving_average()
        
        # 获取策略参数
        strategy_params = config.get_strategy_params()
        assert 'short_window' in strategy_params
        assert 'long_window' in strategy_params
        assert 'stocks' in strategy_params
        
        # 修改策略参数
        config.set_strategy_param('short_window', 10)
        updated_params = config.get_strategy_params()
        assert updated_params['short_window'] == 10
    
    def test_date_validation_across_components(self):
        """测试跨组件的日期验证"""
        # 测试有效日期范围
        valid_config = BacktestConfig(
            start_date="2022-01-01",
            end_date="2022-12-31"
        )
        assert valid_config.validate()
        
        # 测试日期范围获取
        start_date, end_date = valid_config.get_date_range()
        assert start_date.year == 2022
        assert end_date.year == 2022
        assert start_date.month == 1
        assert end_date.month == 12


class TestErrorHandling:
    """错误处理测试"""
    
    def test_invalid_date_formats(self):
        """测试无效日期格式处理"""
        with pytest.raises(ValueError):
            BacktestConfig(start_date="invalid-date")
        
        with pytest.raises(ValueError):
            BacktestConfig(end_date="2022-13-32")  # 无效月份和日期
    
    def test_invalid_parameters(self):
        """测试无效参数处理"""
        config = BacktestConfig(
            initial_capital=-1000,  # 负数资金
            max_position_size=1.5,  # 超过1的持仓比例
            commission_rate=-0.01   # 负数佣金
        )
        
        assert not config.validate()
    
    def test_missing_required_fields(self):
        """测试缺失必需字段的处理"""
        # 测试空的策略参数
        config = BacktestConfig()
        assert config.strategy_params == {}
        
        # 测试空股票列表
        config.strategy_params = {'stocks': []}
        params = config.get_strategy_params()
        assert params['stocks'] == []


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"]) 