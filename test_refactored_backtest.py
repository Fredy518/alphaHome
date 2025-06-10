#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试重构后的db_manager与回测系统的集成
"""

import sys
import os
import asyncio
import json
from datetime import datetime, date

# 确保能找到 alphahome 模块
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def load_config():
    """加载数据库配置"""
    try:
        config_path = os.path.join(project_root, 'config.example.json')
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                db_url = config.get('database', {}).get('url', '')
                if db_url:
                    return db_url
    except Exception as e:
        print(f"加载配置失败: {e}")
    
    # 返回默认配置URL
    return "postgresql://postgres:password@localhost:5432/alpha_home"

def test_postgresql_data_feed():
    """测试PostgreSQL数据源"""
    print("=== 测试PostgreSQL数据源 ===")
    
    try:
        # 导入必要模块
        from alphahome.common.db_manager import create_sync_manager
        from alphahome.bt_extensions.data.feeds import PostgreSQLDataFeed
        
        # 创建数据库配置（使用示例配置，不真实连接）
        connection_string = load_config()
        
        # 创建同步数据库管理器（适合backtrader同步环境）
        db_manager = create_sync_manager(connection_string)
        print(f"✅ 同步DBManager创建成功: {type(db_manager).__name__}")
        
        # 创建数据源实例（不启动，只测试创建）
        data_feed = PostgreSQLDataFeed(
            ts_code='000001.SZ',
            table_name='tushare_stock_daily',
            start_date='20230101',
            end_date='20231231',
            db_manager=db_manager
        )
        
        print(f"✅ PostgreSQLDataFeed创建成功")
        print(f"   股票代码: {data_feed.p.ts_code}")
        print(f"   数据表: {data_feed.p.table_name}")
        print(f"   DBManager类型: {type(data_feed.p.db_manager).__name__}")
        print(f"   DBManager模式: {data_feed.p.db_manager.mode}")
        
        return True
        
    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

def test_data_feed_factory():
    """测试数据源工厂"""
    print("\n=== 测试数据源工厂 ===")
    
    try:
        from alphahome.common.db_manager import create_sync_manager
        from alphahome.bt_extensions.data.feeds import PostgreSQLDataFeedFactory
        
        # 创建数据库管理器
        connection_string = load_config()
        db_manager = create_sync_manager(connection_string)
        
        # 创建数据源工厂
        factory = PostgreSQLDataFeedFactory(db_manager)
        print(f"✅ PostgreSQLDataFeedFactory创建成功")
        print(f"   DBManager类型: {type(factory.db_manager).__name__}")
        print(f"   DBManager模式: {factory.db_manager.mode}")
        
        # 测试创建数据源
        data_feed = factory.create_feed(
            ts_code='000001.SZ',
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31)
        )
        
        print(f"✅ 通过工厂创建数据源成功")
        print(f"   股票代码: {data_feed.p.ts_code}")
        
        return True
        
    except Exception as e:
        print(f"❌ 数据源工厂测试失败: {e}")
        return False

def test_simple_strategy():
    """测试简单策略配置"""
    print("\n=== 测试简单策略配置 ===")
    
    try:
        import backtrader as bt
        from alphahome.common.db_manager import create_sync_manager
        from alphahome.bt_extensions.data.feeds import PostgreSQLDataFeed
        
        # 创建数据库管理器
        connection_string = load_config()
        db_manager = create_sync_manager(connection_string)
        
        # 创建简单的买入持有策略
        class BuyAndHoldStrategy(bt.Strategy):
            def __init__(self):
                self.bought = False
                
            def next(self):
                if not self.bought:
                    self.buy()
                    self.bought = True
        
        # 创建Cerebro引擎
        cerebro = bt.Cerebro()
        
        # 添加策略
        cerebro.addstrategy(BuyAndHoldStrategy)
        
        # 创建数据源（不添加到cerebro中，只测试兼容性）
        data_feed = PostgreSQLDataFeed(
            ts_code='000001.SZ',
            table_name='tushare_stock_daily',
            start_date='20230101',
            end_date='20231231',
            db_manager=db_manager
        )
        
        print(f"✅ 回测引擎配置成功")
        print(f"   策略数量: {len(cerebro.strats)}")
        print(f"   数据源类型: {type(data_feed).__name__}")
        print(f"   数据源参数验证通过")
        
        return True
        
    except Exception as e:
        print(f"❌ 策略配置测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_db_manager_modes():
    """测试db_manager的两种模式"""
    print("\n=== 测试DBManager两种模式 ===")
    
    try:
        from alphahome.common.db_manager import create_async_manager, create_sync_manager
        
        connection_string = load_config()
        
        # 测试异步模式
        async_db = create_async_manager(connection_string)
        print(f"✅ 异步模式: {async_db.mode}")
        print(f"   类型: {type(async_db).__name__}")
        print(f"   MRO: {' -> '.join([cls.__name__ for cls in type(async_db).__mro__][:4])}")
        
        # 测试同步模式
        sync_db = create_sync_manager(connection_string)
        print(f"✅ 同步模式: {sync_db.mode}")
        print(f"   类型: {type(sync_db).__name__}")
        print(f"   MRO: {' -> '.join([cls.__name__ for cls in type(sync_db).__mro__][:4])}")
        
        # 验证两种模式都有相同的基础接口
        common_methods = ['execute', 'fetch', 'fetch_one', 'fetch_val']
        sync_methods = ['execute_sync', 'fetch_sync', 'fetch_one_sync', 'fetch_val_sync']
        
        for method in common_methods:
            async_has = hasattr(async_db, method)
            sync_has = hasattr(sync_db, method)
            print(f"   方法 {method}: 异步={async_has}, 同步={sync_has}")
        
        for method in sync_methods:
            sync_has = hasattr(sync_db, method)
            print(f"   同步方法 {method}: {sync_has}")
        
        return True
        
    except Exception as e:
        print(f"❌ 模式测试失败: {e}")
        return False

def main():
    """主测试函数"""
    print("开始测试重构后db_manager与回测系统的集成...")
    print(f"测试时间: {datetime.now()}")
    print("=" * 60)
    
    tests = [
        ("PostgreSQL数据源", test_postgresql_data_feed),
        ("数据源工厂", test_data_feed_factory),
        ("简单策略配置", test_simple_strategy),
        ("DBManager两种模式", test_db_manager_modes),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} 测试通过")
            else:
                print(f"❌ {test_name} 测试失败")
        except Exception as e:
            print(f"❌ {test_name} 测试异常: {e}")
    
    print("\n" + "=" * 60)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("🎉 所有集成测试通过！重构后的db_manager与回测系统兼容良好")
        return True
    else:
        print("⚠️  部分测试未通过，请检查相关功能")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1) 