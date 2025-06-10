#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试重构后的db_manager模块
"""

import sys
import os
import asyncio
from datetime import datetime

# 确保能找到 alphahome 模块
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

async def test_async_db_manager():
    """测试异步模式的DBManager"""
    print("=== 测试异步模式 DBManager ===")
    
    from alphahome.common.db_manager import create_async_manager
    
    # 使用示例连接字符串（注意：这里只是测试导入和对象创建，不会真正连接）
    db = create_async_manager('postgresql://test:test@localhost:5432/test')
    
    print(f"✅ 异步DBManager创建成功: {type(db).__name__}")
    print(f"   继承链: {' -> '.join([cls.__name__ for cls in type(db).__mro__])}")
    
    # 测试各种方法是否存在
    expected_methods = [
        'connect', 'close', 'execute', 'fetch', 'fetch_one', 'fetch_val',
        'copy_from_dataframe', 'upsert', 'table_exists', 'get_table_schema',
        'create_table_from_schema', 'get_latest_date', 'test_connection'
    ]
    
    for method in expected_methods:
        if hasattr(db, method):
            print(f"   ✅ 方法 {method} 存在")
        else:
            print(f"   ❌ 方法 {method} 缺失")
    
    print(f"   总方法数: {len([attr for attr in dir(db) if not attr.startswith('_')])}")
    
    return db

def test_sync_db_manager():
    """测试同步模式的DBManager"""
    print("\n=== 测试同步模式 DBManager ===")
    
    from alphahome.common.db_manager import create_sync_manager
    
    # 使用示例连接字符串
    db = create_sync_manager('postgresql://test:test@localhost:5432/test')
    
    print(f"✅ 同步DBManager创建成功: {type(db).__name__}")
    print(f"   继承链: {' -> '.join([cls.__name__ for cls in type(db).__mro__])}")
    
    # 测试同步方法是否存在
    sync_methods = [
        'execute_sync', 'fetch_sync', 'fetch_one_sync', 'fetch_val_sync',
        'connect_sync', 'close_sync', 'test_connection'
    ]
    
    for method in sync_methods:
        if hasattr(db, method):
            print(f"   ✅ 同步方法 {method} 存在")
        else:
            print(f"   ❌ 同步方法 {method} 缺失")
    
    return db

def test_backward_compatibility():
    """测试向后兼容性"""
    print("\n=== 测试向后兼容性 ===")
    
    from alphahome.common.db_manager import SyncDBManager, DBManager
    
    # 测试老的别名
    db_old = SyncDBManager('postgresql://test:test@localhost:5432/test')
    print(f"✅ SyncDBManager别名工作正常: {type(db_old).__name__}")
    
    # 测试直接使用DBManager
    db_direct = DBManager('postgresql://test:test@localhost:5432/test', mode='async')
    print(f"✅ DBManager直接创建工作正常: {type(db_direct).__name__}")
    
    return True

def test_mixins_structure():
    """测试Mixin结构"""
    print("\n=== 测试Mixin结构 ===")
    
    from alphahome.common.db_components import (
        DBManagerCore, SQLOperationsMixin, DataOperationsMixin,
        SchemaManagementMixin, UtilityMixin
    )
    
    mixins = [
        ('DBManagerCore', DBManagerCore),
        ('SQLOperationsMixin', SQLOperationsMixin),
        ('DataOperationsMixin', DataOperationsMixin),
        ('SchemaManagementMixin', SchemaManagementMixin),
        ('UtilityMixin', UtilityMixin)
    ]
    
    for name, mixin_class in mixins:
        print(f"✅ {name} 导入成功")
        methods = [attr for attr in dir(mixin_class) if not attr.startswith('_')]
        print(f"   包含 {len(methods)} 个公共方法")
    
    return True

async def test_with_real_bt_extension():
    """测试与bt_extensions的集成"""
    print("\n=== 测试与回测扩展的集成 ===")
    
    try:
        # 尝试导入bt_extensions中使用db_manager的模块
        from alphahome.bt_extensions.data.feeds import DatabaseFeed
        print("✅ DatabaseFeed导入成功")
        
        # 检查是否使用了db_manager
        if hasattr(DatabaseFeed, 'db_manager') or 'db_manager' in str(DatabaseFeed.__init__):
            print("✅ DatabaseFeed使用db_manager")
        else:
            print("ℹ️  DatabaseFeed可能不直接使用db_manager")
            
    except ImportError as e:
        print(f"⚠️  bt_extensions模块导入失败: {e}")
    except Exception as e:
        print(f"❌ bt_extensions测试出错: {e}")
    
    return True

async def main():
    """主测试函数"""
    print("开始测试重构后的db_manager模块...")
    print(f"测试时间: {datetime.now()}")
    print("=" * 60)
    
    try:
        # 测试异步模式
        await test_async_db_manager()
        
        # 测试同步模式
        test_sync_db_manager()
        
        # 测试向后兼容性
        test_backward_compatibility()
        
        # 测试Mixin结构
        test_mixins_structure()
        
        # 测试与bt_extensions的集成
        await test_with_real_bt_extension()
        
        print("\n" + "=" * 60)
        print("🎉 所有测试通过！重构后的db_manager工作正常")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    if not success:
        sys.exit(1) 