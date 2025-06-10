#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置管理器使用演示

展示如何使用统一的ConfigManager：
1. 基本配置获取
2. 模块特定配置
3. 配置热重载
4. 环境变量回退
5. 配置缓存机制
"""

import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from alphahome.common.config_manager import (
    ConfigManager,
    get_database_url,
    get_tushare_token,
    get_task_config,
    get_backtesting_config,
    load_config,
    reload_config
)


def demo_basic_config():
    """演示基本配置获取"""
    print("📋 基本配置演示")
    print("-" * 30)
    
    # 1. 数据库配置
    db_url = get_database_url()
    if db_url:
        # 隐藏敏感信息
        safe_url = db_url.split('@')[1] if '@' in db_url else db_url
        print(f"数据库连接: ***@{safe_url}")
    else:
        print("数据库连接: 未配置")
    
    # 2. API配置
    token = get_tushare_token()
    if token:
        print(f"Tushare Token: {token[:10]}... (已隐藏)")
    else:
        print("Tushare Token: 未配置")
    
    print()


def demo_task_config():
    """演示任务特定配置"""
    print("⚙️  任务配置演示")
    print("-" * 30)
    
    # 1. 获取特定任务的完整配置
    stock_daily_config = get_task_config('tushare_stock_daily')
    print(f"股票日线任务配置: {stock_daily_config}")
    
    # 2. 获取特定配置项
    batch_size = get_task_config('tushare_stock_daily', 'batch_size', 50)
    print(f"批次大小: {batch_size}")
    
    # 3. 不存在的任务返回默认值
    unknown_config = get_task_config('unknown_task', 'some_key', 'default_value')
    print(f"未知任务配置: {unknown_config}")
    
    print()


def demo_backtesting_config():
    """演示回测配置"""
    print("📊 回测配置演示")
    print("-" * 30)
    
    # 1. 获取完整回测配置
    bt_config = get_backtesting_config()
    print("回测配置:")
    for key, value in bt_config.items():
        print(f"  {key}: {value}")
    
    # 2. 获取特定配置项
    cash = get_backtesting_config('default_cash', 100000)
    commission = get_backtesting_config('default_commission', 0.001)
    cache_enabled = get_backtesting_config('cache_data', True)
    
    print(f"\n特定配置项:")
    print(f"  默认资金: {cash:,}")
    print(f"  默认手续费: {commission:.3%}")
    print(f"  启用缓存: {cache_enabled}")
    
    print()


def demo_singleton_pattern():
    """演示单例模式"""
    print("🔄 单例模式演示")
    print("-" * 30)
    
    # 创建多个ConfigManager实例
    config1 = ConfigManager()
    config2 = ConfigManager()
    config3 = ConfigManager()
    
    # 验证它们是同一个实例
    print(f"config1 ID: {id(config1)}")
    print(f"config2 ID: {id(config2)}")
    print(f"config3 ID: {id(config3)}")
    print(f"是否为同一实例: {config1 is config2 is config3}")
    
    print()


def demo_config_structure():
    """演示完整配置结构"""
    print("📁 配置结构演示")
    print("-" * 30)
    
    # 获取完整配置
    full_config = load_config()
    
    print("完整配置结构:")
    for section, content in full_config.items():
        print(f"[{section}]")
        if isinstance(content, dict):
            for key, value in content.items():
                if 'password' in key.lower() or 'token' in key.lower():
                    # 隐藏敏感信息
                    display_value = "***" if value else "未设置"
                else:
                    display_value = value
                print(f"  {key}: {display_value}")
        else:
            print(f"  {content}")
        print()


def demo_config_reload():
    """演示配置重载"""
    print("🔄 配置重载演示")
    print("-" * 30)
    
    # 显示当前配置加载时间
    config_manager = ConfigManager()
    
    print("当前配置:")
    current_config = load_config()
    print(f"配置项数量: {len(current_config)}")
    
    # 重载配置
    print("\n重载配置...")
    try:
        new_config = reload_config()
        print("✅ 配置重载成功")
        print(f"新配置项数量: {len(new_config)}")
    except Exception as e:
        print(f"❌ 配置重载失败: {e}")
    
    print()


def demo_config_path():
    """演示配置文件路径"""
    print("📍 配置路径演示")
    print("-" * 30)
    
    config_manager = ConfigManager()
    
    print(f"应用名称: {config_manager.APP_NAME}")
    print(f"应用作者: {config_manager.APP_AUTHOR}")
    print(f"配置目录: {config_manager.config_dir}")
    print(f"配置文件: {config_manager.config_file}")
    
    # 检查配置文件是否存在
    if os.path.exists(config_manager.config_file):
        stat = os.stat(config_manager.config_file)
        size = stat.st_size
        mtime = datetime.fromtimestamp(stat.st_mtime)
        print(f"文件状态: 存在 ({size} bytes, 修改于 {mtime})")
    else:
        print("文件状态: 不存在")
    
    print()


def demo_environment_fallback():
    """演示环境变量回退"""
    print("🌍 环境变量回退演示")
    print("-" * 30)
    
    # 检查环境变量
    import os
    
    db_url_env = os.environ.get('DATABASE_URL')
    token_env = os.environ.get('TUSHARE_TOKEN')
    
    print("环境变量:")
    print(f"  DATABASE_URL: {'已设置' if db_url_env else '未设置'}")
    print(f"  TUSHARE_TOKEN: {'已设置' if token_env else '未设置'}")
    
    # 对比配置文件和环境变量
    config_db_url = get_database_url()
    config_token = get_tushare_token()
    
    print("\n最终使用的配置:")
    if config_db_url:
        safe_url = config_db_url.split('@')[1] if '@' in config_db_url else config_db_url
        print(f"  数据库URL: ***@{safe_url}")
    else:
        print("  数据库URL: 未配置")
        
    if config_token:
        print(f"  Tushare Token: {config_token[:10]}...")
    else:
        print("  Tushare Token: 未配置")
    
    print()


def main():
    """主演示函数"""
    print("🎯 AlphaHome 配置管理器演示")
    print("=" * 50)
    print("展示统一配置管理器的功能和使用方法")
    print()
    
    try:
        # 1. 基本配置演示
        demo_basic_config()
        
        # 2. 任务配置演示
        demo_task_config()
        
        # 3. 回测配置演示
        demo_backtesting_config()
        
        # 4. 单例模式演示
        demo_singleton_pattern()
        
        # 5. 配置结构演示
        demo_config_structure()
        
        # 6. 配置路径演示
        demo_config_path()
        
        # 7. 环境变量回退演示
        demo_environment_fallback()
        
        # 8. 配置重载演示
        demo_config_reload()
        
        print("🎉 演示完成！")
        print("\n💡 重要特性:")
        print("   ✅ 单例模式确保全局唯一配置")
        print("   ✅ 自动配置文件迁移")
        print("   ✅ 环境变量回退支持")
        print("   ✅ 配置缓存提高性能")
        print("   ✅ 模块特定配置支持")
        print("   ✅ 热重载配置支持")
        
    except Exception as e:
        print(f"❌ 演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 