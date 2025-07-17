import pytest
import pandas as pd
import time
import shutil
from pathlib import Path

from alphahome.bt_extensions.utils.cache_manager import CacheManager

# 创建一个临时的缓存目录用于测试
@pytest.fixture
def temp_cache_dir(tmp_path):
    """创建一个临时目录用于磁盘缓存测试"""
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    yield cache_dir
    # 测试结束后清理目录
    shutil.rmtree(cache_dir)

@pytest.fixture
def cache_manager(temp_cache_dir):
    """创建一个具有小容量的CacheManager实例用于测试"""
    # 使用非常小的容量来方便测试驱逐策略
    manager = CacheManager(
        max_memory_items=3,
        max_memory_mb=1,
        disk_cache_dir=str(temp_cache_dir),
        disk_cache_ttl=1  # 1秒过期，便于测试
    )
    # 清理缓存以确保测试隔离
    manager.clear()
    return manager

@pytest.fixture
def memory_only_cache_manager():
    """创建一个只启用内存缓存的CacheManager实例"""
    manager = CacheManager(
        max_memory_items=3,
        enable_disk_cache=False  # 禁用磁盘缓存
    )
    manager.clear()
    return manager


def test_set_and_get_memory(cache_manager):
    """测试基本的内存缓存设置和获取功能"""
    cache_manager.set("key1", "value1")
    cache_manager.set("key2", 123)
    df = pd.DataFrame({'a': [1, 2]})
    cache_manager.set("key3", df)

    assert cache_manager.get("key1") == "value1"
    assert cache_manager.get("key2") == 123
    pd.testing.assert_frame_equal(cache_manager.get("key3"), df)
    assert cache_manager.get("non_existent_key") is None

def test_memory_lru_eviction(memory_only_cache_manager):
    """测试内存缓存的LRU (最近最少使用) 驱逐策略 (禁用磁盘)"""
    # 使用 memory_only_cache_manager fixture
    cache_manager = memory_only_cache_manager

    cache_manager.set("key1", "value1") # 最老
    cache_manager.set("key2", "value2")
    cache_manager.set("key3", "value3")

    # 此时缓存已满 (max_memory_items=3)
    # 访问 key1，使其变为最近使用的
    cache_manager.get("key1")

    # 添加新项 key4，应该会驱逐最久未使用的 key2
    cache_manager.set("key4", "value4")

    assert cache_manager.get("key1") == "value1"
    assert cache_manager.get("key3") == "value3"
    assert cache_manager.get("key4") == "value4"
    assert cache_manager.get("key2") is None, "key2 应该已被驱逐"

def test_disk_caching(cache_manager):
    """测试磁盘缓存的写入和读取"""
    key = "disk_key_1"
    value = "this should be on disk"
    
    cache_manager.set(key, value)
    
    # 清空内存缓存来强制从磁盘读取
    cache_manager.clear_memory()
    
    # 确认内存中不存在
    assert key not in cache_manager._memory_cache
    
    # 从磁盘获取
    retrieved_value = cache_manager.get(key)
    assert retrieved_value == value
    
    # 验证获取后，它现在在内存中
    assert key in cache_manager._memory_cache

def test_disk_cache_expiration(cache_manager):
    """测试磁盘缓存的过期功能"""
    key = "expired_key"
    value = "this will expire"

    cache_manager.set(key, value)
    
    # 清空内存以确保我们之后会检查磁盘
    cache_manager.clear_memory()

    # 等待超过TTL（1秒）
    time.sleep(1.5)
    
    # 尝试获取，应该因为过期而失败
    retrieved_value = cache_manager.get(key)
    assert retrieved_value is None, "过期的缓存应该返回 None"

def test_cache_stats(cache_manager):
    """测试缓存统计功能是否正确记录"""
    # 1. 内存未命中，然后设置
    assert cache_manager.get("stat_key") is None
    cache_manager.set("stat_key", "stat_value")

    # 2. 内存命中
    assert cache_manager.get("stat_key") == "stat_value"

    # 3. 磁盘命中
    cache_manager.clear_memory()
    assert cache_manager.get("stat_key") == "stat_value"
    
    # 4. 磁盘未命中 (因为已过期)
    time.sleep(1.5)
    # 再次清空内存，强制从磁盘检查
    cache_manager.clear_memory()
    assert cache_manager.get("stat_key") is None
    
    # 5. 驱逐
    cache_manager.set("a", 1)
    cache_manager.set("b", 2)
    cache_manager.set("c", 3)
    cache_manager.set("d", 4) # 触发一次驱逐

    stats = cache_manager.get_stats()

    assert stats['memory_misses'] > 0
    assert stats['memory_hits'] > 0
    assert stats['disk_hits'] > 0
    assert stats['disk_misses'] > 0
    assert stats['evictions'] > 0
    assert stats['total_sets'] > 0

def test_clear_cache(cache_manager):
    """测试清理缓存的功能"""
    cache_manager.set("mem_key", "mem_val")
    cache_manager.set("disk_key", "disk_val")

    # 清理所有
    cache_manager.clear()
    
    assert cache_manager.get("mem_key") is None
    assert cache_manager.get("disk_key") is None
    
    stats = cache_manager.get_stats()
    # clear() 应该也重置了统计数据
    assert stats['total_sets'] == 0 