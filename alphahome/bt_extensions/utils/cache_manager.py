#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
智能缓存管理器 - Backtrader增强工具

提供高效的缓存管理功能，支持内存和磁盘混合缓存策略。
"""

import os
import pickle
import hashlib
import time
from typing import Any, Optional, Dict, List
from collections import OrderedDict
import pandas as pd
from ...common.logging_utils import get_logger


class CacheManager:
    """
    智能缓存管理器
    
    支持：
    - LRU内存缓存
    - 磁盘持久化缓存
    - 自动过期清理
    - 内存使用监控
    - 缓存统计和优化建议
    """
    
    def __init__(self, 
                 max_memory_items: int = 1000,
                 max_memory_mb: int = 512,
                 disk_cache_dir: str = "cache/backtrader_data",
                 disk_cache_ttl: int = 86400 * 7,  # 7天
                 enable_disk_cache: bool = True):
        """
        初始化缓存管理器
        
        Args:
            max_memory_items: 内存缓存最大条目数
            max_memory_mb: 内存缓存最大MB数
            disk_cache_dir: 磁盘缓存目录
            disk_cache_ttl: 磁盘缓存过期时间（秒）
            enable_disk_cache: 是否启用磁盘缓存
        """
        self.max_memory_items = max_memory_items
        self.max_memory_mb = max_memory_mb
        self.disk_cache_dir = disk_cache_dir
        self.disk_cache_ttl = disk_cache_ttl
        self.enable_disk_cache = enable_disk_cache
        
        # 内存缓存 (LRU)
        self._memory_cache: OrderedDict = OrderedDict()
        self._memory_size_mb = 0
        
        # 统计信息
        self._stats = {
            'memory_hits': 0,
            'memory_misses': 0,
            'disk_hits': 0,
            'disk_misses': 0,
            'evictions': 0,
            'total_sets': 0
        }
        
        self.logger = get_logger("cache_manager")
        
        # 创建磁盘缓存目录
        if self.enable_disk_cache:
            os.makedirs(self.disk_cache_dir, exist_ok=True)
            self._cleanup_expired_disk_cache()
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存数据
        
        Args:
            key: 缓存键
            
        Returns:
            缓存的数据，如果不存在则返回None
        """
        # 1. 检查内存缓存
        if key in self._memory_cache:
            # LRU: 移到最后
            value = self._memory_cache.pop(key)
            self._memory_cache[key] = value
            self._stats['memory_hits'] += 1
            self.logger.debug(f"内存缓存命中: {key}")
            return value
        
        self._stats['memory_misses'] += 1
        
        # 2. 检查磁盘缓存
        if self.enable_disk_cache:
            disk_value = self._get_from_disk(key)
            if disk_value is not None:
                # 将磁盘数据加载到内存
                self._set_memory(key, disk_value)
                self._stats['disk_hits'] += 1
                self.logger.debug(f"磁盘缓存命中: {key}")
                return disk_value
        
        self._stats['disk_misses'] += 1
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        设置缓存数据
        
        Args:
            key: 缓存键
            value: 要缓存的数据
            ttl: 过期时间（秒），None表示使用默认TTL
        """
        self._stats['total_sets'] += 1
        
        # 设置内存缓存
        self._set_memory(key, value)
        
        # 设置磁盘缓存
        if self.enable_disk_cache:
            self._set_disk(key, value, ttl)
        
        self.logger.debug(f"缓存设置: {key}")
    
    def _set_memory(self, key: str, value: Any):
        """设置内存缓存"""
        # 估算数据大小（简化版本）
        size_mb = self._estimate_size_mb(value)
        
        # 如果已存在，先删除旧的
        if key in self._memory_cache:
            old_value = self._memory_cache.pop(key)
            self._memory_size_mb -= self._estimate_size_mb(old_value)
        
        # 检查是否需要清理空间
        while (len(self._memory_cache) >= self.max_memory_items or 
               self._memory_size_mb + size_mb > self.max_memory_mb):
            if not self._memory_cache:
                break
            self._evict_oldest()
        
        # 添加新数据
        self._memory_cache[key] = value
        self._memory_size_mb += size_mb
    
    def _evict_oldest(self):
        """驱逐最老的缓存项"""
        if self._memory_cache:
            old_key, old_value = self._memory_cache.popitem(last=False)
            self._memory_size_mb -= self._estimate_size_mb(old_value)
            self._stats['evictions'] += 1
            self.logger.debug(f"驱逐缓存: {old_key}")
    
    def _estimate_size_mb(self, value: Any) -> float:
        """估算数据大小（MB）"""
        if isinstance(value, pd.DataFrame):
            return value.memory_usage(deep=True).sum() / 1024 / 1024
        else:
            # 简化估算：使用pickle后的大小
            try:
                return len(pickle.dumps(value)) / 1024 / 1024
            except:
                return 1.0  # 默认1MB
    
    def _get_disk_path(self, key: str) -> str:
        """获取磁盘缓存文件路径"""
        # 使用MD5哈希避免文件名过长或包含特殊字符
        hash_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.disk_cache_dir, f"{hash_key}.cache")
    
    def _get_from_disk(self, key: str) -> Optional[Any]:
        """从磁盘获取缓存数据"""
        cache_file = self._get_disk_path(key)
        
        try:
            if os.path.exists(cache_file):
                # 检查是否过期
                file_time = os.path.getmtime(cache_file)
                if time.time() - file_time > self.disk_cache_ttl:
                    os.remove(cache_file)
                    return None
                
                # 读取数据
                with open(cache_file, 'rb') as f:
                    data = pickle.load(f)
                return data
        except Exception as e:
            self.logger.warning(f"磁盘缓存读取失败: {cache_file}, {e}")
            # 删除损坏的缓存文件
            try:
                os.remove(cache_file)
            except:
                pass
        
        return None
    
    def _set_disk(self, key: str, value: Any, ttl: Optional[int] = None):
        """设置磁盘缓存"""
        cache_file = self._get_disk_path(key)
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(value, f)
        except Exception as e:
            self.logger.warning(f"磁盘缓存写入失败: {cache_file}, {e}")
    
    def _cleanup_expired_disk_cache(self):
        """清理过期的磁盘缓存"""
        if not os.path.exists(self.disk_cache_dir):
            return
        
        current_time = time.time()
        removed_count = 0
        
        for filename in os.listdir(self.disk_cache_dir):
            if filename.endswith('.cache'):
                filepath = os.path.join(self.disk_cache_dir, filename)
                try:
                    file_time = os.path.getmtime(filepath)
                    if current_time - file_time > self.disk_cache_ttl:
                        os.remove(filepath)
                        removed_count += 1
                except Exception as e:
                    self.logger.warning(f"清理缓存文件失败: {filepath}, {e}")
        
        if removed_count > 0:
            self.logger.info(f"清理了 {removed_count} 个过期的磁盘缓存文件")
    
    def clear(self):
        """清理所有缓存"""
        # 清理内存缓存
        self._memory_cache.clear()
        self._memory_size_mb = 0
        
        # 清理磁盘缓存
        if self.enable_disk_cache and os.path.exists(self.disk_cache_dir):
            for filename in os.listdir(self.disk_cache_dir):
                if filename.endswith('.cache'):
                    filepath = os.path.join(self.disk_cache_dir, filename)
                    try:
                        os.remove(filepath)
                    except Exception as e:
                        self.logger.warning(f"删除缓存文件失败: {filepath}, {e}")
        
        self.logger.info("所有缓存已清理")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_requests = (self._stats['memory_hits'] + self._stats['memory_misses'])
        memory_hit_rate = (self._stats['memory_hits'] / total_requests * 100) if total_requests > 0 else 0
        
        total_disk_requests = (self._stats['disk_hits'] + self._stats['disk_misses'])
        disk_hit_rate = (self._stats['disk_hits'] / total_disk_requests * 100) if total_disk_requests > 0 else 0
        
        overall_hit_rate = ((self._stats['memory_hits'] + self._stats['disk_hits']) / 
                           (total_requests) * 100) if total_requests > 0 else 0
        
        return {
            'memory_items': len(self._memory_cache),
            'memory_size_mb': self._memory_size_mb,
            'memory_hit_rate': memory_hit_rate,
            'disk_hit_rate': disk_hit_rate,
            'overall_hit_rate': overall_hit_rate,
            'evictions': self._stats['evictions'],
            'total_sets': self._stats['total_sets'],
            **self._stats
        }
    
    def get_optimization_suggestions(self) -> List[str]:
        """获取缓存优化建议"""
        stats = self.get_stats()
        suggestions = []
        
        if stats['memory_hit_rate'] < 50:
            suggestions.append("内存缓存命中率较低，考虑增加max_memory_items或max_memory_mb")
        
        if stats['evictions'] > stats['total_sets'] * 0.5:
            suggestions.append("缓存驱逐过于频繁，建议增加内存缓存大小")
        
        if stats['disk_hit_rate'] < 30 and self.enable_disk_cache:
            suggestions.append("磁盘缓存效果不佳，考虑调整缓存策略或TTL")
        
        if stats['memory_size_mb'] > self.max_memory_mb * 0.9:
            suggestions.append("内存缓存接近上限，建议监控内存使用")
        
        return suggestions 