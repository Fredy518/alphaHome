#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
智能缓存管理器 - Backtrader增强工具

提供高效的缓存管理功能，支持内存和磁盘混合缓存策略。
"""

import hashlib
import os
import pickle
import time
import json
from collections import OrderedDict
from typing import Any, Dict, List, Optional
from functools import wraps
import logging

import pandas as pd

from ...common.logging_utils import get_logger
from .exceptions import CacheOperationError


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

    def __init__(
        self,
        max_memory_items: int = 1000,
        max_memory_mb: int = 512,
        disk_cache_dir: str = "cache/backtrader_data",
        disk_cache_ttl: int = 86400 * 7,  # 7天
        enable_disk_cache: bool = True,
        log_level: int = logging.INFO,
    ):
        """
        初始化缓存管理器

        Args:
            max_memory_items: 内存缓存最大条目数
            max_memory_mb: 内存缓存最大MB数
            disk_cache_dir: 磁盘缓存目录
            disk_cache_ttl: 磁盘缓存过期时间（秒）
            enable_disk_cache: 是否启用磁盘缓存
            log_level: 日志级别
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
            "memory_hits": 0,
            "memory_misses": 0,
            "disk_hits": 0,
            "disk_misses": 0,
            "evictions": 0,
            "total_sets": 0,
        }

        self.logger = get_logger("cache_manager")
        self.log_level = log_level
        self.logger.setLevel(self.log_level)

        # 创建磁盘缓存目录
        if self.enable_disk_cache:
            os.makedirs(self.disk_cache_dir, exist_ok=True)
            self._cleanup_expired_disk_cache()

    def _get_cache_key(self, *args, **kwargs) -> str:
        """
        Generates a stable cache key from function arguments using JSON or Pickle.
        """
        try:
            # Sort kwargs to ensure key consistency
            sorted_kwargs = OrderedDict(sorted(kwargs.items()))
            payload = {'args': args, 'kwargs': sorted_kwargs}
            # Use compact, sorted JSON for a stable, readable key
            key_data = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        except TypeError:
            # Fallback to pickle for non-JSON serializable objects
            try:
                payload = {'args': args, 'kwargs': kwargs}
                key_data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
            except Exception as e:
                self.logger.error(f"Failed to serialize cache key arguments with pickle: {e}", exc_info=True)
                # As a last resort, use a potentially unstable key but log a warning
                key_data = str((args, kwargs)).encode('utf-8')
                self.logger.warning("Cache key was generated using unstable str() serialization.")

        return hashlib.md5(key_data).hexdigest()

    def memoize(self, func):
        """
        Decorator to cache the results of a function based on its arguments.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not self.enable_disk_cache and not self.max_memory_items > 0: # A more robust check
                return func(*args, **kwargs)

            key = self._get_cache_key(*args, **kwargs)
            
            # 1. Check memory cache
            if key in self._memory_cache:
                # LRU: move to end
                self._memory_cache.move_to_end(key)
                self._stats["memory_hits"] += 1
                self.logger.debug(f"Memory cache hit: {key}")
                return self._memory_cache[key]

            self._stats["memory_misses"] += 1

            # 2. Check disk cache
            if self.enable_disk_cache:
                disk_value = self._get_from_disk(key)
                if disk_value is not None:
                    # Load disk data into memory
                    self._set_memory(key, disk_value)
                    self._stats["disk_hits"] += 1
                    self.logger.debug(f"Disk cache hit: {key}")
                    return disk_value

            self._stats["disk_misses"] += 1

            # 3. Calculate result and cache it
            result = func(*args, **kwargs)
            self._set_memory(key, result)
            if self.enable_disk_cache:
                self._set_disk(key, result)

            return result

        return wrapper

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
            self._memory_cache.move_to_end(key)
            self._stats["memory_hits"] += 1
            self.logger.debug(f"内存缓存命中: {key}")
            return self._memory_cache[key]

        self._stats["memory_misses"] += 1

        # 2. 检查磁盘缓存
        if self.enable_disk_cache:
            disk_value = self._get_from_disk(key)
            if disk_value is not None:
                # 将磁盘数据加载到内存
                self._set_memory(key, disk_value)
                self._stats["disk_hits"] += 1
                self.logger.debug(f"磁盘缓存命中: {key}")
                return disk_value

        self._stats["disk_misses"] += 1
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        设置缓存数据

        Args:
            key: 缓存键
            value: 要缓存的数据
            ttl: 过期时间（秒），None表示使用默认TTL
        """
        self._stats["total_sets"] += 1

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

        # 如果键已存在，直接更新值，不改变其LRU位置
        # 只有'get'操作才应将项标记为“最近使用”
        if key in self._memory_cache:
            old_value = self._memory_cache[key]
            self._memory_size_mb -= self._estimate_size_mb(old_value)
            self._memory_cache[key] = value
            self._memory_size_mb += size_mb
            return

        # 检查是否需要清理空间
        while (
            len(self._memory_cache) >= self.max_memory_items
            or self._memory_size_mb + size_mb > self.max_memory_mb
        ):
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
            self._stats["evictions"] += 1
            self.logger.debug(f"驱逐缓存: {old_key}")

    def _estimate_size_mb(self, value: Any) -> float:
        """估算数据大小（MB）"""
        if isinstance(value, pd.DataFrame):
            return value.memory_usage(deep=True).sum() / 1024 / 1024
        else:
            # 简化估算：使用pickle后的大小
            try:
                return len(pickle.dumps(value)) / 1024 / 1024
            except pickle.PicklingError as e:
                self.logger.warning(f"Could not estimate size for value of type {type(value)}. Defaulting to 1.0 MB. Error: {e}")
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
                if (time.time() - file_time) > self.disk_cache_ttl:
                    self.logger.debug(f"磁盘缓存已过期: {key}")
                    os.remove(cache_file)
                    return None

                # 读取数据
                with open(cache_file, "rb") as f:
                    data = pickle.load(f)
                return data
        except (pickle.UnpicklingError, EOFError) as e:
            self.logger.warning(f"磁盘缓存读取或反序列化失败: {cache_file}, {e}")
            # 删除损坏的缓存文件
            try:
                os.remove(cache_file)
            except OSError as remove_error:
                self.logger.error(f"无法删除损坏的缓存文件 {cache_file}: {remove_error}")
        except Exception as e:
            self.logger.error(f"从磁盘获取缓存时发生未知错误: {cache_file}", exc_info=True)

        return None

    def _set_disk(self, key: str, value: Any, ttl: Optional[int] = None):
        """设置磁盘缓存"""
        cache_file = self._get_disk_path(key)

        try:
            with open(cache_file, "wb") as f:
                pickle.dump(value, f)
        except pickle.PicklingError as e:
            self.logger.error(f"磁盘缓存序列化失败: {cache_file}, {e}", exc_info=True)
            raise CacheOperationError(f"无法序列化值以存入磁盘缓存: {key}") from e
        except Exception as e:
            self.logger.error(f"磁盘缓存写入时发生未知错误: {cache_file}", exc_info=True)
            raise CacheOperationError(f"无法将值写入磁盘缓存: {key}") from e

    def _cleanup_expired_disk_cache(self):
        """清理过期的磁盘缓存"""
        if not os.path.exists(self.disk_cache_dir):
            return

        current_time = time.time()
        removed_count = 0

        for filename in os.listdir(self.disk_cache_dir):
            if filename.endswith(".cache"):
                filepath = os.path.join(self.disk_cache_dir, filename)
                try:
                    file_time = os.path.getmtime(filepath)
                    if current_time - file_time > self.disk_cache_ttl:
                        os.remove(filepath)
                        removed_count += 1
                except FileNotFoundError:
                    # 文件可能在列出和处理之间被删除，这不是一个错误
                    continue
                except Exception as e:
                    self.logger.warning(f"清理缓存文件失败: {filepath}, {e}")

        if removed_count > 0:
            self.logger.info(f"清理了 {removed_count} 个过期的磁盘缓存文件")

    def clear_memory(self):
        """仅清理内存缓存（主要用于测试）"""
        self._memory_cache.clear()
        self._memory_size_mb = 0
        self.logger.info("内存缓存已清理")

    def clear(self):
        """清理所有缓存（内存和磁盘）并重置统计信息"""
        self.clear_memory()

        # 清理磁盘缓存
        if self.enable_disk_cache and os.path.exists(self.disk_cache_dir):
            for filename in os.listdir(self.disk_cache_dir):
                if filename.endswith(".cache"):
                    filepath = os.path.join(self.disk_cache_dir, filename)
                    try:
                        os.remove(filepath)
                    except OSError as e:
                        self.logger.warning(f"清理磁盘缓存文件失败: {filepath}, {e}")
        
        self._reset_stats()
        self.logger.info("所有缓存已清理")

    def _reset_stats(self):
        """重置所有统计数据"""
        self._stats = {
            "memory_hits": 0,
            "memory_misses": 0,
            "disk_hits": 0,
            "disk_misses": 0,
            "evictions": 0,
            "total_sets": 0,
        }

    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息
        """
        total_requests = self._stats["memory_hits"] + self._stats["memory_misses"]
        memory_hit_rate = (
            (self._stats["memory_hits"] / total_requests * 100)
            if total_requests > 0
            else 0
        )

        total_disk_requests = self._stats["disk_hits"] + self._stats["disk_misses"]
        disk_hit_rate = (
            (self._stats["disk_hits"] / total_disk_requests * 100)
            if total_disk_requests > 0
            else 0
        )

        overall_hit_rate = (
            (
                (self._stats["memory_hits"] + self._stats["disk_hits"])
                / (total_requests)
                * 100
            )
            if total_requests > 0
            else 0
        )

        return {
            "memory_items": len(self._memory_cache),
            "memory_size_mb": self._memory_size_mb,
            "memory_hit_rate": memory_hit_rate,
            "disk_hit_rate": disk_hit_rate,
            "overall_hit_rate": overall_hit_rate,
            "evictions": self._stats["evictions"],
            "total_sets": self._stats["total_sets"],
            **self._stats,
        }

    def get_optimization_suggestions(self) -> List[str]:
        """获取缓存优化建议"""
        stats = self.get_stats()
        suggestions = []

        if stats["memory_hit_rate"] < 50:
            suggestions.append(
                "内存缓存命中率较低，考虑增加max_memory_items或max_memory_mb"
            )

        if stats["evictions"] > stats["total_sets"] * 0.5:
            suggestions.append("缓存驱逐过于频繁，建议增加内存缓存大小")

        if stats["disk_hit_rate"] < 30 and self.enable_disk_cache:
            suggestions.append("磁盘缓存效果不佳，考虑调整缓存策略或TTL")

        if stats["memory_size_mb"] > self.max_memory_mb * 0.9:
            suggestions.append("内存缓存接近上限，建议监控内存使用")

        return suggestions
