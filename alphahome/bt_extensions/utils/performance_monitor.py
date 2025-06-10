#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
性能监控器 - Backtrader增强工具

提供实时性能监控功能，帮助优化回测性能。
"""

import time
import psutil
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime
from ...common.logging_utils import get_logger


class PerformanceMonitor:
    """
    性能监控器
    
    监控：
    - CPU使用率
    - 内存使用情况
    - 执行时间
    - 磁盘I/O
    - 网络I/O
    """
    
    def __init__(self, monitor_interval: float = 1.0):
        """
        初始化性能监控器
        
        Args:
            monitor_interval: 监控间隔（秒）
        """
        self.monitor_interval = monitor_interval
        self.logger = get_logger("performance_monitor")
        
        # 监控状态
        self._monitoring = False
        self._monitor_thread = None
        
        # 监控数据
        self._start_time = None
        self._end_time = None
        self._cpu_samples = []
        self._memory_samples = []
        self._io_samples = []
        
        # 进程信息
        self._process = psutil.Process()
        self._initial_memory = None
        self._initial_io = None
    
    def start_monitoring(self):
        """开始性能监控"""
        if self._monitoring:
            self.logger.warning("性能监控已经在运行")
            return
        
        self.logger.info("开始性能监控")
        
        # 重置数据
        self._start_time = time.time()
        self._end_time = None
        self._cpu_samples.clear()
        self._memory_samples.clear()
        self._io_samples.clear()
        
        # 记录初始状态
        self._initial_memory = self._process.memory_info()
        try:
            self._initial_io = self._process.io_counters()
        except (psutil.AccessDenied, AttributeError):
            self._initial_io = None
        
        # 启动监控线程
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
    
    def stop_monitoring(self) -> Dict[str, Any]:
        """
        停止性能监控并返回统计结果
        
        Returns:
            性能统计数据
        """
        if not self._monitoring:
            self.logger.warning("性能监控未在运行")
            return {}
        
        self.logger.info("停止性能监控")
        
        self._monitoring = False
        self._end_time = time.time()
        
        # 等待监控线程结束
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
        
        # 生成统计报告
        stats = self._generate_stats()
        self.logger.info(f"监控完成，持续时间: {stats['duration']:.2f}秒")
        
        return stats
    
    def _monitor_loop(self):
        """监控循环"""
        while self._monitoring:
            try:
                # 收集CPU使用率
                cpu_percent = self._process.cpu_percent()
                self._cpu_samples.append({
                    'timestamp': time.time(),
                    'cpu_percent': cpu_percent
                })
                
                # 收集内存使用情况
                memory_info = self._process.memory_info()
                memory_percent = self._process.memory_percent()
                self._memory_samples.append({
                    'timestamp': time.time(),
                    'rss': memory_info.rss,  # 物理内存
                    'vms': memory_info.vms,  # 虚拟内存
                    'percent': memory_percent
                })
                
                # 收集I/O信息
                try:
                    io_counters = self._process.io_counters()
                    self._io_samples.append({
                        'timestamp': time.time(),
                        'read_bytes': io_counters.read_bytes,
                        'write_bytes': io_counters.write_bytes,
                        'read_count': io_counters.read_count,
                        'write_count': io_counters.write_count
                    })
                except (psutil.AccessDenied, AttributeError):
                    pass
                
                time.sleep(self.monitor_interval)
                
            except Exception as e:
                self.logger.error(f"监控过程中出错: {e}")
                time.sleep(self.monitor_interval)
    
    def _generate_stats(self) -> Dict[str, Any]:
        """生成性能统计报告"""
        if not self._start_time or not self._end_time:
            return {}
        
        duration = self._end_time - self._start_time
        
        stats = {
            'duration': duration,
            'start_time': datetime.fromtimestamp(self._start_time).isoformat(),
            'end_time': datetime.fromtimestamp(self._end_time).isoformat(),
            'samples_count': len(self._cpu_samples)
        }
        
        # CPU统计
        if self._cpu_samples:
            cpu_values = [sample['cpu_percent'] for sample in self._cpu_samples]
            stats['cpu'] = {
                'avg_percent': sum(cpu_values) / len(cpu_values),
                'max_percent': max(cpu_values),
                'min_percent': min(cpu_values)
            }
        
        # 内存统计
        if self._memory_samples:
            rss_values = [sample['rss'] for sample in self._memory_samples]
            vms_values = [sample['vms'] for sample in self._memory_samples]
            percent_values = [sample['percent'] for sample in self._memory_samples]
            
            stats['memory'] = {
                'initial_rss_mb': self._initial_memory.rss / 1024 / 1024 if self._initial_memory else 0,
                'final_rss_mb': rss_values[-1] / 1024 / 1024,
                'peak_rss_mb': max(rss_values) / 1024 / 1024,
                'avg_rss_mb': sum(rss_values) / len(rss_values) / 1024 / 1024,
                'avg_percent': sum(percent_values) / len(percent_values),
                'max_percent': max(percent_values),
                'memory_growth_mb': (rss_values[-1] - rss_values[0]) / 1024 / 1024 if len(rss_values) > 1 else 0
            }
        
        # I/O统计
        if self._io_samples and self._initial_io:
            final_io = self._io_samples[-1]
            
            total_read_bytes = final_io['read_bytes'] - self._initial_io.read_bytes
            total_write_bytes = final_io['write_bytes'] - self._initial_io.write_bytes
            total_read_count = final_io['read_count'] - self._initial_io.read_count
            total_write_count = final_io['write_count'] - self._initial_io.write_count
            
            stats['io'] = {
                'total_read_mb': total_read_bytes / 1024 / 1024,
                'total_write_mb': total_write_bytes / 1024 / 1024,
                'total_read_count': total_read_count,
                'total_write_count': total_write_count,
                'avg_read_speed_mbps': (total_read_bytes / 1024 / 1024) / duration if duration > 0 else 0,
                'avg_write_speed_mbps': (total_write_bytes / 1024 / 1024) / duration if duration > 0 else 0
            }
        
        return stats
    
    def get_current_stats(self) -> Dict[str, Any]:
        """获取当前瞬时性能数据"""
        try:
            current_memory = self._process.memory_info()
            
            stats = {
                'timestamp': time.time(),
                'cpu_percent': self._process.cpu_percent(),
                'memory_rss_mb': current_memory.rss / 1024 / 1024,
                'memory_vms_mb': current_memory.vms / 1024 / 1024,
                'memory_percent': self._process.memory_percent(),
                'num_threads': self._process.num_threads()
            }
            
            try:
                io_counters = self._process.io_counters()
                stats.update({
                    'io_read_mb': io_counters.read_bytes / 1024 / 1024,
                    'io_write_mb': io_counters.write_bytes / 1024 / 1024,
                    'io_read_count': io_counters.read_count,
                    'io_write_count': io_counters.write_count
                })
            except (psutil.AccessDenied, AttributeError):
                pass
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取当前性能数据失败: {e}")
            return {}
    
    def print_stats(self, stats: Optional[Dict[str, Any]] = None):
        """打印性能统计报告"""
        if stats is None:
            stats = self.get_current_stats()
        
        if not stats:
            self.logger.warning("无性能数据可显示")
            return
        
        print("\n" + "="*50)
        print("         性能监控报告")
        print("="*50)
        
        if 'duration' in stats:
            print(f"执行时间: {stats['duration']:.2f} 秒")
        
        if 'cpu' in stats:
            cpu = stats['cpu']
            print(f"\nCPU使用率:")
            print(f"  平均: {cpu['avg_percent']:.1f}%")
            print(f"  最大: {cpu['max_percent']:.1f}%")
        
        if 'memory' in stats:
            memory = stats['memory']
            print(f"\n内存使用:")
            print(f"  初始: {memory['initial_rss_mb']:.1f} MB")
            print(f"  最终: {memory['final_rss_mb']:.1f} MB")
            print(f"  峰值: {memory['peak_rss_mb']:.1f} MB")
            print(f"  增长: {memory['memory_growth_mb']:.1f} MB")
            print(f"  平均占用: {memory['avg_percent']:.1f}%")
        
        if 'io' in stats:
            io = stats['io']
            print(f"\n磁盘I/O:")
            print(f"  读取: {io['total_read_mb']:.1f} MB ({io['total_read_count']} 次)")
            print(f"  写入: {io['total_write_mb']:.1f} MB ({io['total_write_count']} 次)")
            print(f"  读取速度: {io['avg_read_speed_mbps']:.1f} MB/s")
            print(f"  写入速度: {io['avg_write_speed_mbps']:.1f} MB/s")
        
        # 单个数据点显示
        if 'cpu_percent' in stats:
            print(f"\n当前状态:")
            print(f"  CPU: {stats['cpu_percent']:.1f}%")
            print(f"  内存: {stats['memory_rss_mb']:.1f} MB ({stats['memory_percent']:.1f}%)")
            print(f"  线程数: {stats.get('num_threads', 'N/A')}")
        
        print("="*50)
    
    def get_performance_recommendations(self, stats: Optional[Dict[str, Any]] = None) -> List[str]:
        """获取性能优化建议"""
        if stats is None:
            if not self._memory_samples:
                return ["无性能数据可分析"]
            stats = self._generate_stats()
        
        recommendations = []
        
        # CPU使用率建议
        if 'cpu' in stats:
            avg_cpu = stats['cpu']['avg_percent']
            max_cpu = stats['cpu']['max_percent']
            
            if avg_cpu > 80:
                recommendations.append("CPU使用率过高，考虑减少并行进程数或优化算法")
            elif avg_cpu < 30:
                recommendations.append("CPU使用率较低，可以增加并行进程数以提高性能")
            
            if max_cpu > 95:
                recommendations.append("CPU达到瓶颈，建议检查是否有死循环或低效代码")
        
        # 内存使用建议
        if 'memory' in stats:
            memory_growth = stats['memory']['memory_growth_mb']
            peak_memory = stats['memory']['peak_rss_mb']
            avg_percent = stats['memory']['avg_percent']
            
            if memory_growth > 1000:  # 增长超过1GB
                recommendations.append("内存增长过快，可能存在内存泄漏")
            
            if peak_memory > 2000:  # 峰值超过2GB
                recommendations.append("内存使用过高，考虑增加缓存清理或减少批次大小")
            
            if avg_percent > 80:
                recommendations.append("系统内存使用率过高，建议增加物理内存或优化数据结构")
        
        # I/O性能建议
        if 'io' in stats:
            read_speed = stats['io']['avg_read_speed_mbps']
            write_speed = stats['io']['avg_write_speed_mbps']
            
            if read_speed > 100:
                recommendations.append("磁盘读取频繁，考虑使用SSD或增加内存缓存")
            
            if write_speed > 50:
                recommendations.append("磁盘写入频繁，考虑减少日志输出或使用更快的存储")
        
        if not recommendations:
            recommendations.append("性能表现良好，无明显瓶颈")
        
        return recommendations 