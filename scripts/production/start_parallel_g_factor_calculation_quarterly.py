#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子季度并行计算启动器
自动启动多个终端窗口，每个负责不同季度的计算

使用方法：
python scripts/production/start_parallel_g_factor_calculation_quarterly.py --start_year 2020 --end_year 2024 --workers 16
"""

import argparse
import subprocess
import sys
import os
import time
from datetime import datetime
from typing import List, Tuple


def generate_quarters(start_year: int, end_year: int) -> List[Tuple[int, int]]:
    """
    生成指定年份范围内的所有季度
    
    Args:
        start_year: 开始年份
        end_year: 结束年份
    
    Returns:
        list: 季度列表，每个元素为(year, quarter)
    """
    quarters = []
    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):  # 1-4季度
            quarters.append((year, quarter))
    return quarters


def smart_quarter_allocation(quarters: List[Tuple[int, int]], workers: int) -> List[List[Tuple[int, int]]]:
    """
    智能季度分配算法
    按时间顺序轮询分配，平衡各进程的计算量
    
    Args:
        quarters: 季度列表 [(year, quarter), ...]
        workers: 工作进程数
    
    Returns:
        list: 每个进程分配的季度列表
    """
    total_quarters = len(quarters)
    
    if total_quarters <= workers:
        # 季度数 <= 进程数，每个进程分配一个季度
        allocation = [[quarter] for quarter in quarters]
        # 补充空列表
        while len(allocation) < workers:
            allocation.append([])
        return allocation
    
    # 季度数 > 进程数，需要轮询分配
    allocation = [[] for _ in range(workers)]
    
    # 按时间顺序排序（保持时间顺序）
    quarters_sorted = sorted(quarters, key=lambda x: (x[0], x[1]))
    
    # 计算每个进程应该分配的季度数
    base_quarters_per_worker = total_quarters // workers  # 每个进程的基础季度数
    extra_quarters = total_quarters % workers  # 多出来的季度数
    
    # 前extra_quarters个进程多分配1个季度
    quarters_per_worker = [base_quarters_per_worker + 1 if i < extra_quarters else base_quarters_per_worker 
                          for i in range(workers)]
    
    # 按时间顺序分配季度
    quarter_index = 0
    for worker_id in range(workers):
        for _ in range(quarters_per_worker[worker_id]):
            if quarter_index < len(quarters_sorted):
                allocation[worker_id].append(quarters_sorted[quarter_index])
                quarter_index += 1
    
    return allocation


def get_quarter_date_range(year: int, quarter: int) -> Tuple[str, str]:
    """
    获取指定季度的日期范围
    
    Args:
        year: 年份
        quarter: 季度 (1-4)
    
    Returns:
        tuple: (开始日期, 结束日期)
    """
    if quarter == 1:
        start_date = f"{year}-01-01"
        end_date = f"{year}-03-31"
    elif quarter == 2:
        start_date = f"{year}-04-01"
        end_date = f"{year}-06-30"
    elif quarter == 3:
        start_date = f"{year}-07-01"
        end_date = f"{year}-09-30"
    elif quarter == 4:
        start_date = f"{year}-10-01"
        end_date = f"{year}-12-31"
    else:
        raise ValueError(f"无效的季度: {quarter}")
    
    return start_date, end_date


def start_worker_process(worker_id: int, quarters: List[Tuple[int, int]], total_workers: int):
    """启动单个工作进程"""
    if os.name == 'nt':
        # Windows系统 - 使用更简单的命令结构
        title = f"G-Factor-Q-Worker-{worker_id}"
        
        # 构建季度参数
        quarter_args = []
        for year, quarter in quarters:
            quarter_args.extend(["--quarter", f"{year}Q{quarter}"])
        
        cmd = f'python scripts/production/g_factor_parallel_by_quarter.py --worker_id {worker_id} --total_workers {total_workers} {" ".join(quarter_args)}'
        
        # 使用os.system，避免复杂的subprocess调用
        system_cmd = f'start "{title}" cmd /k "cd /d {os.getcwd()} && {cmd}"'
        os.system(system_cmd)
    else:
        # Linux/Mac系统
        quarter_args = []
        for year, quarter in quarters:
            quarter_args.extend(["--quarter", f"{year}Q{quarter}"])
        
        cmd = [
            sys.executable,
            "scripts/production/g_factor_parallel_by_quarter.py",
            "--worker_id", str(worker_id),
            "--total_workers", str(total_workers)
        ] + quarter_args
        
        subprocess.Popen([
            "gnome-terminal", "--title", f"G-Factor-Q-Worker-{worker_id}",
            "--", "bash", "-c", f"cd {os.getcwd()} && {' '.join(cmd)}; exec bash"
        ])


def main():
    parser = argparse.ArgumentParser(description='G因子季度并行计算启动器')
    parser.add_argument('--start_year', type=int, default=2020, help='开始年份 (默认: 2020)')
    parser.add_argument('--end_year', type=int, default=2024, help='结束年份 (默认: 2024)')
    parser.add_argument('--workers', type=int, default=16, help='工作进程数 (默认: 16)')
    parser.add_argument('--delay', type=int, default=2, help='进程启动间隔秒数 (默认: 2)')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.start_year > args.end_year:
        print(f"❌ start_year ({args.start_year}) 必须小于等于 end_year ({args.end_year})")
        sys.exit(1)
    
    if args.workers <= 0:
        print(f"❌ workers ({args.workers}) 必须大于0")
        sys.exit(1)
    
    # 生成季度列表
    quarters = generate_quarters(args.start_year, args.end_year)
    total_quarters = len(quarters)
    
    # 智能调整工作进程数
    if args.workers > total_quarters:
        print(f"⚠️ 工作进程数 ({args.workers}) 大于季度数 ({total_quarters})")
        print(f"🔧 自动调整工作进程数为: {total_quarters}")
        args.workers = total_quarters
    
    print("🚀 G因子季度并行计算启动器")
    print("=" * 50)
    print(f"📅 计算年份范围: {args.start_year}-{args.end_year}")
    print(f"📊 总季度数: {total_quarters}")
    print(f"👥 工作进程数: {args.workers}")
    print(f"⏱️ 启动间隔: {args.delay}秒")
    print(f"🕐 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 计算季度分配 - 智能分配算法
    worker_quarters_list = smart_quarter_allocation(quarters, args.workers)
    
    print(f"📊 季度分配 (智能分配):")
    for worker_id, worker_quarters in enumerate(worker_quarters_list):
        if worker_quarters:
            quarter_strs = [f"{year}Q{q}" for year, q in worker_quarters]
            print(f"   进程{worker_id}: {quarter_strs}")
        else:
            print(f"   进程{worker_id}: []")
    print()
    
    # 启动所有工作进程
    print("🚀 启动工作进程...")
    for worker_id in range(args.workers):
        if worker_quarters_list[worker_id]:  # 只启动有分配季度的进程
            print(f"   启动进程 {worker_id}...")
            start_worker_process(worker_id, worker_quarters_list[worker_id], args.workers)
            
            if worker_id < args.workers - 1:  # 最后一个进程不需要等待
                time.sleep(args.delay)
        else:
            print(f"   跳过进程 {worker_id} (无分配季度)")
    
    print()
    print("✅ 所有工作进程已启动!")
    print()
    print("📊 监控说明:")
    print("   - 每个终端窗口显示一个工作进程的进度")
    print("   - 可以随时关闭单个终端窗口来停止对应进程")
    print("   - 所有进程完成后，G因子数据将保存到数据库")
    print()
    print("💡 性能预期:")
    estimated_time_per_quarter = 0.5  # 假设每季度需要30分钟
    total_estimated_time = total_quarters * estimated_time_per_quarter / args.workers
    print(f"   - 预计总耗时: {total_estimated_time:.1f}小时 (并行)")
    print(f"   - 串行耗时: {total_quarters * estimated_time_per_quarter:.1f}小时")
    print(f"   - 理论加速比: {args.workers}x")
    print(f"   - 季度分配: 每个进程负责 {total_quarters/args.workers:.1f} 个季度")
    print()
    print("🎯 建议:")
    print("   - 监控数据库连接数，避免连接池耗尽")
    print("   - 定期检查磁盘空间，确保有足够存储空间")
    print("   - 可以随时调整工作进程数来平衡负载")
    print("   - 季度并行比年度并行粒度更细，适合大规模计算")


if __name__ == "__main__":
    main()
