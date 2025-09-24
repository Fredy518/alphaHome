#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
G因子年度并行计算启动器
自动启动多个终端窗口，每个负责不同年份的计算

使用方法：
python scripts/analysis/start_parallel_g_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10
"""

import argparse
import subprocess
import sys
import os
import time
from datetime import datetime


def smart_year_allocation(years, workers):
    """
    智能年份分配算法
    按时间顺序轮询分配，早期年份优先合并
    
    Args:
        years: 年份列表
        workers: 工作进程数
    
    Returns:
        list: 每个进程分配的年份列表
    """
    total_years = len(years)
    
    if total_years <= workers:
        # 年份数 <= 进程数，每个进程分配一个年份
        allocation = [[year] for year in years]
        # 补充空列表
        while len(allocation) < workers:
            allocation.append([])
        return allocation
    
    # 年份数 > 进程数，需要轮询分配
    allocation = [[] for _ in range(workers)]
    
    # 按年份排序（保持时间顺序）
    years_sorted = sorted(years)
    
    # 计算每个进程应该分配的年数
    base_years_per_worker = total_years // workers  # 每个进程的基础年数
    extra_years = total_years % workers  # 多出来的年数
    
    # 前extra_years个进程多分配1年
    years_per_worker = [base_years_per_worker + 1 if i < extra_years else base_years_per_worker 
                       for i in range(workers)]
    
    # 按时间顺序分配年份
    year_index = 0
    for worker_id in range(workers):
        for _ in range(years_per_worker[worker_id]):
            if year_index < len(years_sorted):
                allocation[worker_id].append(years_sorted[year_index])
                year_index += 1
    
    return allocation


def start_worker_process(worker_id: int, start_year: int, end_year: int, total_workers: int):
    """启动单个工作进程"""
    if os.name == 'nt':
        # Windows系统 - 使用更简单的命令结构
        title = f"Worker-{worker_id}"
        cmd = f'python scripts/production/factor_calculators/g_factor/g_factor_parallel_by_year.py --start_year {start_year} --end_year {end_year} --worker_id {worker_id} --total_workers {total_workers}'
        
        # 使用os.system，避免复杂的subprocess调用
        system_cmd = f'start "{title}" cmd /k "cd /d {os.getcwd()} && {cmd}"'
        os.system(system_cmd)
    else:
        # Linux/Mac系统
        cmd = [
            sys.executable,
            "scripts/production/factor_calculators/g_factor/g_factor_parallel_by_year.py",
            "--start_year", str(start_year),
            "--end_year", str(end_year),
            "--worker_id", str(worker_id),
            "--total_workers", str(total_workers)
        ]
        subprocess.Popen([
            "gnome-terminal", "--title", f"Worker-{worker_id}",
            "--", "bash", "-c", f"cd {os.getcwd()} && {' '.join(cmd)}; exec bash"
        ])


def main():
    parser = argparse.ArgumentParser(description='G因子年度并行计算启动器')
    parser.add_argument('--start_year', type=int, default=2020, help='开始年份 (默认: 2020)')
    parser.add_argument('--end_year', type=int, default=2024, help='结束年份 (默认: 2024)')
    parser.add_argument('--workers', type=int, default=10, help='工作进程数 (默认: 10)')
    parser.add_argument('--delay', type=int, default=2, help='进程启动间隔秒数 (默认: 2)')
    
    args = parser.parse_args()
    
    # 验证参数
    if args.start_year > args.end_year:
        print(f"❌ start_year ({args.start_year}) 必须小于等于 end_year ({args.end_year})")
        sys.exit(1)
    
    if args.workers <= 0:
        print(f"❌ workers ({args.workers}) 必须大于0")
        sys.exit(1)
    
    # 计算年份数量
    total_years = args.end_year - args.start_year + 1
    
    # 智能调整工作进程数
    if args.workers > total_years:
        print(f"⚠️ 工作进程数 ({args.workers}) 大于年份数 ({total_years})")
        print(f"🔧 自动调整工作进程数为: {total_years}")
        args.workers = total_years
    
    print("🚀 G因子年度并行计算启动器")
    print("=" * 50)
    print(f"📅 计算年份范围: {args.start_year}-{args.end_year}")
    print(f"👥 工作进程数: {args.workers}")
    print(f"⏱️ 启动间隔: {args.delay}秒")
    print(f"🕐 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 计算年份分配 - 智能分配算法
    years = list(range(args.start_year, args.end_year + 1))
    total_years = len(years)
    
    # 智能分配：早期年份合并，平衡计算量
    worker_years_list = smart_year_allocation(years, args.workers)
    
    print(f"📊 年份分配 (智能分配):")
    for worker_id, worker_years in enumerate(worker_years_list):
        print(f"   进程{worker_id}: {worker_years}")
    print()
    
    # 启动所有工作进程
    print("🚀 启动工作进程...")
    for worker_id in range(args.workers):
        if worker_years_list[worker_id]:  # 只启动有分配年份的进程
            print(f"   启动进程 {worker_id}...")
            start_worker_process(worker_id, args.start_year, args.end_year, args.workers)
            
            if worker_id < args.workers - 1:  # 最后一个进程不需要等待
                time.sleep(args.delay)
        else:
            print(f"   跳过进程 {worker_id} (无分配年份)")
    
    print()
    print("✅ 所有工作进程已启动!")
    print()
    print("📊 监控说明:")
    print("   - 每个终端窗口显示一个工作进程的进度")
    print("   - 可以随时关闭单个终端窗口来停止对应进程")
    print("   - 所有进程完成后，G因子数据将保存到数据库")
    print()
    print("💡 性能预期:")
    estimated_time_per_year = 2  # 假设每年需要2小时
    total_estimated_time = total_years * estimated_time_per_year / args.workers
    print(f"   - 预计总耗时: {total_estimated_time:.1f}小时 (并行)")
    print(f"   - 串行耗时: {total_years * estimated_time_per_year:.1f}小时")
    print(f"   - 理论加速比: {args.workers}x")
    print(f"   - 年份分配: 每个进程负责 {total_years/args.workers:.1f} 年")
    print()
    print("🎯 建议:")
    print("   - 监控数据库连接数，避免连接池耗尽")
    print("   - 定期检查磁盘空间，确保有足够存储空间")
    print("   - 可以随时调整工作进程数来平衡负载")


if __name__ == "__main__":
    main()
