#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
统计整个项目中所有Python文件的行数

功能:
- 递归扫描项目目录中的所有.py文件
- 统计每个文件的总行数（包括空行和注释行）
- 显示每个文件的路径和行数
- 计算所有Python文件的总行数
- 提供详细的统计报告

使用方法:
1. 在项目根目录运行: python scripts/count_python_lines.py
2. 查看输出的统计报告
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class PythonLinesCounter:
    """Python文件行数统计器"""
    
    def __init__(self, root_path: str = "."):
        """
        初始化统计器
        
        Args:
            root_path: 项目根目录路径，默认为当前目录
        """
        self.root_path = Path(root_path)
        self.python_files: List[Tuple[Path, int]] = []
        self.total_lines = 0
        
    def find_python_files(self) -> List[Path]:
        """查找所有.py文件"""
        python_files = []
        
        # 定义要排除的目录
        exclude_dirs = {
            '.git', '.idea', '__pycache__', '.pytest_cache', 
            '.roo', '.cunzhi-memory', 'alphaHome.egg-info',
            'node_modules', '.venv', 'venv', '.env'
        }
        
        for root, dirs, files in os.walk(self.root_path):
            # 排除不需要的目录
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                if file.endswith('.py'):
                    file_path = Path(root) / file
                    python_files.append(file_path)
        
        return sorted(python_files)
    
    def count_file_lines(self, file_path: Path) -> int:
        """统计单个文件的行数"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return sum(1 for _ in f)
        except Exception as e:
            logger.warning(f"无法读取文件 {file_path}: {e}")
            return 0
    
    def count_all_lines(self) -> None:
        """统计所有Python文件的行数"""
        logger.info("正在扫描Python文件...")
        python_files = self.find_python_files()
        
        if not python_files:
            logger.info("未找到任何Python文件")
            return
        
        logger.info(f"找到 {len(python_files)} 个Python文件")
        logger.info("-" * 80)
        
        # 统计每个文件的行数
        for file_path in python_files:
            lines = self.count_file_lines(file_path)
            relative_path = file_path.relative_to(self.root_path)
            self.python_files.append((relative_path, lines))
            self.total_lines += lines
            
            # 显示每个文件的信息
            logger.info(f"{str(relative_path):<60} {lines:>6} 行")
    
    def print_summary(self) -> None:
        """打印统计摘要"""
        logger.info("-" * 80)
        logger.info(f"总计: {len(self.python_files)} 个Python文件")
        logger.info(f"总行数: {self.total_lines:,} 行")
        
        if self.python_files:
            # 找出最大和最小的文件
            max_file = max(self.python_files, key=lambda x: x[1])
            min_file = min(self.python_files, key=lambda x: x[1])
            
            logger.info(f"最大文件: {max_file[0]} ({max_file[1]} 行)")
            logger.info(f"最小文件: {min_file[0]} ({min_file[1]} 行)")
            
            # 计算平均行数
            avg_lines = self.total_lines / len(self.python_files)
            logger.info(f"平均行数: {avg_lines:.1f} 行/文件")
    
    def save_report(self, output_file: str = "python_lines_report.txt") -> None:
        """保存详细报告到文件"""
        report_path = self.root_path / output_file
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("Python文件行数统计报告\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"项目根目录: {self.root_path.absolute()}\n")
            f.write(f"统计时间: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("文件列表:\n")
            f.write("-" * 80 + "\n")
            
            for file_path, lines in self.python_files:
                f.write(f"{str(file_path):<60} {lines:>6} 行\n")
            
            f.write("-" * 80 + "\n")
            f.write(f"总计: {len(self.python_files)} 个Python文件\n")
            f.write(f"总行数: {self.total_lines:,} 行\n")
            
            if self.python_files:
                max_file = max(self.python_files, key=lambda x: x[1])
                min_file = min(self.python_files, key=lambda x: x[1])
                avg_lines = self.total_lines / len(self.python_files)
                
                f.write(f"最大文件: {max_file[0]} ({max_file[1]} 行)\n")
                f.write(f"最小文件: {min_file[0]} ({min_file[1]} 行)\n")
                f.write(f"平均行数: {avg_lines:.1f} 行/文件\n")
        
        logger.info(f"详细报告已保存到: {report_path}")


def main():
    """主函数"""
    # 获取项目根目录（脚本所在目录的上一级）
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    print("Python文件行数统计工具")
    print("=" * 50)
    print(f"项目根目录: {project_root}")
    print()
    
    counter = PythonLinesCounter(project_root)
    counter.count_all_lines()
    counter.print_summary()
    
    # 询问是否保存报告
    save_report = input("\n是否保存详细报告到文件？(y/n): ").strip().lower()
    if save_report == 'y':
        counter.save_report()
    
    print("\n统计完成！")


if __name__ == "__main__":
    main() 