#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库表质量检查工具

对数据库中的表进行全面的质量检查，并生成报告。
可以检查所有表或指定表。
"""

import os
import sys
import asyncio
import argparse
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict, Any, Optional, Union
import json
import dotenv

# 将项目根目录添加到 Python 路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

# 配置基础日志
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_filename = f"db_quality_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_filepath = os.path.join(log_dir, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filepath, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # 同时输出到控制台
    ]
)

logger = logging.getLogger('db_quality_check')

# 导入数据模块
from data_module import DataQualityChecker, TaskFactory, DBManager


class DBQualityChecker:
    """数据库质量检查工具
    
    对数据库中的表进行全面质量检查，生成报告和图表。
    """
    
    def __init__(self, output_dir: str = None):
        """初始化数据库质量检查工具
        
        Args:
            output_dir: 输出目录，默认为logs/db_quality_{日期}
        """
        self.logger = logging.getLogger('db_quality_check')
        
        # 设置输出目录
        if output_dir is None:
            current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = os.path.join(log_dir, f'db_quality_{current_time}')
        
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 创建图表目录
        self.charts_dir = os.path.join(self.output_dir, 'charts')
        os.makedirs(self.charts_dir, exist_ok=True)
        
        # 初始化质量检查器
        self.checker = DataQualityChecker()
        
        # 结果汇总
        self.results = []
        self.summary = {}
    
    async def get_all_tables(self) -> List[str]:
        """获取数据库中所有表名
        
        Returns:
            List[str]: 表名列表
        """
        db_manager = TaskFactory.get_db_manager()
        
        # 查询所有用户表（非系统表）
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema='public' 
          AND table_type='BASE TABLE'
        ORDER BY table_name;
        """
        
        result = await db_manager.fetch(query)
        tables = [row[0] for row in result]
        
        self.logger.info(f"数据库中共找到 {len(tables)} 个表")
        return tables
    
    async def check_table(self, table_name: str, start_date: str = None, end_date: str = None) -> Dict:
        """检查单个表的质量
        
        Args:
            table_name: 表名
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
            
        Returns:
            Dict: 检查结果
        """
        self.logger.info(f"开始检查表: {table_name}")
        
        try:
            # 运行质量检查
            result = await self.checker.run_check(table_name, start_date, end_date)
            
            # 如果生成了图表，移动到指定目录
            if 'charts' in result:
                for chart_type, chart_path in result['charts'].items():
                    # 复制图表到输出目录
                    import shutil
                    new_path = os.path.join(self.charts_dir, os.path.basename(chart_path))
                    shutil.copy2(chart_path, new_path)
                    result['charts'][chart_type] = new_path
            
            # 添加到结果列表
            self.results.append(result)
            self.logger.info(f"表 {table_name} 检查完成")
            
            return result
        
        except Exception as e:
            self.logger.error(f"检查表 {table_name} 时发生错误: {str(e)}", exc_info=True)
            error_result = {
                "table_name": table_name,
                "status": "error",
                "error_message": str(e)
            }
            self.results.append(error_result)
            return error_result
    
    async def check_tables(self, tables: List[str] = None, start_date: str = None, end_date: str = None) -> List[Dict]:
        """检查多个表的质量
        
        Args:
            tables: 要检查的表名列表，如果为None则检查所有表
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
            
        Returns:
            List[Dict]: 所有表的检查结果
        """
        # 如果未指定表，获取所有表
        if tables is None:
            tables = await self.get_all_tables()
        
        self.logger.info(f"将检查 {len(tables)} 个表")
        
        # 清空之前的结果
        self.results = []
        
        # 依次检查每个表
        for table_name in tables:
            await self.check_table(table_name, start_date, end_date)
        
        # 生成总结
        self.generate_summary()
        
        return self.results
    
    def generate_summary(self):
        """生成检查结果总结"""
        total_tables = len(self.results)
        error_tables = sum(1 for r in self.results if r.get('status') == 'error')
        success_tables = total_tables - error_tables
        
        tables_with_issues = []
        
        # 分析每个表的结果
        for result in self.results:
            if result.get('status') == 'error':
                tables_with_issues.append({
                    'table_name': result.get('table_name'),
                    'issue': 'error',
                    'message': result.get('error_message')
                })
                continue
                
            if 'summary' in result:
                summary = result['summary']
                
                # 检查是否有缺失数据问题
                if summary.get('top_null_columns'):
                    for col in summary['top_null_columns']:
                        if col['null_ratio'] > 0.1:  # 空值比例超过10%
                            tables_with_issues.append({
                                'table_name': result['table_name'],
                                'issue': 'high_null_ratio',
                                'column': col['column'],
                                'null_ratio': f"{col['null_ratio']:.2%}"
                            })
                
                # 检查日期覆盖率问题
                if 'date_coverage_ratio' in summary and summary['date_coverage_ratio'] < 0.9:
                    tables_with_issues.append({
                        'table_name': result['table_name'],
                        'issue': 'low_date_coverage',
                        'coverage_ratio': f"{summary['date_coverage_ratio']:.2%}"
                    })
        
        # 保存总结信息
        self.summary = {
            'total_tables': total_tables,
            'success_tables': success_tables,
            'error_tables': error_tables,
            'tables_with_issues': tables_with_issues
        }
    
    def save_results(self):
        """保存检查结果到文件"""
        # 保存详细结果
        results_path = os.path.join(self.output_dir, 'detailed_results.json')
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        # 保存总结
        summary_path = os.path.join(self.output_dir, 'summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(self.summary, f, ensure_ascii=False, indent=2)
        
        # 生成HTML报告
        self._generate_html_report()
        
        self.logger.info(f"检查结果保存到目录: {self.output_dir}")
        return {
            'results_path': results_path,
            'summary_path': summary_path,
            'report_path': os.path.join(self.output_dir, 'report.html')
        }
    
    def _generate_html_report(self):
        """生成HTML格式的报告"""
        # 创建一个简单的HTML报告
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>数据库质量检查报告</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2, h3 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ padding: 8px; text-align: left; border: 1px solid #ddd; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .issue {{ color: #e74c3c; }}
                .good {{ color: #2ecc71; }}
                .warning {{ color: #f39c12; }}
                .summary {{ background-color: #eef; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            </style>
        </head>
        <body>
            <h1>数据库质量检查报告</h1>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="summary">
                <h2>总结</h2>
                <p>检查表总数: <strong>{self.summary['total_tables']}</strong></p>
                <p>成功检查表数: <strong class="good">{self.summary['success_tables']}</strong></p>
                <p>检查出错表数: <strong class="issue">{self.summary['error_tables']}</strong></p>
                <p>发现问题表数: <strong class="warning">{len(self.summary['tables_with_issues'])}</strong></p>
            </div>
        """
        
        # 添加问题表部分
        if self.summary['tables_with_issues']:
            html_content += """
            <h2>发现问题的表</h2>
            <table>
                <tr>
                    <th>表名</th>
                    <th>问题类型</th>
                    <th>详细信息</th>
                </tr>
            """
            
            for issue in self.summary['tables_with_issues']:
                issue_type = issue['issue']
                if issue_type == 'error':
                    detail = issue.get('message', '')
                elif issue_type == 'high_null_ratio':
                    detail = f"列 {issue['column']} 有 {issue['null_ratio']} 的空值"
                elif issue_type == 'low_date_coverage':
                    detail = f"日期覆盖率只有 {issue['coverage_ratio']}"
                else:
                    detail = ''
                    
                html_content += f"""
                <tr>
                    <td>{issue['table_name']}</td>
                    <td>{issue_type}</td>
                    <td>{detail}</td>
                </tr>
                """
            
            html_content += "</table>"
        
        # 添加每个表的详细结果
        html_content += "<h2>各表详细检查结果</h2>"
        
        for result in self.results:
            table_name = result.get('table_name', '未知表')
            
            html_content += f"<h3>表: {table_name}</h3>"
            
            if result.get('status') == 'error':
                html_content += f"<p class='issue'>错误: {result.get('error_message', '未知错误')}</p>"
                continue
            
            if 'summary' in result:
                summary = result['summary']
                html_content += "<table>"
                
                # 记录数
                html_content += f"<tr><td>总记录数</td><td>{summary.get('total_records', 0)}</td></tr>"
                
                # 日期覆盖率
                if 'date_coverage_ratio' in summary:
                    coverage_cls = 'good' if summary['date_coverage_ratio'] >= 0.9 else 'warning'
                    html_content += f"<tr><td>日期覆盖率</td><td class='{coverage_cls}'>{summary['date_coverage_ratio']:.2%}</td></tr>"
                    html_content += f"<tr><td>总日期数</td><td>{summary.get('total_dates', 0)}</td></tr>"
                    html_content += f"<tr><td>空日期数</td><td>{summary.get('empty_dates', 0)}</td></tr>"
                
                # 有空值的列数
                if 'columns_with_nulls' in summary:
                    html_content += f"<tr><td>有空值的列数</td><td>{summary['columns_with_nulls']}</td></tr>"
                
                html_content += "</table>"
                
                # 添加空值最多的列
                if summary.get('top_null_columns'):
                    html_content += """
                    <h4>空值最多的列</h4>
                    <table>
                        <tr>
                            <th>列名</th>
                            <th>空值比例</th>
                        </tr>
                    """
                    
                    for col in summary['top_null_columns']:
                        null_cls = 'good' if col['null_ratio'] < 0.05 else 'warning' if col['null_ratio'] < 0.2 else 'issue'
                        html_content += f"""
                        <tr>
                            <td>{col['column']}</td>
                            <td class='{null_cls}'>{col['null_ratio']:.2%}</td>
                        </tr>
                        """
                    
                    html_content += "</table>"
                
                # 添加覆盖率最差的月份
                if summary.get('worst_months'):
                    html_content += """
                    <h4>覆盖率最差的月份</h4>
                    <table>
                        <tr>
                            <th>月份</th>
                            <th>覆盖率</th>
                            <th>总天数</th>
                            <th>有数据天数</th>
                        </tr>
                    """
                    
                    for month in summary['worst_months']:
                        coverage_cls = 'good' if month['coverage_ratio'] >= 0.9 else 'warning' if month['coverage_ratio'] >= 0.7 else 'issue'
                        html_content += f"""
                        <tr>
                            <td>{month['month']}</td>
                            <td class='{coverage_cls}'>{month['coverage_ratio']:.2%}</td>
                            <td>{month['total_days']}</td>
                            <td>{month['covered_days']}</td>
                        </tr>
                        """
                    
                    html_content += "</table>"
            
            # 添加图表（如果有）
            if 'charts' in result:
                html_content += "<h4>图表</h4>"
                html_content += "<div style='display: flex; flex-wrap: wrap;'>"
                
                for chart_type, chart_path in result['charts'].items():
                    chart_basename = os.path.basename(chart_path)
                    # 生成图表的相对路径（从HTML文件到图表文件）
                    chart_rel_path = f"charts/{chart_basename}"
                    html_content += f"""
                    <div style='margin-right: 20px; margin-bottom: 20px;'>
                        <p>{chart_type}</p>
                        <img src="{chart_rel_path}" style="max-width: 100%; height: auto;">
                    </div>
                    """
                
                html_content += "</div>"
        
        # 结束HTML
        html_content += """
        </body>
        </html>
        """
        
        # 保存HTML报告
        report_path = os.path.join(self.output_dir, 'report.html')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)


async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='数据库表质量检查工具')
    parser.add_argument('-t', '--tables', nargs='+', help='要检查的表名（不提供则检查所有表）')
    parser.add_argument('-s', '--start-date', help='开始日期 (YYYYMMDD格式)')
    parser.add_argument('-e', '--end-date', help='结束日期 (YYYYMMDD格式)')
    parser.add_argument('-o', '--output-dir', help='输出目录')
    parser.add_argument('-v', '--verbose', action='store_true', help='显示详细日志')
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 获取数据库连接字符串
    db_dsn = os.environ.get("DATABASE_URL", "postgresql://postgres:wuhao123@localhost:5432/tusharedb")
    
    try:
        # 初始化TaskFactory
        await TaskFactory.initialize(db_dsn)
        logger.info(f"数据库连接已初始化")
        
        # 创建质量检查器
        checker = DBQualityChecker(args.output_dir)
        
        # 执行检查
        await checker.check_tables(args.tables, args.start_date, args.end_date)
        
        # 保存结果
        file_paths = checker.save_results()
        
        logger.info("检查完成!")
        logger.info(f"详细结果: {file_paths['results_path']}")
        logger.info(f"总结: {file_paths['summary_path']}")
        logger.info(f"HTML报告: {file_paths['report_path']}")
        
    except Exception as e:
        logger.error(f"执行过程中发生错误: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # 关闭数据库连接
        await TaskFactory.shutdown()
        logger.info("数据库连接已关闭")


if __name__ == "__main__":
    asyncio.run(main()) 