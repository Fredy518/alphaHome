#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
通用数据质量检查示例脚本

使用通用数据质量检查器对任何表进行检查，并输出结果。
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path
import argparse
from datetime import datetime

# 将项目根目录添加到路径
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(project_root, 'logs', f'data_quality_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'))
    ]
)
logger = logging.getLogger('data_quality_check')

# 导入通用数据质量检查器
from data_module import DataQualityChecker, TaskFactory

async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='检查数据表质量')
    parser.add_argument('table_name', help='要检查的表名')
    parser.add_argument('--start_date', help='开始日期 (YYYYMMDD格式)', default=None)
    parser.add_argument('--end_date', help='结束日期 (YYYYMMDD格式)', default=None)
    parser.add_argument('--output', help='输出结果的文件路径', default=None)
    
    args = parser.parse_args()
    
    try:
        # 初始化检查器
        checker = DataQualityChecker()
        
        # 执行检查
        logger.info(f"开始检查表 {args.table_name} 的数据质量...")
        results = await checker.run_check(
            table_name=args.table_name,
            start_date=args.start_date,
            end_date=args.end_date
        )
        
        # 打印摘要
        if 'summary' in results:
            logger.info("数据质量检查摘要:")
            logger.info("-" * 80)
            
            summary = results['summary']
            logger.info(f"总记录数: {summary['total_records']}")
            logger.info(f"包含空值的列数: {summary['columns_with_nulls']}")
            
            if summary.get('top_null_columns'):
                logger.info("空值比例最高的列:")
                for col in summary['top_null_columns']:
                    logger.info(f"  - {col['column']}: {col['null_ratio']:.2%}")
            
            if 'date_coverage_ratio' in summary:
                logger.info(f"日期覆盖率: {summary['date_coverage_ratio']:.2%}")
                logger.info(f"总交易日数: {summary['total_dates']}")
                logger.info(f"缺失数据的交易日数: {summary['empty_dates']}")
                
                if summary.get('worst_months'):
                    logger.info("覆盖率最差的月份:")
                    for month in summary['worst_months']:
                        logger.info(f"  - {month['month']}: {month['coverage_ratio']:.2%} ({month['covered_days']}/{month['total_days']})")
            
            logger.info("-" * 80)
        
        # 保存结果
        if args.output:
            output_file = args.output
        else:
            output_dir = os.path.join(project_root, 'reports')
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{args.table_name}_quality_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # 删除不能JSON序列化的项
            results_copy = results.copy()
            if 'charts' in results_copy:
                # 保留图表文件路径，但不尝试序列化图表对象
                chart_paths = {k: v for k, v in results_copy['charts'].items()}
                results_copy['chart_paths'] = chart_paths
            
            json.dump(results_copy, f, ensure_ascii=False, indent=2, default=str)
            
        logger.info(f"检查结果已保存到: {output_file}")
        if 'charts' in results:
            for chart_name, chart_path in results['charts'].items():
                logger.info(f"图表 '{chart_name}' 已保存到: {chart_path}")
        
    except Exception as e:
        logger.error(f"执行检查时发生错误: {str(e)}", exc_info=True)
    finally:
        # 关闭数据库连接
        await TaskFactory.shutdown()

if __name__ == "__main__":
    asyncio.run(main()) 