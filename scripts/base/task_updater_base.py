#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
任务更新器基类

为所有数据更新脚本提供统一的基础设施：
1. 环境设置
2. 日志配置
3. 参数解析
4. 错误处理
5. 结果汇总
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import dotenv

from data_module.task_factory import TaskFactory

class TaskUpdaterBase:
    def __init__(
        self,
        task_name: str,
        task_type: str,
        description: str,
        support_report_period: bool = False
    ):
        self.task_name = task_name
        self.task_type = task_type
        self.description = description
        self.support_report_period = support_report_period
        
        # 设置项目路径
        self.project_root = Path(__file__).resolve().parent.parent.parent
        sys.path.append(str(self.project_root))
        
        # 加载环境变量
        dotenv_path = self.project_root / '.env'
        dotenv.load_dotenv(dotenv_path=dotenv_path)
        
        # 设置日志
        self.setup_logging()
        
    def setup_logging(self):
        """配置日志"""
        # 创建logs目录（如果不存在）
        log_dir = self.project_root / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        # 生成日志文件名
        log_filename = log_dir / f'{self.task_name}_{datetime.now().strftime("%Y%m%d")}.log'
        
        # 配置日志格式和处理器
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(str(log_filename))
            ]
        )
        self.logger = logging.getLogger(self.task_name)
        self.logger.info(f"日志文件位置: {log_filename}")
    
    def setup_parser(self) -> argparse.ArgumentParser:
        """设置命令行参数解析器"""
        parser = argparse.ArgumentParser(description=self.description)
        parser.add_argument('--full-update', action='store_true', help='全量更新模式')
        parser.add_argument('--start-date', help='更新起始日期 (YYYYMMDD)')
        parser.add_argument('--end-date', help='更新结束日期 (YYYYMMDD)')
        parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True)
        
        if self.support_report_period:
            parser.add_argument('--quarters', type=int, help='更新最近N个季度')
            parser.add_argument('--years', type=int, help='更新最近N年')
            parser.add_argument('--report-period', help='指定报告期 (YYYYMM)')
        
        return parser
    
    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        """执行任务更新（子类必须实现）"""
        # 1. 获取任务实例
        task = await TaskFactory.get_task(self.task_name)
        
        # 2. 准备更新参数
        update_kwargs = {'show_progress': args.show_progress}
        
        # 3. 根据不同模式执行更新
        if args.full_update:
            # 全量更新
            return await task.execute(start_date='19900101', end_date='当前日期', **update_kwargs)
        elif args.report_period or args.quarters or args.years:
            # 报告期更新
            start_date, end_date = self.calculate_report_dates(args)
            return await task.execute(start_date=start_date, end_date=end_date, **update_kwargs)
        elif args.start_date:
            # 日期范围更新
            end_date = args.end_date or datetime.now().strftime('%Y%m%d')
            return await task.execute(start_date=args.start_date, end_date=end_date, **update_kwargs)
        else:
            # 智能增量更新（默认）
            return await task.smart_incremental_update(lookback_days=365, **update_kwargs)
    
    def summarize_result(self, result: Dict[str, Any], args: argparse.Namespace) -> None:
        """汇总结果"""
        if not isinstance(result, dict):
            self.logger.error(f"结果格式错误: {result}")
            return
            
        self.logger.info(f"{self.task_type}数据更新结果汇总:")
        
        # 显示更新参数
        if hasattr(args, 'report_period') and args.report_period:
            self.logger.info(f"报告期: {self.format_report_period(args.report_period)}")
        elif hasattr(args, 'quarters') and args.quarters:
            self.logger.info(f"更新季度数: 最近 {args.quarters} 个季度")
        elif hasattr(args, 'years') and args.years:
            self.logger.info(f"更新年份数: 最近 {args.years} 年")
        elif args.start_date:
            self.logger.info(f"更新日期范围: {args.start_date} 至 {args.end_date or datetime.now().strftime('%Y%m%d')}")
        elif args.full_update:
            self.logger.info("更新模式: 全量更新")
        else:
            self.logger.info("更新模式: 智能增量更新")
            
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        failed_batches = result.get('failed_batches', 0)
        
        if status == 'success':
            self.logger.info(f"- {self.task_type}: 成功, 更新 {rows} 行数据")
        elif status == 'partial_success':
            self.logger.info(f"- {self.task_type}: 部分成功, 更新 {rows} 行数据, {failed_batches} 个批次失败")
        elif status == 'no_data':
            self.logger.info(f"- {self.task_type}: 没有需要更新的数据")
        else:
            self.logger.error(f"- {self.task_type}: 失败, 错误: {result.get('error', 'unknown error')}")
            
        self.logger.info(f"总计更新: {rows} 行{self.task_type}数据")
    
    @staticmethod
    def format_report_period(period: Optional[str]) -> str:
        """格式化报告期"""
        if not period:
            return "未指定"
        if len(period) != 6:
            return period
        year, month = period[:4], int(period[4:])
        quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
        quarter = quarter_map.get(month, f"{month}月")
        return f"{year}年{quarter}"
    
    async def run(self):
        """运行任务更新"""
        start_time = datetime.now()
        self.logger.info(f"开始执行{self.task_type} ({self.task_name}) 数据更新...")
        
        parser = self.setup_parser()
        args = parser.parse_args()
        
        await TaskFactory.initialize()
        self.logger.info("TaskFactory初始化成功")
        
        result = None
        try:
            result = await self.update_task(args)
            self.summarize_result(result, args)
        except Exception as e:
            self.logger.error(f"{self.task_type}数据更新过程中发生错误: {str(e)}", exc_info=True)
            if result is None:
                self.summarize_result({
                    'task_name': self.task_name,
                    'status': 'error',
                    'error': f"更新主流程异常: {str(e)}",
                    'rows': 0,
                    'failed_batches': 0
                }, args)
        finally:
            await TaskFactory.shutdown()
            self.logger.info("TaskFactory已关闭")
        
        end_time = datetime.now()
        duration = end_time - start_time
        self.logger.info(f"{self.task_type}数据更新执行完毕。总耗时: {duration}") 