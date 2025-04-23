#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票每日基本指标数据更新脚本

使用 TaskUpdaterBase 基类实现的股票每日基本指标数据更新工具。
支持以下功能：
1. 按天数更新数据
2. 支持指定日期范围更新
3. 支持多种更新模式（增量/全量）
4. 详细的日志记录和结果汇总
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime, timedelta
import dotenv
from pathlib import Path

# 添加项目根目录到系统路径
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

# 加载环境变量
dotenv_path = project_root / '.env'
dotenv.load_dotenv(dotenv_path=dotenv_path)

from scripts.base.task_updater_base import TaskUpdaterBase
from data_module.task_factory import TaskFactory

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_stock_dailybasic"

class DailyBasicUpdater(TaskUpdaterBase):
    """
    股票每日基本指标数据更新工具
    
    继承自 TaskUpdaterBase，用于更新股票每日基本指标数据。
    支持按天数更新、指定日期范围更新、全量更新等多种更新模式。
    """
    
    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="股票每日基本指标",
            description="股票每日基本指标数据更新工具",
            support_report_period=False
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    async def update_task(self, start_date=None, end_date=None, full_update=False):
        """
        更新股票每日基本指标数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            full_update: 是否全量更新
            
        Returns:
            tuple: (成功更新数量, 失败数量, 错误信息列表)
        """
        self.logger.info("开始更新股票每日基本指标数据...")
        
        try:
            task = await TaskFactory.get_task(TARGET_TASK_NAME)
            result = await task.run(start_date=start_date, end_date=end_date, full_update=full_update)
            
            if not isinstance(result, dict):
                self.logger.error(f"任务返回结果格式错误: {result}")
                return 0, 1, [f"任务返回结果格式错误: {result}"]
                
            success_count = result.get("success", 0)
            failed_count = result.get("failed", 0)
            error_msgs = result.get("error_msgs", [])
            
            self.logger.info(f"更新完成。成功: {success_count}, 失败: {failed_count}")
            if error_msgs:
                self.logger.warning(f"错误信息: {error_msgs}")
                
            return success_count, failed_count, error_msgs
            
        except Exception as e:
            error_msg = f"更新过程发生错误: {str(e)}"
            self.logger.error(error_msg)
            return 0, 1, [error_msg]

    async def summarize_result(self, success_count, failed_count, error_msgs, start_date=None, end_date=None, full_update=False):
        """
        汇总更新结果
        
        Args:
            success_count: 成功更新数量
            failed_count: 失败数量
            error_msgs: 错误信息列表
            start_date: 开始日期
            end_date: 结束日期
            full_update: 是否全量更新
        """
        total = success_count + failed_count
        
        if total == 0:
            self.logger.info("本次更新无数据")
            return
            
        success_rate = (success_count / total) * 100 if total > 0 else 0
        
        # 输出更新模式
        if full_update:
            self.logger.info("更新模式: 全量更新")
        elif start_date and end_date:
            self.logger.info(f"更新模式: 指定日期范围更新 ({start_date} 至 {end_date})")
        else:
            self.logger.info("更新模式: 增量更新")
            
        # 输出更新结果统计
        self.logger.info(f"更新结果汇总:")
        self.logger.info(f"总数据量: {total}")
        self.logger.info(f"成功数量: {success_count}")
        self.logger.info(f"失败数量: {failed_count}")
        self.logger.info(f"成功率: {success_rate:.2f}%")
        
        # 如果有错误信息，输出详细错误
        if error_msgs:
            self.logger.warning("错误详情:")
            for msg in error_msgs:
                self.logger.warning(f"- {msg}")

async def main():
    updater = DailyBasicUpdater()
    
    parser = argparse.ArgumentParser(description="更新股票每日基本指标数据")
    parser.add_argument("--start-date", help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end-date", help="结束日期 (YYYYMMDD)")
    parser.add_argument("--full-update", action="store_true", help="全量更新")
    
    args = parser.parse_args()
    
    # 参数检查
    if args.start_date and not args.end_date:
        parser.error("如果指定开始日期，必须同时指定结束日期")
    if args.end_date and not args.start_date:
        parser.error("如果指定结束日期，必须同时指定开始日期")
        
    try:
        # 执行更新
        success_count, failed_count, error_msgs = await updater.update_task(
            start_date=args.start_date,
            end_date=args.end_date,
            full_update=args.full_update
        )
        
        # 汇总结果
        await updater.summarize_result(
            success_count,
            failed_count,
            error_msgs,
            start_date=args.start_date,
            end_date=args.end_date,
            full_update=args.full_update
        )
        
    except Exception as e:
        logging.error(f"更新过程发生错误: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 