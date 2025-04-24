#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
运行指数基本信息 (TushareIndexBasicTask) 全量更新任务的脚本。

参照 update_report_rc.py 的模式重写。

使用方法:
python scripts/tasks/index/run_tushare_index_basic_full_update.py [--full-update]

说明:
- 此脚本用于启动 'tushare_index_basic' 任务。
- 该任务本身设计为始终执行全量替换（通过 pre_execute 清空表）。
- --full-update 参数在此脚本中主要用于日志记录，不影响实际任务逻辑。
"""

import os
import sys
import asyncio
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any
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

# 配置基本日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
        # 可以添加 FileHandler 如果需要
    ]
)

# 定义目标任务名称常量
TARGET_TASK_NAME = "tushare_index_basic"

class IndexBasicUpdater(TaskUpdaterBase):
    """
    指数基本信息数据更新工具

    继承自 TaskUpdaterBase，专门用于更新指数基本信息 (全量替换)。
    """

    def __init__(self):
        super().__init__(
            task_name=TARGET_TASK_NAME,
            task_type="指数基本信息",
            description="指数基本信息全量更新工具"
            # support_report_period=False (default is False)
        )
        # self.logger 已由基类初始化
        self.logger.name = self.__class__.__name__ # 可以重命名 logger

    async def update_task(self, args: argparse.Namespace) -> Dict[str, Any]:
        """
        执行指数基本信息的全量更新任务。
        
        Args:
            args: 解析后的命令行参数 (可能包含 show_progress)
            
        Returns:
            Dict: 任务执行结果字典
        """
        self.logger.info(f"开始执行任务: {self.task_name}")
        
        try:
            task = await TaskFactory.get_task(self.task_name)
            
            # 准备传递给 task.execute 的参数
            # TushareTask.execute 接受 show_progress
            execute_kwargs = {}
            if hasattr(args, 'show_progress') and args.show_progress is not None:
                execute_kwargs['show_progress'] = args.show_progress
            else:
                # 可以设置一个默认值，或者依赖 task.execute 的默认值
                execute_kwargs['show_progress'] = False 
            
            # 直接调用 execute，无需日期参数，任务内部处理全量逻辑
            result = await task.execute(**execute_kwargs)
            
            if not isinstance(result, dict):
                self.logger.error(f"任务 {self.task_name} 返回结果格式错误: {result}")
                return {"status": "error", "error": "任务返回结果格式错误", "rows": 0}
                
            self.logger.info(f"任务 {self.task_name} 执行完成，状态: {result.get('status', 'unknown')}, 行数: {result.get('rows', 0)}")
            return result

        except Exception as e:
            error_msg = f"任务 {self.task_name} 更新过程发生错误: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return {"status": "error", "error": error_msg, "rows": 0}

    def summarize_result(self, result: Dict[str, Any], args: argparse.Namespace):
        """
        汇总全量更新结果。
        
        Args:
            result: update_task 返回的结果字典
            args: 命令行参数 (主要用于判断是否显式指定了 --full-update)
        """
        status = result.get('status', 'unknown')
        rows = result.get('rows', 0)
        error_msg = result.get('error')

        self.logger.info(f"------ {self.task_type} ({self.task_name}) 更新结果汇总 ------")
        self.logger.info(f"更新模式: 全量替换 (任务内置逻辑)")
        if hasattr(args, 'full_update') and args.full_update:
            self.logger.info(f"(命令行指定了 --full-update 标志)")

        if status == 'success':
            self.logger.info(f"状态: 成功")
            self.logger.info(f"更新行数: {rows}")
        elif status == 'no_data':
            self.logger.info(f"状态: 无数据")
            self.logger.info(f"说明: Tushare API 未返回任何数据。")
        elif status == 'error':
            self.logger.error(f"状态: 失败")
            if error_msg:
                self.logger.error(f"错误信息: {error_msg}")
            else:
                self.logger.error("错误信息: 未知错误")
        else:
            # 处理其他可能的状态，例如 TushareTask 可能返回的 partial_success
            self.logger.warning(f"状态: {status}")
            self.logger.info(f"更新行数: {rows}")
            if error_msg:
                self.logger.warning(f"相关信息: {error_msg}")
        self.logger.info(f"--------------------------------------------------")

async def main():
    updater = IndexBasicUpdater()

    parser = argparse.ArgumentParser(description=f"更新 {updater.task_type} ({updater.task_name}) 数据")
    # 虽然任务总是全量，保留此参数以符合 TaskUpdaterBase 的日志格式化和用户预期
    parser.add_argument("--full-update", action="store_true", help="执行全量更新 (此任务始终全量，此标志主要影响日志)")
    # 添加 show_progress 参数，如果 TaskUpdaterBase 或 Task 支持的话
    # 检查 TaskUpdaterBase 的 setup_parser 方法确认参数名
    # TaskUpdaterBase 使用的是 --show-progress / --no-show-progress
    parser.add_argument('--show-progress', action=argparse.BooleanOptionalAction, default=True, help="显示进度条")

    args = parser.parse_args()

    # 初始化 TaskFactory (假设需要)
    await TaskFactory.initialize()
    
    result = None
    try:
        # 执行更新
        result = await updater.update_task(args)

        # 汇总结果
        updater.summarize_result(result, args)

    except Exception as e:
        logging.error(f"更新主流程发生未知错误: {str(e)}", exc_info=True)
        # 即使主流程错误，也尝试用已知信息汇总
        if result is None:
            result = {"status": "error", "error": f"主流程错误: {str(e)}", "rows": 0}
        updater.summarize_result(result, args)
        sys.exit(1)
    finally:
        # 关闭 TaskFactory (假设需要)
        await TaskFactory.shutdown()

if __name__ == "__main__":
    # 使用示例：
    # python scripts/tasks/index/run_tushare_index_basic_full_update.py
    # python scripts/tasks/index/run_tushare_index_basic_full_update.py --full-update # 日志更明确
    # python scripts/tasks/index/run_tushare_index_basic_full_update.py --no-show-progress # 不显示进度条
    asyncio.run(main()) 