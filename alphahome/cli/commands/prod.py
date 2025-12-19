"""
生产脚本命令组

集成 scripts/production/* 下的生产脚本和任务。
初期采用 subprocess 透传方式，后续逐步改造为包内模块。
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path
from typing import Optional, Dict, List

from alphahome.cli.core.exitcodes import SUCCESS, FAILURE, INVALID_ARGS
from alphahome.cli.core.logging_config import get_cli_logger
from alphahome.cli.commands.base import CommandGroup

logger = get_cli_logger(__name__)

# 生产脚本别名映射
# 格式: 别名 -> (脚本路径, 描述)
PROD_SCRIPTS = {
    'data-collection': (
        'scripts/production/data_updaters/tushare/data_collection_smart_update_production.py',
        '通用数据采集智能增量更新'
    ),
    'pit-update': (
        'scripts/production/data_updaters/pit/pit_data_update_production.py',
        'PIT数据统一更新'
    ),
    'g-factor': (
        'scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation.py',
        'G因子年度并行计算启动器'
    ),
    'g-factor-quarterly': (
        'scripts/production/factor_calculators/g_factor/start_parallel_g_factor_calculation_quarterly.py',
        'G因子季度并行计算启动器'
    ),
    'p-factor': (
        'scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py',
        'P因子年度并行计算启动器'
    ),
    'p-factor-quarterly': (
        'scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation_quarterly.py',
        'P因子季度并行计算启动器'
    ),
}

# 已改造为包内模块的脚本映射
# 格式: 别名 -> (模块路径, 函数名, 描述)
PROD_MODULES = {
    'p-factor': (
        'alphahome.production.factors.p_factor',
        'run_parallel_p_factor_calculation',
        'P因子年度并行计算启动器 (包内模块)'
    ),
}


class ProdCommandGroup(CommandGroup):
    """生产脚本命令组"""
    
    group_name = "prod"
    group_help = "生产脚本和任务管理"
    
    def add_subparsers(self, subparsers: argparse._SubParsersAction) -> None:
        """添加生产命令到子解析器容器"""
        prod_parser = subparsers.add_parser(
            self.group_name,
            help=self.group_help,
            description="AlphaHome 生产脚本和任务"
        )
        
        sub = prod_parser.add_subparsers(dest='prod_command', help='生产命令')
        
        # run: 运行生产脚本
        run_parser = sub.add_parser(
            'run',
            help='运行生产脚本',
            description='使用别名运行预定义的生产脚本'
        )
        run_parser.add_argument(
            'alias',
            help=f'脚本别名: {", ".join(PROD_SCRIPTS.keys())}'
        )
        run_parser.add_argument(
            'script_args',
            nargs=argparse.REMAINDER,
            help='传递给脚本的参数'
        )
        run_parser.set_defaults(func=_run_prod_script)
        
        # list: 列出所有可用脚本
        list_parser = sub.add_parser('list', help='列出所有可用的生产脚本')
        list_parser.set_defaults(func=_list_prod_scripts)
        
        prod_parser.set_defaults(group='prod')


def _run_prod_script(args) -> int:
    """运行生产脚本"""
    try:
        alias = args.alias

        # 检查是否是已改造的包内模块
        if alias in PROD_MODULES:
            module_path, func_name, description = PROD_MODULES[alias]

            logger.info(f"执行包内模块: {description}")
            logger.info(f"模块路径: {module_path}.{func_name}")

            # 动态导入并调用包内模块
            try:
                import importlib
                module = importlib.import_module(module_path)
                func = getattr(module, func_name)

                # 创建参数解析器来解析脚本参数
                import argparse
                parser = argparse.ArgumentParser()
                # 添加p-factor相关的参数
                if alias == 'p-factor':
                    parser.add_argument('--start_year', type=int, default=2020)
                    parser.add_argument('--end_year', type=int, default=2024)
                    parser.add_argument('--workers', type=int, default=10)
                    parser.add_argument('--delay', type=int, default=2)

                # 准备传递给模块的参数
                script_args = getattr(args, 'script_args', [])

                # 如果参数中包含 '--'，从 '--' 之后开始才是脚本参数
                if '--' in script_args:
                    script_args = script_args[script_args.index('--') + 1:]

                logger.debug(f"模块参数: {script_args}")

                # 解析参数
                module_args = parser.parse_args(script_args)

                # 调用模块函数
                return func(module_args)

            except Exception as e:
                logger.error(f"调用包内模块失败: {e}", exc_info=True)
                return FAILURE

        # 原有的subprocess方式处理
        elif alias in PROD_SCRIPTS:
            script_path, description = PROD_SCRIPTS[alias]

            # 解析相对路径
            project_root = Path(__file__).parent.parent.parent.parent
            full_script_path = project_root / script_path

            if not full_script_path.exists():
                logger.error(f"脚本文件不存在: {full_script_path}")
                return INVALID_ARGS

            logger.info(f"执行生产脚本: {description}")
            logger.info(f"脚本路径: {full_script_path}")

            # 准备传递给脚本的参数
            script_args = getattr(args, 'script_args', [])

            # 如果参数中包含 '--'，从 '--' 之后开始才是脚本参数
            if '--' in script_args:
                script_args = script_args[script_args.index('--') + 1:]

            logger.debug(f"脚本参数: {script_args}")

            # 使用 subprocess 调起脚本
            # 保持脚本的原始输出和错误处理
            cmd = [sys.executable, str(full_script_path)] + script_args

            logger.debug(f"执行命令: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                cwd=str(project_root)
            )

            if result.returncode != 0:
                logger.error(f"脚本执行失败，退出码: {result.returncode}")

            return result.returncode

        else:
            logger.error(f"未知的脚本别名: {alias}")
            logger.info(f"可用别名: {', '.join(list(PROD_SCRIPTS.keys()) + list(PROD_MODULES.keys()))}")
            return INVALID_ARGS

    except KeyboardInterrupt:
        logger.warning("用户中断执行")
        return 130
    except Exception as e:
        logger.error(f"运行生产脚本失败: {e}", exc_info=True)
        return FAILURE


def _list_prod_scripts(args) -> int:
    """列出所有可用的生产脚本"""
    try:
        import sys
        
        # 直接写入 sys.stdout.buffer 以避免 TextIOWrapper 问题
        output = "\n可用的生产脚本:\n"
        output += "-" * 60 + "\n"

        # 显示包内模块（优先显示）
        for alias, (module_path, func_name, description) in PROD_MODULES.items():
            output += f"  {alias:<25} {description}\n"

        # 显示传统脚本
        for alias, (script_path, description) in PROD_SCRIPTS.items():
            output += f"  {alias:<25} {description}\n"

        output += "-" * 60 + "\n"
        output += f"\n用法: ah prod run <alias> [-- script_args]\n"
        output += f"示例: ah prod run p-factor -- --start_year 2020 --end_year 2024 --workers 5\n"
        output += f"      ah prod run data-collection -- --workers 5 --log_level DEBUG\n\n"
        
        # 使用 sys.stdout 直接写入，避免 TextIOWrapper 关闭问题
        sys.stdout.write(output)
        sys.stdout.flush()
        
        return SUCCESS
        
    except Exception as e:
        logger.error(f"列出脚本失败: {e}", exc_info=True)
        return FAILURE
