#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生产级P因子计算运维脚本
===================

功能特性：
- 统一的增量更新和历史回填功能
- 智能模式选择和数据状态检测
- 完整的错误处理和异常恢复机制
- 集成退市股票筛选和全市场排名系统
- 断点续传和失败重试机制
- 生产级性能和安全标准

作者: AlphaHome Team
版本: 2.0.0
创建时间: 2025-08-12
"""

import sys
import os
import argparse
import time
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from research.tools.context import ResearchContext
from research.pgs_factor.processors.production_p_factor_calculator import ProductionPFactorCalculator


class ProductionPFactorRunner:
    """生产级P因子计算运维脚本
    
    负责CLI参数解析、任务调度和错误处理，
    实际计算逻辑委托给ProductionPFactorCalculator。
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """初始化运维脚本
        
        Args:
            config: 配置参数字典
        """
        self.config = config or {}
        self.calculator = None
        self.logger = None
        
        # 执行统计
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_dates': 0,
            'successful_dates': 0,
            'failed_dates': 0,
            'total_stocks_processed': 0,
            'total_records_saved': 0,
            'errors': [],
            'performance_metrics': {}
        }
        
        self._setup_logging()
        self._initialize_calculator()
    
    def _setup_logging(self):
        """设置生产级日志系统"""
        log_level = self.config.get('log_level', 'INFO')
        log_file = self.config.get('log_file') or f'production_p_factor_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

        # 创建日志目录
        log_dir = Path('logs/production_p_factor')
        log_dir.mkdir(parents=True, exist_ok=True)

        # 完整的日志文件路径
        log_file_path = log_dir / log_file

        # 配置日志格式
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        # 配置日志处理器
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format=log_format,
            handlers=[
                logging.FileHandler(log_file_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )

        self.logger = logging.getLogger('ProductionPFactorRunner')
        self.logger.info(f"日志系统初始化完成，日志文件: {log_file_path}")
    
    def _initialize_calculator(self):
        """初始化P因子计算器"""
        try:
            # 创建研究上下文
            context = ResearchContext()
            
            # 初始化计算器
            self.calculator = ProductionPFactorCalculator(context)
            
            self.logger.info("P因子计算器初始化完成")
            
        except Exception as e:
            self.logger.error(f"初始化P因子计算器失败: {e}")
            raise
    
    def execute_production_calculation(self, start_date: str, end_date: str,
                                     force_mode: Optional[str] = None) -> Dict[str, Any]:
        """执行生产级P因子计算
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            force_mode: 强制执行模式 ('incremental', 'backfill', None为自动检测)
        
        Returns:
            执行结果统计
        """
        self.stats['start_time'] = time.time()
        
        try:
            self.logger.info("=" * 80)
            self.logger.info("开始生产级P因子计算")
            self.logger.info("=" * 80)
            self.logger.info(f"计算范围: {start_date} ~ {end_date}")
            
            # 委托给计算器执行
            result = self.calculator.calculate_p_factors_batch_pit(
                start_date=start_date,
                end_date=end_date,
                mode=force_mode
            )
            
            # 更新统计信息
            self.stats.update(result)
            self.stats['end_time'] = time.time()
            self.stats['total_time'] = self.stats['end_time'] - self.stats['start_time']
            
            # 生成最终报告
            return self._generate_final_report()
            
        except Exception as e:
            self.logger.error(f"生产级P因子计算失败: {e}")
            self.stats['errors'].append(str(e))
            return self._generate_final_report(success=False, error=str(e))
    
    def _generate_final_report(self, success: bool = True, error: str = None) -> Dict[str, Any]:
        """生成最终执行报告"""
        total_time = self.stats.get('total_time', 0)
        success_rate = (self.stats['successful_dates'] / max(self.stats['total_dates'], 1)) * 100
        
        report = {
            'success': success and self.stats['successful_dates'] > 0,
            'total_dates': self.stats['total_dates'],
            'successful_dates': self.stats['successful_dates'],
            'failed_dates': self.stats['failed_dates'],
            'success_rate': success_rate,
            'total_stocks_processed': self.stats['total_stocks_processed'],
            'total_records_saved': self.stats['total_records_saved'],
            'total_time': total_time,
            'average_time_per_date': total_time / max(self.stats['total_dates'], 1),
            'errors': self.stats['errors']
        }
        
        if error:
            report['error'] = error
        
        # 记录最终报告
        self.logger.info("=" * 80)
        self.logger.info("生产级P因子计算完成")
        self.logger.info("=" * 80)
        self.logger.info(f"执行结果: {'成功' if report['success'] else '失败'}")
        self.logger.info(f"总计算日期: {report['total_dates']}")
        self.logger.info(f"成功日期: {report['successful_dates']}")
        self.logger.info(f"失败日期: {report['failed_dates']}")
        self.logger.info(f"成功率: {report['success_rate']:.1f}%")
        self.logger.info(f"处理股票: {report['total_stocks_processed']:,} 只")
        self.logger.info(f"保存记录: {report['total_records_saved']:,} 条")
        self.logger.info(f"总耗时: {report['total_time']:.2f} 秒")
        
        if report['errors']:
            self.logger.warning(f"错误数量: {len(report['errors'])}")
            for i, error in enumerate(report['errors'][:5], 1):  # 只显示前5个错误
                self.logger.warning(f"错误 {i}: {error}")
        
        return report


def create_argument_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description='生产级P因子计算运维脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 日常增量更新
  python production_p_factor_runner.py --start-date 2025-08-01 --end-date 2025-08-12

  # 历史回填
  python production_p_factor_runner.py --start-date 2024-01-01 --end-date 2024-12-31 --mode backfill

  # 试运行检查
  python production_p_factor_runner.py --start-date 2025-08-12 --end-date 2025-08-12 --dry-run

  # 数据质量验证
  python production_p_factor_runner.py --start-date 2025-08-01 --end-date 2025-08-12 --validate-only
        """
    )

    # 必需参数
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='开始日期 (YYYY-MM-DD格式)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        required=True,
        help='结束日期 (YYYY-MM-DD格式)'
    )

    # 执行模式
    parser.add_argument(
        '--mode',
        type=str,
        choices=['auto', 'incremental', 'backfill'],
        default='auto',
        help='执行模式. auto: 自动检测, incremental: 增量更新, backfill: 历史回填. 默认: auto'
    )

    # 重试配置
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='最大重试次数. 默认: 3'
    )

    parser.add_argument(
        '--retry-delay',
        type=int,
        default=5,
        help='重试延迟时间(秒). 默认: 5'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别. 默认: INFO'
    )

    parser.add_argument(
        '--log-file',
        type=str,
        help='自定义日志文件名. 默认: 自动生成'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='试运行模式，只检查配置和数据状态，不执行实际计算'
    )

    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        default=True,
        help='遇到错误时继续执行后续日期. 默认: True'
    )

    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='仅执行数据质量验证，不进行计算'
    )

    return parser


def validate_date_format(date_str: str) -> bool:
    """验证日期格式"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def main():
    """主函数"""
    parser = create_argument_parser()
    args = parser.parse_args()

    # 验证日期格式
    if not validate_date_format(args.start_date):
        print(f"错误: 开始日期格式不正确: {args.start_date}")
        print("请使用格式: YYYY-MM-DD")
        sys.exit(1)

    if not validate_date_format(args.end_date):
        print(f"错误: 结束日期格式不正确: {args.end_date}")
        print("请使用格式: YYYY-MM-DD")
        sys.exit(1)

    # 验证日期范围
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    if start_date > end_date:
        print("错误: 开始日期不能晚于结束日期")
        sys.exit(1)

    if end_date > date.today():
        print("警告: 结束日期晚于今天，可能没有相应的财务数据")

    # 构建配置
    config = {
        'max_retries': args.max_retries,
        'retry_delay': args.retry_delay,
        'exponential_backoff': True,
        'log_level': args.log_level,
        'log_file': args.log_file,
        'continue_on_error': args.continue_on_error
    }

    try:
        # 初始化运维脚本
        runner = ProductionPFactorRunner(config)

        if args.dry_run:
            # 试运行模式
            print("🔍 试运行模式")
            print(f"计算范围: {args.start_date} ~ {args.end_date}")

            # 委托给计算器进行检查
            mode = runner.calculator.detect_execution_mode(args.start_date, args.end_date)
            calc_dates = runner.calculator.generate_calculation_dates(args.start_date, args.end_date, mode)

            print(f"检测到执行模式: {mode}")
            print(f"需要计算的日期数: {len(calc_dates)}")
            if calc_dates:
                print(f"日期范围: {calc_dates[0]} ~ {calc_dates[-1]}")

            print("✅ 试运行完成，配置正常")
            return

        if args.validate_only:
            # 仅验证模式
            print("🔍 数据质量验证模式")
            calc_dates = runner.calculator._generate_friday_dates(args.start_date, args.end_date)
            runner.calculator._validate_calculation_results(calc_dates)
            return

        # 执行生产级计算
        force_mode = None if args.mode == 'auto' else args.mode
        result = runner.execute_production_calculation(
            start_date=args.start_date,
            end_date=args.end_date,
            force_mode=force_mode
        )

        # 根据结果设置退出码
        if result['success']:
            print(f"\n🎉 P因子计算成功完成!")
            print(f"成功率: {result['success_rate']:.1f}%")
            print(f"处理记录: {result['total_records_saved']:,} 条")
            sys.exit(0)
        else:
            print(f"\n❌ P因子计算执行失败!")
            if result.get('error'):
                print(f"错误信息: {result['error']}")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断执行")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 程序执行异常: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
