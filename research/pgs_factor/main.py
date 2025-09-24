#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PGS因子计算系统主程序 v2.0
==========================

轻量级统一接口，无历史包袱的全新设计。

使用示例：
    # 统一数据管道接口
    python main.py --sync-pit --mode incremental
    python main.py --calculate-factors --date 2024-12-31
    python main.py --query-factors --date 2024-12-31
    
    # 传统单独组件调用
    python main.py --pit-manager --mode full
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import List, Optional
from pathlib import Path

# 统一日志工具（方案C）
try:
    from alphahome.common.logging_utils import setup_logging, get_logger
except Exception:
    setup_logging = None
    def get_logger(name: Optional[str] = None):
        return logging.getLogger(name)

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from research.tools.context import ResearchContext
from research.pgs_factor import DataPipeline, PITManager


class PGSFactorSystem:
    """PGS因子计算系统 v2.0 - 轻量级设计
    
    提供统一的DataPipeline接口和传统组件接口。
    """
    
    def __init__(self):
        """初始化因子计算系统"""
        self.context = ResearchContext()
        self.pipeline = DataPipeline(self.context)
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('PGSFactorSystem')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def sync_pit_data(self, mode: str = 'incremental', sources: List[str] = None):
        """同步PIT数据

        注意：此方法已迁移到生产脚本系统
        新位置：scripts/production/data_updaters/pit/pit_data_update_production.py
        """
        self.logger.warning("⚠️  PIT数据同步功能已迁移到生产脚本系统")
        self.logger.warning("   新位置: scripts/production/data_updaters/pit/")
        self.logger.warning("   建议使用: python scripts/production/data_updaters/pit/pit_data_update_production.py")

        # 调用新的生产脚本
        import subprocess
        import sys
        import os

        try:
            # 构建命令
            cmd = [
                sys.executable,
                os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts', 'production', 'data_updaters', 'pit', 'pit_data_update_production.py'),
                '--target', 'all',
                '--mode', mode
            ]

            self.logger.info(f"执行命令: {' '.join(cmd)}")

            # 执行命令
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(__file__)))

            if result.returncode == 0:
                self.logger.info("PIT数据同步成功完成")
                return {"status": "success", "message": "PIT数据同步成功"}
            else:
                self.logger.error(f"PIT数据同步失败: {result.stderr}")
                return {"status": "error", "message": result.stderr}

        except Exception as e:
            self.logger.error(f"PIT数据同步异常: {e}")
            return {"status": "error", "message": str(e)}
    
    def calculate_factors(self, calc_date: str, factors: List[str] = None, stocks: List[str] = None):
        """计算因子"""
        self.logger.info(f"开始计算因子 - 日期: {calc_date}")
        
        if factors is None:
            factors = ['P', 'G', 'S']
        
        result = self.pipeline.calculate_factors(
            factors=factors,
            calc_date=calc_date,
            stocks=stocks
        )
        
        self.logger.info(f"因子计算完成: {result}")
        return result
    
    def query_factors(self, calc_date: str, factors: List[str] = None, stocks: List[str] = None):
        """查询因子数据"""
        self.logger.info(f"查询因子数据 - 日期: {calc_date}")
        
        result = self.pipeline.query_factors(
            calc_date=calc_date,
            factors=factors,
            stocks=stocks
        )
        
        self.logger.info(f"查询完成，返回 {len(result)} 条记录")
        return result
    
    def run_pit_manager(self, mode: str = 'incremental', stocks: List[str] = None):
        """运行PIT数据管理器"""
        self.logger.info(f"运行PIT数据管理器 - 模式: {mode}")
        
        pit_manager = PITManager(self.context)
        pit_manager.ensure_tables_exist()
        
        if mode == 'full':
            # 全量重建
            result = {
                'report': pit_manager.process_report_data(stocks),
                'express': pit_manager.process_express_data(stocks),
                'forecast': pit_manager.process_forecast_data(stocks)
            }
        else:
            # 增量更新
            result = {
                'report': pit_manager.process_report_data(stocks, '2024-01-01'),
                'express': pit_manager.process_express_data(stocks, '2024-01-01'),
                'forecast': pit_manager.process_forecast_data(stocks, '2024-01-01')
            }
        
        self.logger.info(f"PIT数据管理完成: {result}")
        return result


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PGS因子计算系统 v2.0')
    
    # 统一数据管道接口
    parser.add_argument('--sync-pit', action='store_true', help='同步PIT数据')
    parser.add_argument('--calculate-factors', action='store_true', help='计算因子')
    parser.add_argument('--query-factors', action='store_true', help='查询因子数据')
    
    # 传统组件接口
    parser.add_argument('--pit-manager', action='store_true', help='运行PIT数据管理器')
    
    # 通用参数
    parser.add_argument('--mode', default='incremental', choices=['incremental', 'full'], 
                       help='处理模式')
    parser.add_argument('--date', help='计算日期 (YYYY-MM-DD)')
    parser.add_argument('--factors', nargs='+', choices=['P', 'G', 'S'], 
                       help='因子类型')
    parser.add_argument('--stocks', nargs='+', help='股票列表')
    parser.add_argument('--sources', nargs='+', choices=['report', 'express', 'forecast'],
                       help='数据源列表')
    
    args = parser.parse_args()
    
    # 统一初始化日志（方案C），仅初始化一次
    try:
        if setup_logging:
            setup_logging(log_level="INFO", log_to_file=True, log_dir="logs", log_filename=f"pgs_factor_{datetime.now().strftime('%Y%m%d')}.log")
    except Exception:
        pass

    # 创建系统实例
    system = PGSFactorSystem()

    try:
        if args.sync_pit:
            # 同步PIT数据
            system.sync_pit_data(
                mode=args.mode,
                sources=args.sources
            )
            
        elif args.calculate_factors:
            # 计算因子
            if not args.date:
                args.date = datetime.now().strftime('%Y-%m-%d')
            
            system.calculate_factors(
                calc_date=args.date,
                factors=args.factors,
                stocks=args.stocks
            )
            
        elif args.query_factors:
            # 查询因子数据
            if not args.date:
                args.date = datetime.now().strftime('%Y-%m-%d')
            
            result = system.query_factors(
                calc_date=args.date,
                factors=args.factors,
                stocks=args.stocks
            )
            
            print(f"查询结果: {result}")
            
        elif args.pit_manager:
            # 运行PIT数据管理器
            system.run_pit_manager(
                mode=args.mode,
                stocks=args.stocks
            )
            
        else:
            # 默认显示帮助
            parser.print_help()
            
    except Exception as e:
        logging.error(f"执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
