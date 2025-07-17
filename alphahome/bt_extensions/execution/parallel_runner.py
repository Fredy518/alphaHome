#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
并行回测执行器 - Backtrader增强工具

提供多进程并行回测功能，充分利用多核CPU资源。
"""

import multiprocessing as mp
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Union

import backtrader as bt

from ...common.db_manager import create_sync_manager
from ...common.logging_utils import get_logger
from ...common.config_manager import ConfigManager
from ...providers import AlphaDataTool
from ..data.feeds import PostgreSQLDataFeed
from ..utils.performance_monitor import PerformanceMonitor
from .batch_loader import BatchDataLoader


class ParallelBacktestRunner:
    """
    并行回测执行器

    支持：
    - 多进程并行回测
    - 智能任务分配
    - 进度监控和性能统计
    - 结果汇总和分析
    - 资源管理和错误处理
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        batch_size: Optional[int] = None,
        config_manager: Optional[ConfigManager] = None,
    ):
        """
        初始化并行回测执行器

        Args:
            max_workers: 最大工作进程数。如果为None，则从配置或CPU核心数确定。
            batch_size: 每个批次的股票数量。如果为None，则从配置或默认值确定。
            config_manager: 配置管理器实例，用于获取默认值。
        """
        cm = config_manager or ConfigManager()
        config = cm.load_config()

        # 确定 max_workers
        if max_workers is not None:
            self.max_workers = max_workers
        else:
            self.max_workers = config.get('bt_extensions', {}).get('parallel_runner', {}).get('max_workers', mp.cpu_count() - 1)

        # 确定 batch_size
        if batch_size is not None:
            self.batch_size = batch_size
        else:
            self.batch_size = config.get('bt_extensions', {}).get('parallel_runner', {}).get('batch_size', 50)
        
        self.max_workers = max(1, self.max_workers) # 确保至少有一个worker

        self.logger = get_logger("parallel_runner")
        self.performance_monitor = PerformanceMonitor()
        self.db_config = config.get('database') # 预先加载数据库配置

    def run_parallel_backtests(
        self,
        stock_codes: List[str],
        strategy_class: type,
        strategy_params: Dict[str, Any],
        start_date: date,
        end_date: date,
        initial_cash: float = 100000.0,
        commission: float = 0.001,
        **cerebro_kwargs,
    ) -> Dict[str, Any]:
        """
        执行并行回测

        Args:
            stock_codes: 股票代码列表
            strategy_class: 策略类
            strategy_params: 策略参数
            start_date: 开始日期
            end_date: 结束日期
            initial_cash: 初始资金
            commission: 手续费率
            **cerebro_kwargs: 其他Cerebro参数

        Returns:
            包含所有结果的字典
        """
        self.logger.info(
            f"开始并行回测: {len(stock_codes)} 只股票，{self.max_workers} 个进程"
        )

        # 创建任务批次
        batches = self._create_batches(stock_codes)

        # 准备任务参数
        task_args = []
        for batch_idx, batch_codes in enumerate(batches):
            task_args.append(
                {
                    "batch_idx": batch_idx,
                    "stock_codes": batch_codes,
                    "strategy_class": strategy_class,
                    "strategy_params": strategy_params,
                    "start_date": start_date,
                    "end_date": end_date,
                    "initial_cash": initial_cash,
                    "commission": commission,
                    "db_config": self.db_config,  # 将数据库配置传递给子进程
                    **cerebro_kwargs,
                }
            )

        # 开始性能监控
        self.performance_monitor.start_monitoring()

        # 执行并行任务
        results = {}
        failed_batches = []

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_batch = {
                executor.submit(_run_batch_backtest, args): batch_idx
                for batch_idx, args in enumerate(task_args)
            }

            # 收集结果
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_result = future.result()
                    results.update(batch_result)

                    # 计算进度
                    completed_batches = len([f for f in future_to_batch if f.done()])
                    progress = completed_batches / len(batches) * 100

                    self.logger.info(
                        f"批次 {batch_idx + 1} 完成 "
                        f"({len(batch_result)} 只股票) - "
                        f"进度: {progress:.1f}%"
                    )

                except Exception as e:
                    self.logger.error(f"批次 {batch_idx + 1} 失败: {e}")
                    failed_batches.append(batch_idx)

        # 停止性能监控
        performance_stats = self.performance_monitor.stop_monitoring()

        # 汇总结果
        summary = self._create_summary(results, failed_batches, performance_stats)

        self.logger.info(
            f"并行回测完成: 成功 {len(results)}/{len(stock_codes)} 只股票, "
            f"耗时 {performance_stats['duration']:.2f}秒"
        )

        return {
            "results": results,
            "summary": summary,
            "performance": performance_stats,
            "failed_batches": failed_batches,
        }

    def run_strategy_comparison(
        self,
        stock_codes: List[str],
        strategy_configs: List[Dict[str, Any]],
        start_date: date,
        end_date: date,
        **common_kwargs,
    ) -> Dict[str, Any]:
        """
        多策略比较回测

        Args:
            stock_codes: 股票代码列表
            strategy_configs: 策略配置列表，每个包含class和params
            start_date: 开始日期
            end_date: 结束日期
            **common_kwargs: 通用参数

        Returns:
            策略比较结果
        """
        self.logger.info(
            f"开始策略比较: {len(strategy_configs)} 个策略，{len(stock_codes)} 只股票"
        )

        comparison_results = {}

        for strategy_idx, config in enumerate(strategy_configs):
            strategy_name = config.get("name", f"Strategy_{strategy_idx + 1}")
            self.logger.info(f"执行策略: {strategy_name}")

            strategy_results = self.run_parallel_backtests(
                stock_codes=stock_codes,
                strategy_class=config["class"],
                strategy_params=config["params"],
                start_date=start_date,
                end_date=end_date,
                **common_kwargs,
            )

            comparison_results[strategy_name] = strategy_results

        # 生成比较分析
        comparison_analysis = self._analyze_strategy_comparison(comparison_results)

        return {
            "strategy_results": comparison_results,
            "comparison_analysis": comparison_analysis,
        }

    def _create_batches(self, stock_codes: List[str]) -> List[List[str]]:
        """创建股票批次"""
        batches = []
        for i in range(0, len(stock_codes), self.batch_size):
            batch = stock_codes[i : i + self.batch_size]
            batches.append(batch)
        return batches

    def _create_summary(
        self,
        results: Dict[str, Any],
        failed_batches: List[int],
        performance_stats: Dict[str, Any],
    ) -> Dict[str, Any]:
        """创建结果摘要"""
        if not results:
            return {
                "total_stocks": 0,
                "successful_stocks": 0,
                "failed_batches": len(failed_batches),
                "success_rate": 0.0,
                "performance": performance_stats,
            }

        # 计算总体统计
        total_returns = []
        win_rates = []
        max_drawdowns = []

        for stock_code, result in results.items():
            if "final_value" in result and "initial_value" in result:
                ret = (result["final_value"] / result["initial_value"] - 1) * 100
                total_returns.append(ret)

            if "trades" in result and result["trades"]:
                wins = sum(1 for trade in result["trades"] if trade.get("pnl", 0) > 0)
                win_rate = wins / len(result["trades"]) * 100
                win_rates.append(win_rate)

            if "max_drawdown" in result:
                max_drawdowns.append(result["max_drawdown"])

        return {
            "total_stocks": len(results),
            "successful_stocks": len(results),
            "failed_batches": len(failed_batches),
            "success_rate": len(results)
            / (len(results) + len(failed_batches) * self.batch_size)
            * 100,
            "avg_return": (
                sum(total_returns) / len(total_returns) if total_returns else 0
            ),
            "avg_win_rate": sum(win_rates) / len(win_rates) if win_rates else 0,
            "avg_max_drawdown": (
                sum(max_drawdowns) / len(max_drawdowns) if max_drawdowns else 0
            ),
            "performance": performance_stats,
        }

    def _analyze_strategy_comparison(
        self, comparison_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """分析策略比较结果"""
        strategy_metrics = {}

        for strategy_name, strategy_result in comparison_results.items():
            summary = strategy_result.get("summary", {})
            strategy_metrics[strategy_name] = {
                "avg_return": summary.get("avg_return", 0),
                "success_rate": summary.get("success_rate", 0),
                "avg_win_rate": summary.get("avg_win_rate", 0),
                "avg_max_drawdown": summary.get("avg_max_drawdown", 0),
                "execution_time": summary.get("performance", {}).get("duration", 0),
            }

        # 找出最佳策略
        best_strategy = None
        best_score = float("-inf")

        for strategy_name, metrics in strategy_metrics.items():
            # 简单的评分系统（可以根据需要调整权重）
            score = (
                metrics["avg_return"] * 0.4
                + metrics["avg_win_rate"] * 0.3
                + (100 - abs(metrics["avg_max_drawdown"])) * 0.3
            )

            if score > best_score:
                best_score = score
                best_strategy = strategy_name

        return {
            "strategy_metrics": strategy_metrics,
            "best_strategy": best_strategy,
            "best_score": best_score,
            "ranking": sorted(
                strategy_metrics.items(), key=lambda x: x[1]["avg_return"], reverse=True
            ),
        }


def _run_batch_backtest(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    在单个进程中运行一个批次回测

    Args:
        args: 包含所有回测参数的字典

    Returns:
        该批次所有股票的回测结果字典
    """
    batch_idx = args["batch_idx"]
    stock_codes = args["stock_codes"]
    strategy_class = args["strategy_class"]
    strategy_params = args["strategy_params"]
    start_date = args["start_date"]
    end_date = args["end_date"]
    db_config = args["db_config"] # 从参数中获取数据库配置

    logger = get_logger(f"batch_worker_{batch_idx}")
    logger.info(f"批次 {batch_idx + 1} 开始处理 {len(stock_codes)} 只股票")

    # 在子进程中创建依赖项
    try:
        if not db_config:
            raise ValueError("数据库配置未提供给工作进程")
            
        db_manager = create_sync_manager(db_config)
        alpha_data_tool = AlphaDataTool(db_manager)
        batch_loader = BatchDataLoader(alpha_data_tool)
    except Exception as e:
        logger.error(f"批次 {batch_idx + 1} 初始化失败: {e}")
        return {}


    # 1. 批量预加载数据
    try:
        all_stocks_data = batch_loader.load_stocks_data(
            stock_codes, start_date, end_date, use_cache=True, adjust=True
        )
        logger.info(f"批次 {batch_idx + 1} 数据加载完成")
    except Exception as e:
        logger.error(f"批次 {batch_idx + 1} 数据加载失败: {e}")
        return {}

    batch_results = {}

    # 2. 逐个运行回测
    for stock_code in stock_codes:
        stock_data = all_stocks_data.get(stock_code)
        if stock_data is None or stock_data.empty:
            logger.warning(f"股票 {stock_code} 无数据，跳过回测")
            continue

        try:
            cerebro = bt.Cerebro(stdstats=False)  # 禁用默认统计，使用我们的增强分析器

            # 添加预加载的数据
            data_feed = PostgreSQLDataFeed(
                ts_code=stock_code,
                preloaded_data=stock_data,
                start_date=start_date,
                end_date=end_date,
            )
            cerebro.adddata(data_feed)

            # 添加策略和分析器
            cerebro.addstrategy(strategy_class, **strategy_params)
            cerebro.addanalyzer(bt.analyzers.Sharpe, _name="sharpe")
            cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
            cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")

            # 设置初始资金和手续费
            cerebro.broker.setcash(args.get("initial_cash", 100000.0))
            cerebro.broker.setcommission(commission=args.get("commission", 0.001))

            # 运行回测
            results = cerebro.run()
            strategy_instance = results[0]

            # 提取结果
            final_value = cerebro.broker.getvalue()
            pnl = final_value - args.get("initial_cash", 100000.0)

            batch_results[stock_code] = {
                "final_value": final_value,
                "pnl": pnl,
                "sharpe_ratio": strategy_instance.analyzers.sharpe.get_analysis().get(
                    "sharperatio", 0.0
                ),
                "max_drawdown": strategy_instance.analyzers.drawdown.get_analysis().get(
                    "max", {}
                ).get("drawdown", 0.0),
                "total_return": strategy_instance.analyzers.returns.get_analysis().get(
                    "rtot", 0.0
                ),
            }

        except Exception as e:
            logger.error(f"股票 {stock_code} 回测失败: {e}")

    logger.info(f"批次 {batch_idx + 1} 完成处理")
    return batch_results
