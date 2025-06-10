#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
增强分析器 - Backtrader增强工具

提供更丰富和详细的回测结果分析功能。
"""

from typing import Any, Dict, List, Optional

import backtrader as bt
import numpy as np
import pandas as pd

from ...common.logging_utils import get_logger


class EnhancedAnalyzer(bt.Analyzer):
    """
    增强回测分析器

    提供比Backtrader默认分析器更详细的分析功能：
    - 详细的收益率分析
    - 风险指标计算
    - 交易统计分析
    - 资金曲线分析
    - 绩效评估指标
    """

    def __init__(self):
        self.logger = get_logger("enhanced_analyzer")

        # 存储数据
        self.daily_values = []
        self.daily_returns = []
        self.trade_records = []
        self.positions = []

        # 基础统计
        self.start_value = None
        self.end_value = None
        self.peak_value = None
        self.valley_value = None

    def start(self):
        """分析开始时的初始化"""
        self.start_value = self.strategy.broker.getvalue()
        self.peak_value = self.start_value
        self.valley_value = self.start_value
        self.logger.debug(f"分析开始，初始资金: {self.start_value:.2f}")

    def next(self):
        """每个交易日的分析"""
        current_value = self.strategy.broker.getvalue()
        current_date = self.strategy.datas[0].datetime.date(0)

        # 记录每日资金
        self.daily_values.append({"date": current_date, "value": current_value})

        # 计算日收益率
        if len(self.daily_values) > 1:
            prev_value = self.daily_values[-2]["value"]
            daily_return = (current_value / prev_value - 1) * 100
            self.daily_returns.append({"date": current_date, "return": daily_return})

        # 更新峰值和谷值
        if current_value > self.peak_value:
            self.peak_value = current_value
        if current_value < self.valley_value:
            self.valley_value = current_value

        # 记录持仓信息
        for data in self.strategy.datas:
            position = self.strategy.getposition(data)
            if position.size != 0:
                self.positions.append(
                    {
                        "date": current_date,
                        "data_name": data._name,
                        "size": position.size,
                        "price": position.price,
                        "value": position.size * data.close[0],
                    }
                )

    def notify_trade(self, trade):
        """交易完成时的通知"""
        if trade.isclosed:
            trade_record = {
                "open_date": bt.num2date(trade.dtopen).date(),
                "close_date": bt.num2date(trade.dtclose).date(),
                "duration": (
                    bt.num2date(trade.dtclose) - bt.num2date(trade.dtopen)
                ).days,
                "size": trade.size,
                "open_price": trade.price,
                "close_price": trade.price + trade.pnl / trade.size,
                "pnl": trade.pnl,
                "pnl_comm": trade.pnlcomm,
                "commission": trade.commission,
                "return_pct": (
                    (trade.pnl / (abs(trade.size) * trade.price)) * 100
                    if trade.price > 0
                    else 0
                ),
            }
            self.trade_records.append(trade_record)

    def stop(self):
        """分析结束时的处理"""
        self.end_value = self.strategy.broker.getvalue()
        self.logger.debug(f"分析结束，最终资金: {self.end_value:.2f}")

    def get_analysis(self) -> Dict[str, Any]:
        """获取完整的分析结果"""
        analysis = {}

        # 基础统计
        analysis["basic"] = self._calculate_basic_stats()

        # 收益率分析
        analysis["returns"] = self._calculate_return_stats()

        # 风险指标
        analysis["risk"] = self._calculate_risk_metrics()

        # 交易分析
        analysis["trades"] = self._calculate_trade_stats()

        # 资金曲线分析
        analysis["equity_curve"] = self._calculate_equity_curve_stats()

        # 绩效指标
        analysis["performance"] = self._calculate_performance_metrics()

        return analysis

    def _calculate_basic_stats(self) -> Dict[str, Any]:
        """计算基础统计信息"""
        total_return = (
            ((self.end_value / self.start_value) - 1) * 100
            if self.start_value > 0
            else 0
        )

        return {
            "start_value": self.start_value,
            "end_value": self.end_value,
            "peak_value": self.peak_value,
            "valley_value": self.valley_value,
            "total_return": total_return,
            "absolute_gain": self.end_value - self.start_value,
            "trading_days": len(self.daily_values),
        }

    def _calculate_return_stats(self) -> Dict[str, Any]:
        """计算收益率统计"""
        if not self.daily_returns:
            return {}

        returns = [r["return"] for r in self.daily_returns]

        return {
            "avg_daily_return": np.mean(returns),
            "std_daily_return": np.std(returns),
            "min_daily_return": np.min(returns),
            "max_daily_return": np.max(returns),
            "positive_days": sum(1 for r in returns if r > 0),
            "negative_days": sum(1 for r in returns if r < 0),
            "win_rate": (
                (sum(1 for r in returns if r > 0) / len(returns)) * 100
                if returns
                else 0
            ),
            "annualized_return": np.mean(returns) * 252,  # 假设252个交易日
            "annualized_volatility": np.std(returns) * np.sqrt(252),
        }

    def _calculate_risk_metrics(self) -> Dict[str, Any]:
        """计算风险指标"""
        if not self.daily_returns or not self.daily_values:
            return {}

        returns = [r["return"] for r in self.daily_returns]
        values = [v["value"] for v in self.daily_values]

        # 最大回撤
        max_drawdown, max_dd_duration = self._calculate_max_drawdown(values)

        # 夏普比率 (假设无风险利率为3%)
        risk_free_rate = 3.0 / 252  # 日化无风险利率
        excess_returns = [r - risk_free_rate for r in [r / 100 for r in returns]]
        sharpe_ratio = (
            (np.mean(excess_returns) / np.std(excess_returns)) * np.sqrt(252)
            if np.std(excess_returns) > 0
            else 0
        )

        # 索提诺比率
        negative_returns = [r for r in returns if r < 0]
        downside_std = np.std(negative_returns) if negative_returns else 0
        sortino_ratio = (
            (np.mean(returns) / downside_std) * np.sqrt(252) if downside_std > 0 else 0
        )

        # VaR和CVaR (5%水平)
        var_5 = np.percentile(returns, 5) if returns else 0
        cvar_5 = (
            np.mean([r for r in returns if r <= var_5]) if returns and var_5 != 0 else 0
        )

        return {
            "max_drawdown": max_drawdown,
            "max_drawdown_duration": max_dd_duration,
            "sharpe_ratio": sharpe_ratio,
            "sortino_ratio": sortino_ratio,
            "var_5": var_5,
            "cvar_5": cvar_5,
            "calmar_ratio": (
                (np.mean(returns) * 252 / abs(max_drawdown)) if max_drawdown != 0 else 0
            ),
        }

    def _calculate_max_drawdown(self, values: List[float]) -> tuple:
        """计算最大回撤和持续时间"""
        if not values:
            return 0, 0

        peak = values[0]
        max_dd = 0
        max_dd_duration = 0
        current_dd_duration = 0

        for value in values:
            if value > peak:
                peak = value
                current_dd_duration = 0
            else:
                drawdown = (peak - value) / peak * 100
                max_dd = max(max_dd, drawdown)
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)

        return max_dd, max_dd_duration

    def _calculate_trade_stats(self) -> Dict[str, Any]:
        """计算交易统计"""
        if not self.trade_records:
            return {}

        winning_trades = [t for t in self.trade_records if t["pnl"] > 0]
        losing_trades = [t for t in self.trade_records if t["pnl"] < 0]

        total_trades = len(self.trade_records)
        win_count = len(winning_trades)
        loss_count = len(losing_trades)

        # 盈亏统计
        total_profit = sum(t["pnl"] for t in winning_trades)
        total_loss = sum(t["pnl"] for t in losing_trades)

        # 平均持仓时间
        avg_duration = np.mean([t["duration"] for t in self.trade_records])

        # 最大连续盈亏
        max_consecutive_wins, max_consecutive_losses = (
            self._calculate_consecutive_trades()
        )

        return {
            "total_trades": total_trades,
            "winning_trades": win_count,
            "losing_trades": loss_count,
            "win_rate": (win_count / total_trades) * 100 if total_trades > 0 else 0,
            "avg_win": total_profit / win_count if win_count > 0 else 0,
            "avg_loss": total_loss / loss_count if loss_count > 0 else 0,
            "profit_factor": (
                abs(total_profit / total_loss) if total_loss != 0 else float("inf")
            ),
            "avg_duration": avg_duration,
            "max_consecutive_wins": max_consecutive_wins,
            "max_consecutive_losses": max_consecutive_losses,
            "largest_win": (
                max([t["pnl"] for t in self.trade_records]) if self.trade_records else 0
            ),
            "largest_loss": (
                min([t["pnl"] for t in self.trade_records]) if self.trade_records else 0
            ),
        }

    def _calculate_consecutive_trades(self) -> tuple:
        """计算最大连续盈亏次数"""
        if not self.trade_records:
            return 0, 0

        max_wins = 0
        max_losses = 0
        current_wins = 0
        current_losses = 0

        for trade in self.trade_records:
            if trade["pnl"] > 0:
                current_wins += 1
                current_losses = 0
                max_wins = max(max_wins, current_wins)
            else:
                current_losses += 1
                current_wins = 0
                max_losses = max(max_losses, current_losses)

        return max_wins, max_losses

    def _calculate_equity_curve_stats(self) -> Dict[str, Any]:
        """计算资金曲线统计"""
        if not self.daily_values:
            return {}

        values = [v["value"] for v in self.daily_values]

        # 计算移动平均
        if len(values) >= 20:
            ma_20 = np.mean(values[-20:])
        else:
            ma_20 = np.mean(values)

        # 计算趋势 (线性回归斜率)
        if len(values) > 1:
            x = np.arange(len(values))
            slope = np.polyfit(x, values, 1)[0]
        else:
            slope = 0

        return {
            "current_ma_20": ma_20,
            "trend_slope": slope,
            "above_ma_days": (
                sum(1 for v in values[-20:] if v > ma_20) if len(values) >= 20 else 0
            ),
            "volatility": np.std(values) / np.mean(values) * 100 if values else 0,
        }

    def _calculate_performance_metrics(self) -> Dict[str, Any]:
        """计算综合绩效指标"""
        basic = self._calculate_basic_stats()
        returns = self._calculate_return_stats()
        risk = self._calculate_risk_metrics()
        trades = self._calculate_trade_stats()

        # Kelly比例
        win_rate = trades.get("win_rate", 0) / 100
        avg_win = trades.get("avg_win", 0)
        avg_loss = trades.get("avg_loss", 0)

        if avg_loss != 0:
            win_loss_ratio = abs(avg_win / avg_loss)
            kelly_fraction = win_rate - (1 - win_rate) / win_loss_ratio
        else:
            kelly_fraction = 0

        # 综合评分 (0-100)
        score_components = {
            "return_score": min(
                max(basic.get("total_return", 0) / 50 * 25, 0), 25
            ),  # 最高25分
            "risk_score": min(
                max((100 - risk.get("max_drawdown", 100)) / 100 * 25, 0), 25
            ),  # 最高25分
            "consistency_score": min(
                max(trades.get("win_rate", 0) / 100 * 25, 0), 25
            ),  # 最高25分
            "efficiency_score": min(
                max(risk.get("sharpe_ratio", 0) / 2 * 25, 0), 25
            ),  # 最高25分
        }

        overall_score = sum(score_components.values())

        return {
            "kelly_fraction": kelly_fraction,
            "score_components": score_components,
            "overall_score": overall_score,
            "grade": self._get_performance_grade(overall_score),
        }

    def _get_performance_grade(self, score: float) -> str:
        """根据评分获取等级"""
        if score >= 90:
            return "A+"
        elif score >= 80:
            return "A"
        elif score >= 70:
            return "B+"
        elif score >= 60:
            return "B"
        elif score >= 50:
            return "C+"
        elif score >= 40:
            return "C"
        else:
            return "D"

    def to_dataframe(self) -> Dict[str, pd.DataFrame]:
        """将分析结果转换为DataFrame格式"""
        result = {}

        # 日收益率DataFrame
        if self.daily_returns:
            result["daily_returns"] = pd.DataFrame(self.daily_returns)
            result["daily_returns"]["date"] = pd.to_datetime(
                result["daily_returns"]["date"]
            )
            result["daily_returns"].set_index("date", inplace=True)

        # 资金曲线DataFrame
        if self.daily_values:
            result["equity_curve"] = pd.DataFrame(self.daily_values)
            result["equity_curve"]["date"] = pd.to_datetime(
                result["equity_curve"]["date"]
            )
            result["equity_curve"].set_index("date", inplace=True)

        # 交易记录DataFrame
        if self.trade_records:
            result["trades"] = pd.DataFrame(self.trade_records)
            result["trades"]["open_date"] = pd.to_datetime(
                result["trades"]["open_date"]
            )
            result["trades"]["close_date"] = pd.to_datetime(
                result["trades"]["close_date"]
            )

        return result
