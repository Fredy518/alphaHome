#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ._schema import (
    DEFAULT_PERIODS_PER_YEAR,
    DEFAULT_RISK_FREE_RATE,
    DRAWDOWN_SCHEMA,
    FFILL_LIMIT,
    METRICS_SCHEMA,
    METRICS_SCHEMA_KEYS,
    PERIODIC_SCHEMA,
)


_TEST_PKG_DIR = Path(__file__).resolve().parents[1] / "tests" / "unit" / "fund_analysis"
if _TEST_PKG_DIR.exists():
    __path__ = list(globals().get("__path__", []))
    if str(_TEST_PKG_DIR) not in __path__:
        __path__.append(str(_TEST_PKG_DIR))


def _as_series(values: Optional[pd.Series]) -> pd.Series:
    if values is None:
        return pd.Series(dtype=float)
    if isinstance(values, pd.Series):
        return pd.to_numeric(values, errors="coerce").dropna()
    return pd.Series(values, dtype=float).dropna()


def _align_pair(left: pd.Series, right: pd.Series) -> tuple[pd.Series, pd.Series]:
    left_s = _as_series(left)
    right_s = _as_series(right)
    if left_s.empty or right_s.empty:
        return left_s.iloc[0:0], right_s.iloc[0:0]
    joined = pd.concat([left_s, right_s], axis=1, join="inner").dropna()
    if joined.empty:
        return left_s.iloc[0:0], right_s.iloc[0:0]
    return joined.iloc[:, 0], joined.iloc[:, 1]


def _safe_float(value: Any) -> float:
    if value is None:
        return float("nan")
    try:
        return float(value)
    except Exception:
        return float("nan")


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_safe(v) for v in value]
    if isinstance(value, pd.DataFrame):
        frame = value.reset_index()
        return [_to_json_safe(record) for record in frame.to_dict(orient="records")]
    if isinstance(value, pd.Series):
        return {str(k): _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "to_dict") and callable(value.to_dict):
        try:
            return _to_json_safe(value.to_dict())
        except Exception:
            pass
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and np.isnan(value):
        return None
    return value


class MetricsAnalyzer:
    def __init__(
        self,
        periods_per_year: int = DEFAULT_PERIODS_PER_YEAR,
        risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    ) -> None:
        self.periods_per_year = int(periods_per_year)
        self.risk_free_rate = float(risk_free_rate)

    def cumulative_return(self, returns: pd.Series) -> float:
        series = _as_series(returns)
        if series.empty:
            return float("nan")
        return float((1.0 + series).prod() - 1.0)

    def annualized_return(self, returns: pd.Series) -> float:
        series = _as_series(returns)
        if series.empty:
            return float("nan")
        cumulative = (1.0 + series).prod()
        if cumulative <= 0:
            return float("nan")
        return float(cumulative ** (self.periods_per_year / len(series)) - 1.0)

    def annualized_volatility(self, returns: pd.Series) -> float:
        series = _as_series(returns)
        if len(series) < 2:
            return float("nan")
        std = series.std(ddof=1)
        if pd.isna(std) or np.isclose(std, 0.0):
            return float("nan")
        return float(std * np.sqrt(self.periods_per_year))

    def sharpe_ratio(self, returns: pd.Series) -> float:
        series = _as_series(returns)
        if len(series) < 2:
            return float("nan")
        rf_per_period = self.risk_free_rate / self.periods_per_year
        excess = series - rf_per_period
        std = excess.std(ddof=1)
        if pd.isna(std) or np.isclose(std, 0.0):
            return float("nan")
        return float(excess.mean() / std * np.sqrt(self.periods_per_year))

    def sortino_ratio(self, returns: pd.Series) -> float:
        series = _as_series(returns)
        if len(series) < 2:
            return float("nan")
        rf_per_period = self.risk_free_rate / self.periods_per_year
        excess = series - rf_per_period
        downside = excess[excess < 0]
        if downside.empty:
            return float("nan")
        downside_std = downside.std(ddof=1)
        if pd.isna(downside_std) or np.isclose(downside_std, 0.0):
            return float("nan")
        return float(excess.mean() / downside_std * np.sqrt(self.periods_per_year))

    def calmar_ratio(self, ann_return: float, max_dd: float) -> float:
        ann_val = _safe_float(ann_return)
        max_dd_val = _safe_float(max_dd)
        if np.isnan(ann_val) or np.isnan(max_dd_val) or max_dd_val <= 0:
            return float("nan")
        return float(ann_val / max_dd_val)

    def win_rate(self, returns: pd.Series) -> float:
        series = _as_series(returns)
        if series.empty:
            return float("nan")
        return float((series > 0).mean())

    def profit_loss_ratio(self, returns: pd.Series) -> float:
        series = _as_series(returns)
        if series.empty:
            return float("nan")
        wins = series[series > 0]
        losses = series[series < 0]
        if losses.empty:
            return float("inf") if not wins.empty else float("nan")
        if wins.empty:
            return 0.0
        return float(wins.mean() / abs(losses.mean()))

    def var(self, returns: pd.Series, confidence: float = 0.95) -> float:
        series = _as_series(returns)
        if series.empty:
            return float("nan")
        return float(series.quantile(1.0 - confidence))

    def cvar(self, returns: pd.Series, confidence: float = 0.95) -> float:
        series = _as_series(returns)
        if series.empty:
            return float("nan")
        threshold = self.var(series, confidence=confidence)
        if np.isnan(threshold):
            return float("nan")
        tail = series[series <= threshold]
        if tail.empty:
            return threshold
        return float(tail.mean())


@dataclass(frozen=True)
class DrawdownPeriod:
    drawdown: float
    peak_date: Optional[pd.Timestamp]
    trough_date: Optional[pd.Timestamp]
    recovery_date: Optional[pd.Timestamp]
    duration: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "drawdown": self.drawdown,
            "peak_date": None if self.peak_date is None else str(self.peak_date),
            "trough_date": None if self.trough_date is None else str(self.trough_date),
            "recovery_date": None if self.recovery_date is None else str(self.recovery_date),
            "duration": self.duration,
        }


class DrawdownAnalyzer:
    def underwater_curve(self, nav: pd.Series) -> pd.Series:
        series = _as_series(nav)
        if series.empty:
            return pd.Series(dtype=float)
        running_max = series.cummax()
        return series / running_max - 1.0

    def max_drawdown(
        self, nav: pd.Series
    ) -> tuple[float, Optional[pd.Timestamp], Optional[pd.Timestamp]]:
        series = _as_series(nav)
        if series.empty:
            return float("nan"), None, None
        underwater = self.underwater_curve(series)
        if underwater.empty:
            return float("nan"), None, None
        trough_date = underwater.idxmin()
        max_dd = abs(float(underwater.loc[trough_date]))
        history = series.loc[:trough_date]
        peak_date = history.idxmax() if not history.empty else None
        return max_dd, peak_date, trough_date

    def top_n_drawdowns(self, nav: pd.Series, n: int = 5) -> List[DrawdownPeriod]:
        series = _as_series(nav)
        if series.empty or n <= 0:
            return []

        remaining = series.copy()
        periods: List[DrawdownPeriod] = []
        for _ in range(n):
            max_dd, peak_date, trough_date = self.max_drawdown(remaining)
            if np.isnan(max_dd) or peak_date is None or trough_date is None or max_dd <= 0:
                break

            peak_value = remaining.loc[peak_date]
            recovery_candidates = remaining.loc[trough_date:]
            recovery_date = None
            for idx, value in recovery_candidates.items():
                if value >= peak_value:
                    recovery_date = idx
                    break

            duration = int((pd.Timestamp(trough_date) - pd.Timestamp(peak_date)).days)
            periods.append(
                DrawdownPeriod(
                    drawdown=max_dd,
                    peak_date=pd.Timestamp(peak_date),
                    trough_date=pd.Timestamp(trough_date),
                    recovery_date=None if recovery_date is None else pd.Timestamp(recovery_date),
                    duration=duration,
                )
            )

            cutoff = recovery_date if recovery_date is not None else trough_date
            remaining = remaining.drop(remaining.loc[peak_date:cutoff].index)
            if remaining.empty:
                break

        periods.sort(key=lambda item: item.drawdown, reverse=True)
        return periods

    def avg_drawdown_duration(self, nav: pd.Series) -> float:
        durations = [item.duration for item in self.top_n_drawdowns(nav, n=10) if item.duration is not None]
        if not durations:
            return float("nan")
        return float(np.mean(durations))

    def max_drawdown_duration(self, nav: pd.Series) -> int:
        durations = [item.duration for item in self.top_n_drawdowns(nav, n=10) if item.duration is not None]
        if not durations:
            return 0
        return int(max(durations))


class PeriodicAnalyzer:
    def __init__(
        self,
        periods_per_year: int = DEFAULT_PERIODS_PER_YEAR,
        risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    ) -> None:
        self.metrics_analyzer = MetricsAnalyzer(periods_per_year, risk_free_rate)
        self.periods_per_year = periods_per_year

    def monthly_returns(self, nav: pd.Series) -> pd.DataFrame:
        series = _as_series(nav)
        if series.empty:
            return pd.DataFrame()
        month_end = series.resample("ME").last()
        monthly = month_end.pct_change().dropna()
        if monthly.empty:
            return pd.DataFrame()
        frame = pd.DataFrame(
            {"year": monthly.index.year, "month": monthly.index.month, "return": monthly.values}
        )
        result = frame.pivot(index="year", columns="month", values="return").sort_index()
        result.columns = [int(col) for col in result.columns]
        return result

    def quarterly_returns(self, nav: pd.Series) -> pd.Series:
        series = _as_series(nav)
        if series.empty:
            return pd.Series(dtype=float)
        quarter_end = series.resample("QE").last()
        returns = quarter_end.pct_change().dropna()
        if returns.empty:
            return pd.Series(dtype=float)
        labels = [f"{idx.year}-Q{idx.quarter}" for idx in returns.index]
        return pd.Series(returns.values, index=labels, dtype=float)

    def yearly_returns(self, nav: pd.Series) -> pd.Series:
        series = _as_series(nav)
        if series.empty:
            return pd.Series(dtype=float)
        year_end = series.resample("YE").last()
        returns = year_end.pct_change().dropna()
        if returns.empty:
            return pd.Series(dtype=float)
        return pd.Series(returns.values, index=returns.index.year.astype(int), dtype=float)

    def rolling_return(self, returns: pd.Series, window: int = 60) -> pd.Series:
        series = _as_series(returns)
        if series.empty:
            return pd.Series(dtype=float)
        return (1.0 + series).rolling(window=window, min_periods=window).apply(np.prod, raw=True) - 1.0

    def rolling_sharpe(self, returns: pd.Series, window: int = 60) -> pd.Series:
        series = _as_series(returns)
        if series.empty:
            return pd.Series(dtype=float)
        rf_per_period = self.metrics_analyzer.risk_free_rate / self.periods_per_year
        excess = series - rf_per_period
        mean = excess.rolling(window=window, min_periods=window).mean()
        std = excess.rolling(window=window, min_periods=window).std(ddof=1).replace(0.0, np.nan)
        return mean / std * np.sqrt(self.periods_per_year)

    def rolling_volatility(self, returns: pd.Series, window: int = 30) -> pd.Series:
        series = _as_series(returns)
        if series.empty:
            return pd.Series(dtype=float)
        return series.rolling(window=window, min_periods=window).std(ddof=1) * np.sqrt(self.periods_per_year)


class RiskAnalyzer:
    def __init__(self, periods_per_year: int = DEFAULT_PERIODS_PER_YEAR) -> None:
        self.periods_per_year = int(periods_per_year)

    def tracking_error(self, returns: pd.Series, benchmark_returns: pd.Series) -> float:
        left, right = _align_pair(returns, benchmark_returns)
        if len(left) < 2:
            return float("nan")
        diff = left - right
        std = diff.std(ddof=1)
        if pd.isna(std):
            return float("nan")
        return float(std * np.sqrt(self.periods_per_year))

    def beta(self, returns: pd.Series, benchmark_returns: pd.Series) -> float:
        left, right = _align_pair(returns, benchmark_returns)
        if len(left) < 2:
            return float("nan")
        variance = np.var(right, ddof=1)
        if np.isclose(variance, 0.0):
            return float("nan")
        covariance = np.cov(left, right, ddof=1)[0, 1]
        return float(covariance / variance)

    def hhi(self, weights: pd.Series) -> float:
        series = _as_series(weights).abs()
        total = series.sum()
        if series.empty or np.isclose(total, 0.0):
            return float("nan")
        normalized = series / total
        return float((normalized ** 2).sum())

    def top_n_concentration(self, weights: pd.Series, n: int = 3) -> float:
        series = _as_series(weights).abs().sort_values(ascending=False)
        total = series.sum()
        if series.empty or np.isclose(total, 0.0):
            return float("nan")
        return float(series.head(n).sum() / total)


class AttributionAnalyzer:
    def contribution(self, weights: pd.Series, returns: pd.Series) -> pd.Series:
        weights_s, returns_s = _align_pair(weights, returns)
        if weights_s.empty:
            return pd.Series(dtype=float)
        return weights_s * returns_s


class ReportBuilder:
    def summary_dataframe(self, metrics: Dict[str, Any]) -> pd.DataFrame:
        return pd.DataFrame([metrics])

    def to_dict(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        return _to_json_safe(report_data)


class PerformanceAnalyzer:
    def __init__(
        self,
        periods_per_year: int = DEFAULT_PERIODS_PER_YEAR,
        risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
    ) -> None:
        self.periods_per_year = int(periods_per_year)
        self.risk_free_rate = float(risk_free_rate)
        self.metrics = MetricsAnalyzer(periods_per_year, risk_free_rate)
        self.drawdowns = DrawdownAnalyzer()
        self.periodic = PeriodicAnalyzer(periods_per_year, risk_free_rate)
        self.risk = RiskAnalyzer(periods_per_year)
        self.report_builder = ReportBuilder()

    def _normalize_nav(self, nav_series: pd.Series) -> pd.Series:
        nav = _as_series(nav_series)
        if nav.empty:
            return nav
        return nav.ffill(limit=FFILL_LIMIT).dropna()

    def _normalize_returns(
        self, nav_series: pd.Series, returns: Optional[pd.Series] = None
    ) -> pd.Series:
        if returns is not None:
            return _as_series(returns)
        nav = self._normalize_nav(nav_series)
        return nav.pct_change().dropna()

    def calculate_metrics(
        self,
        returns: Optional[pd.Series],
        nav_series: pd.Series,
        benchmark: Optional[pd.Series] = None,
    ) -> Dict[str, Any]:
        nav = self._normalize_nav(nav_series)
        normalized_returns = self._normalize_returns(nav, returns)
        benchmark_nav = None if benchmark is None else self._normalize_nav(benchmark)
        benchmark_returns = None if benchmark_nav is None else benchmark_nav.pct_change().dropna()

        max_dd, _, _ = self.drawdowns.max_drawdown(nav)
        cumulative = self.metrics.cumulative_return(normalized_returns)
        annualized = self.metrics.annualized_return(normalized_returns)
        tracking_error = (
            self.risk.tracking_error(normalized_returns, benchmark_returns)
            if benchmark_returns is not None
            else float("nan")
        )
        beta = (
            self.risk.beta(normalized_returns, benchmark_returns)
            if benchmark_returns is not None
            else float("nan")
        )
        excess_return = (
            self.metrics.annualized_return(normalized_returns) - self.metrics.annualized_return(benchmark_returns)
            if benchmark_returns is not None and not benchmark_returns.empty
            else float("nan")
        )
        information_ratio = (
            excess_return / tracking_error
            if tracking_error and not np.isnan(tracking_error) and not np.isclose(tracking_error, 0.0)
            else float("nan")
        )

        result = {
            "cumulative_return": cumulative,
            "annualized_return": annualized,
            "annualized_volatility": self.metrics.annualized_volatility(normalized_returns),
            "sharpe_ratio": self.metrics.sharpe_ratio(normalized_returns),
            "sortino_ratio": self.metrics.sortino_ratio(normalized_returns),
            "calmar_ratio": self.metrics.calmar_ratio(annualized, max_dd),
            "win_rate": self.metrics.win_rate(normalized_returns),
            "profit_loss_ratio": self.metrics.profit_loss_ratio(normalized_returns),
            "var_95": self.metrics.var(normalized_returns, confidence=0.95),
            "cvar_95": self.metrics.cvar(normalized_returns, confidence=0.95),
            "max_drawdown": max_dd,
            "information_ratio": information_ratio,
            "tracking_error": tracking_error,
            "beta": beta,
            "excess_return": excess_return,
            "total_days": int(len(normalized_returns)),
        }
        return {key: result.get(key, float("nan")) for key in METRICS_SCHEMA_KEYS}

    def analyze_drawdowns(self, nav_series: pd.Series) -> Dict[str, Any]:
        nav = self._normalize_nav(nav_series)
        max_dd, _, _ = self.drawdowns.max_drawdown(nav)
        return {
            "max_drawdown": max_dd,
            "top_n_drawdowns": self.drawdowns.top_n_drawdowns(nav, n=5),
            "underwater_curve": self.drawdowns.underwater_curve(nav),
        }

    def calculate_periodic_returns(self, nav_series: pd.Series) -> Dict[str, Any]:
        nav = self._normalize_nav(nav_series)
        return {
            "monthly_returns": self.periodic.monthly_returns(nav),
            "quarterly_returns": self.periodic.quarterly_returns(nav),
            "yearly_returns": self.periodic.yearly_returns(nav),
        }

    def calculate_rolling_metrics(
        self,
        returns: pd.Series,
        return_window: int = 60,
        volatility_window: int = 30,
    ) -> Dict[str, Any]:
        normalized_returns = _as_series(returns)
        return {
            "rolling_return": self.periodic.rolling_return(normalized_returns, window=return_window),
            "rolling_sharpe": self.periodic.rolling_sharpe(normalized_returns, window=return_window),
            "rolling_volatility": self.periodic.rolling_volatility(normalized_returns, window=volatility_window),
        }

    def generate_report(
        self,
        nav_series: pd.Series,
        returns: Optional[pd.Series] = None,
        benchmark: Optional[pd.Series] = None,
        format: str = "dict",
    ) -> Dict[str, Any]:
        nav = self._normalize_nav(nav_series)
        normalized_returns = self._normalize_returns(nav, returns)
        report = {
            "metrics": self.calculate_metrics(normalized_returns, nav, benchmark=benchmark),
            "drawdowns": self.analyze_drawdowns(nav),
            "periodic": self.calculate_periodic_returns(nav),
            "rolling": self.calculate_rolling_metrics(normalized_returns),
        }
        if format == "dict":
            return self.report_builder.to_dict(report)
        return report

    def to_dict(
        self,
        nav_series: pd.Series,
        returns: Optional[pd.Series] = None,
        benchmark: Optional[pd.Series] = None,
    ) -> Dict[str, Any]:
        return self.generate_report(nav_series, returns=returns, benchmark=benchmark, format="dict")


__all__ = [
    "AttributionAnalyzer",
    "DEFAULT_PERIODS_PER_YEAR",
    "DEFAULT_RISK_FREE_RATE",
    "DRAWDOWN_SCHEMA",
    "DrawdownAnalyzer",
    "DrawdownPeriod",
    "FFILL_LIMIT",
    "METRICS_SCHEMA",
    "MetricsAnalyzer",
    "PERIODIC_SCHEMA",
    "PeriodicAnalyzer",
    "PerformanceAnalyzer",
    "ReportBuilder",
    "RiskAnalyzer",
]
