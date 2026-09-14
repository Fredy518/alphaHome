"""I/O-free entrypoints around the frozen P v2.0 and G v1.1 formulas."""

from __future__ import annotations

from typing import Callable

import pandas as pd


def compute_p_snapshot(
    indicators: pd.DataFrame,
    industry: pd.DataFrame,
    as_of_date: str,
    formula: Callable[[pd.DataFrame, str, pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Apply the frozen P formula to caller-supplied data only."""
    return formula(indicators.copy(), as_of_date, industry.copy())


def compute_g_snapshot(
    p_history: pd.DataFrame,
    as_of_date: str,
    formula: Callable[[pd.DataFrame, str], pd.DataFrame],
) -> pd.DataFrame:
    """Apply the frozen G formula to caller-supplied legal-Friday P history."""
    return formula(p_history.copy(), as_of_date)


__all__ = ["compute_g_snapshot", "compute_p_snapshot"]
