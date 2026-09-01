"""Helpers for canonical stock identifiers in Tushare financial statements."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


_TUSHARE_FINANCIAL_ALIAS_RE = re.compile(
    r"^(?P<symbol>\d{6})!\d+(?P<exchange>\.(?:SH|SZ|BJ))$",
    re.IGNORECASE,
)


def normalize_tushare_financial_ts_code(value: Any) -> Any:
    """Collapse a Tushare ``000001!1.SZ`` alias to a canonical TS code.

    The raw Tushare layer remains source-faithful. PIT financial panels use a
    canonical security identity, so an internal numeric variant marker before
    the exchange suffix must not create a second stock.
    """

    if not isinstance(value, str):
        return value

    match = _TUSHARE_FINANCIAL_ALIAS_RE.fullmatch(value.strip())
    if match is None:
        return value
    return f"{match.group('symbol')}{match.group('exchange').upper()}"


def normalize_tushare_financial_ts_codes(
    data: pd.DataFrame,
    logger: Any = None,
) -> pd.DataFrame:
    """Return a copy with Tushare financial aliases canonicalized."""

    if data is None or data.empty or "ts_code" not in data.columns:
        return data

    work = data.copy()
    original = work["ts_code"].copy()
    work["ts_code"] = original.map(normalize_tushare_financial_ts_code)

    changed = original.notna() & work["ts_code"].notna() & original.ne(work["ts_code"])
    changed_count = int(changed.sum())
    if changed_count and logger is not None:
        examples = (
            pd.DataFrame(
                {
                    "source": original.loc[changed].astype(str),
                    "canonical": work.loc[changed, "ts_code"].astype(str),
                }
            )
            .drop_duplicates()
            .head(3)
        )
        sample = ", ".join(
            f"{row.source}->{row.canonical}" for row in examples.itertuples(index=False)
        )
        logger.warning(
            f"规范化 Tushare 财务代码别名 {changed_count} 条（{sample}）"
        )

    return work


__all__ = [
    "normalize_tushare_financial_ts_code",
    "normalize_tushare_financial_ts_codes",
]
