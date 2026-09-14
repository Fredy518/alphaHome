"""Validation gates applied before a production factor snapshot is replaced."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional, Set

import pandas as pd

from .date_policy import FactorDatePolicy


class FactorValidationError(ValueError):
    """Raised when a complete factor date cannot be safely persisted."""


@dataclass(frozen=True)
class FactorValidationResult:
    factor_type: str
    calc_date: date
    row_count: int
    expected_count: Optional[int]

    @property
    def coverage_rate(self) -> Optional[float]:
        if self.expected_count is None:
            return None
        if self.expected_count == 0:
            return 1.0 if self.row_count == 0 else 0.0
        return self.row_count / self.expected_count


def validate_factor_frame(
    frame: pd.DataFrame,
    factor_type: str,
    calc_date: date | str,
    expected_codes: Optional[Iterable[str]] = None,
) -> FactorValidationResult:
    """Validate keys, PIT dates, bounds and the task's eligible universe."""
    factor_type = factor_type.lower()
    if factor_type not in {"p", "g"}:
        raise FactorValidationError(f"未知因子类型: {factor_type}")
    target_date = FactorDatePolicy.require_valid(calc_date)
    required = {"ts_code", "calc_date", "ann_date", "calculation_status"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise FactorValidationError(f"因子结果缺少字段: {missing}")
    if frame.empty:
        raise FactorValidationError(f"{factor_type.upper()}因子结果为空: {target_date}")

    if frame[["ts_code", "calc_date"]].isnull().any().any():
        raise FactorValidationError("因子结果存在空主键")
    if frame.duplicated(["ts_code", "calc_date"]).any():
        raise FactorValidationError("因子结果存在重复(ts_code, calc_date)")

    calc_dates = pd.to_datetime(frame["calc_date"], errors="coerce").dt.date
    if calc_dates.isnull().any() or set(calc_dates) != {target_date}:
        raise FactorValidationError("结果calc_date与目标日期不一致")
    ann_dates = pd.to_datetime(frame["ann_date"], errors="coerce").dt.date
    if ann_dates.isnull().any() or (ann_dates > target_date).any():
        raise FactorValidationError("结果存在空ann_date或ann_date晚于calc_date")

    if not (frame["calculation_status"].astype(str) == "success").all():
        raise FactorValidationError("结果包含非success计算状态")

    if factor_type == "p":
        _require_range(frame, "p_score", 0.0, 100.0)
        if "p_rank" not in frame or frame["p_rank"].isnull().any():
            raise FactorValidationError("P因子缺少有效p_rank")
        if (pd.to_numeric(frame["p_rank"], errors="coerce") < 1).any():
            raise FactorValidationError("P因子p_rank必须大于等于1")
    else:
        _require_range(frame, "g_score", 0.0, 100.0)
        for column in ("rank_es", "rank_em", "rank_rm", "rank_pm"):
            _require_range(frame, column, 0.0, 100.0, allow_null=True)

    expected: Optional[Set[str]] = None
    if expected_codes is not None:
        expected = {str(code) for code in expected_codes if code is not None}
        actual = set(frame["ts_code"].astype(str))
        if actual != expected:
            missing_codes = sorted(expected - actual)[:10]
            extra_codes = sorted(actual - expected)[:10]
            raise FactorValidationError(
                "结果资格集合不完整: "
                f"expected={len(expected)}, actual={len(actual)}, "
                f"missing={missing_codes}, extra={extra_codes}"
            )

    return FactorValidationResult(
        factor_type=factor_type,
        calc_date=target_date,
        row_count=len(frame),
        expected_count=len(expected) if expected is not None else None,
    )


def _require_range(
    frame: pd.DataFrame,
    column: str,
    lower: float,
    upper: float,
    *,
    allow_null: bool = False,
) -> None:
    if column not in frame:
        raise FactorValidationError(f"因子结果缺少字段: {column}")
    values = pd.to_numeric(frame[column], errors="coerce")
    if not allow_null and values.isnull().any():
        raise FactorValidationError(f"{column}存在空值")
    valid = values.dropna()
    if ((valid < lower) | (valid > upper)).any():
        raise FactorValidationError(f"{column}超出[{lower}, {upper}]范围")


__all__ = [
    "FactorValidationError",
    "FactorValidationResult",
    "validate_factor_frame",
]
