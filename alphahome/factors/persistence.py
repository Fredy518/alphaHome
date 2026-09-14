"""Atomic, date-level persistence for factor snapshots."""

from __future__ import annotations

import csv
import hashlib
import io
from datetime import date
from typing import Any, Iterable, Mapping, Optional, Sequence
from uuid import UUID, uuid4

import pandas as pd

from .date_policy import FactorDatePolicy
from .governance import FactorGovernanceStore
from .validation import FactorValidationResult, validate_factor_frame


P_FACTOR_COLUMNS: Sequence[str] = (
    "ts_code",
    "calc_date",
    "ann_date",
    "end_date",
    "data_source",
    "p_score",
    "p_rank",
    "gpa",
    "roe_excl",
    "roa_excl",
    "net_margin_ttm",
    "operating_margin_ttm",
    "roi_ttm",
    "asset_turnover_ttm",
    "equity_multiplier",
    "debt_to_asset_ratio",
    "equity_ratio",
    "revenue_yoy_growth",
    "n_income_yoy_growth",
    "operate_profit_yoy_growth",
    "data_quality",
    "calculation_status",
)

G_FACTOR_COLUMNS: Sequence[str] = (
    "ts_code",
    "calc_date",
    "ann_date",
    "data_source",
    "g_efficiency_surprise",
    "g_efficiency_momentum",
    "g_revenue_momentum",
    "g_profit_momentum",
    "rank_es",
    "rank_em",
    "rank_rm",
    "rank_pm",
    "g_score",
    "data_timeliness_weight",
    "calculation_status",
)

_P_SIX_DECIMAL = {"p_score"}
_P_FOUR_DECIMAL = {
    "gpa",
    "roe_excl",
    "roa_excl",
    "net_margin_ttm",
    "operating_margin_ttm",
    "roi_ttm",
    "asset_turnover_ttm",
    "equity_multiplier",
    "debt_to_asset_ratio",
    "equity_ratio",
    "revenue_yoy_growth",
    "n_income_yoy_growth",
    "operate_profit_yoy_growth",
}
_G_SIX_DECIMAL = set(G_FACTOR_COLUMNS) - {
    "ts_code",
    "calc_date",
    "ann_date",
    "data_source",
    "calculation_status",
}


def factor_frame_checksum(
    frame: pd.DataFrame, columns: Optional[Iterable[str]] = None
) -> str:
    selected = list(columns or frame.columns)
    canonical = frame[selected].copy().sort_values(["ts_code", "calc_date"])
    for column in canonical.columns:
        if pd.api.types.is_datetime64_any_dtype(canonical[column]):
            canonical[column] = canonical[column].dt.strftime("%Y-%m-%d")
        elif column in _P_SIX_DECIMAL or column in _G_SIX_DECIMAL:
            canonical[column] = pd.to_numeric(canonical[column], errors="coerce").round(
                6
            )
        elif column in _P_FOUR_DECIMAL:
            canonical[column] = pd.to_numeric(canonical[column], errors="coerce").round(
                4
            )
        elif column == "p_rank":
            canonical[column] = pd.to_numeric(
                canonical[column], errors="coerce"
            ).astype("Int64")
    payload = canonical.to_csv(index=False, na_rep="<NULL>", lineterminator="\n")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class FactorSnapshotWriter:
    """Replace one complete factor date through a validated staging table."""

    def __init__(self, db_manager: Any):
        if not hasattr(db_manager, "_get_sync_connection"):
            raise TypeError("FactorSnapshotWriter需要同步DBManager")
        self.db_manager = db_manager

    def write(
        self,
        frame: pd.DataFrame,
        factor_type: str,
        calc_date: date | str,
        *,
        expected_codes: Optional[Iterable[str]] = None,
        run_id: Optional[UUID | str] = None,
        task_name: Optional[str] = None,
        input_count: Optional[int] = None,
        duration_ms: Optional[int] = None,
        details: Optional[Mapping[str, Any]] = None,
    ) -> tuple[int, str, FactorValidationResult]:
        factor_type = factor_type.lower()
        target_date = FactorDatePolicy.require_valid(calc_date)
        validation = validate_factor_frame(
            frame, factor_type, target_date, expected_codes=expected_codes
        )
        table = "p_factor" if factor_type == "p" else "g_factor"
        columns = P_FACTOR_COLUMNS if factor_type == "p" else G_FACTOR_COLUMNS
        missing = sorted(set(columns) - set(frame.columns))
        if missing:
            raise ValueError(f"{table}写入缺少字段: {missing}")
        prepared = frame[list(columns)].copy()
        checksum = factor_frame_checksum(prepared, columns)
        stage = f"factor_stage_{uuid4().hex}"
        quoted_columns = ", ".join(f'"{column}"' for column in columns)
        copy_buffer = io.StringIO()
        prepared.to_csv(
            copy_buffer,
            index=False,
            header=False,
            na_rep="\\N",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        copy_buffer.seek(0)

        connection = self.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                    (task_name or f"factor_{factor_type}", target_date.isoformat()),
                )
                cursor.execute(
                    f"CREATE TEMP TABLE {stage} "
                    f"(LIKE factors.{table} INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                cursor.copy_expert(
                    f"COPY {stage} ({quoted_columns}) FROM STDIN "
                    "WITH (FORMAT CSV, NULL '\\N')",
                    copy_buffer,
                )
                cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT ts_code) FROM {stage}")
                row_count, distinct_codes = cursor.fetchone()
                if row_count != len(prepared) or distinct_codes != len(prepared):
                    raise ValueError("staging行数或股票代码唯一性校验失败")
                cursor.execute(
                    f"SELECT COUNT(*) FROM {stage} WHERE calc_date <> %s OR ann_date > calc_date",
                    (target_date,),
                )
                if cursor.fetchone()[0]:
                    raise ValueError("staging存在日期或PIT边界违规")
                cursor.execute(
                    f"DELETE FROM factors.{table} WHERE calc_date = %s", (target_date,)
                )
                cursor.execute(
                    f"INSERT INTO factors.{table} ({quoted_columns}) "
                    f"SELECT {quoted_columns} FROM {stage}"
                )
                if run_id is not None and task_name:
                    denominator = input_count
                    coverage = (
                        len(prepared) / denominator
                        if denominator is not None and denominator > 0
                        else None
                    )
                    FactorGovernanceStore.record_date_cursor(
                        cursor,
                        run_id,
                        task_name,
                        target_date,
                        "success",
                        input_count=denominator or len(prepared),
                        output_count=len(prepared),
                        coverage_rate=coverage,
                        output_checksum=checksum,
                        duration_ms=duration_ms,
                        is_current=True,
                        details=details,
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return len(prepared), checksum, validation

    def clear_expected_no_data(
        self,
        factor_type: str,
        calc_date: date | str,
        *,
        run_id: UUID | str,
        task_name: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> int:
        """Atomically remove a stale P date and record evidenced empty eligibility."""
        factor_type = factor_type.lower()
        if factor_type != "p":
            raise ValueError("expected_no_data清空仅适用于P因子")
        target_date = FactorDatePolicy.require_valid(calc_date)
        connection = self.db_manager._get_sync_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                    (task_name, target_date.isoformat()),
                )
                cursor.execute(
                    "DELETE FROM factors.p_factor WHERE calc_date = %s",
                    (target_date,),
                )
                deleted = int(cursor.rowcount)
                FactorGovernanceStore.record_date_cursor(
                    cursor,
                    run_id,
                    task_name,
                    target_date,
                    "expected_no_data",
                    input_count=0,
                    output_count=0,
                    coverage_rate=1.0,
                    is_current=True,
                    details={**dict(details or {}), "deleted_stale_rows": deleted},
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return deleted


__all__ = [
    "G_FACTOR_COLUMNS",
    "P_FACTOR_COLUMNS",
    "FactorSnapshotWriter",
    "factor_frame_checksum",
]
