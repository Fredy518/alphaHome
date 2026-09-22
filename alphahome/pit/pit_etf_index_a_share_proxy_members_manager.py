"""Manager for labelled A-share proxy rows in ETF-index PIT members."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Sequence

import pandas as pd

from .calculators.etf_index_a_share_proxy_members_calculator import (
    ETFIndexAShareProxyMembersCalculator,
)
from .pit_etf_index_members_manager import PITETFIndexMembersMonthlyManager


class PITETFIndexAShareProxyMembersMonthlyManager(
    PITETFIndexMembersMonthlyManager
):
    """Persist explicit A-share subsets for approved cross-market indices."""

    DEFAULT_PROXY_INDEX_CODES = ("931238.CSI",)

    def __init__(self) -> None:
        super().__init__()
        self.calculator = ETFIndexAShareProxyMembersCalculator()

    def _resolve_index_codes(
        self, index_codes: Sequence[str] | None
    ) -> list[str]:
        allowed = set(self.DEFAULT_PROXY_INDEX_CODES)
        if index_codes is None:
            return sorted(allowed)
        requested = {
            str(value).strip() for value in index_codes if str(value).strip()
        }
        unsupported = sorted(requested - allowed)
        if unsupported:
            raise ValueError(
                "未登记的跨市场A股子样本代理指数: "
                + ",".join(unsupported)
            )
        return sorted(requested)

    def _load_sources(
        self, obs_dates: list[date], index_codes: list[str]
    ) -> pd.DataFrame:
        min_obs = min(obs_dates)
        max_obs = max(obs_dates)
        names = self._load_index_names(index_codes)
        official = self.context.query_dataframe(
            """
            SELECT index_code, trade_date AS weight_trade_date,
                   con_code AS ts_code, weight AS raw_weight
            FROM rawdata.index_weight
            WHERE index_code = ANY(%s)
              AND trade_date BETWEEN %s AND %s
            ORDER BY index_code, trade_date, con_code
            """,
            (
                index_codes,
                min_obs
                - timedelta(
                    days=self.calculator.threshold.official_max_staleness_days
                ),
                max_obs,
            ),
        )
        if official is None or official.empty:
            official = pd.DataFrame(
                columns=[
                    "index_code",
                    "weight_trade_date",
                    "ts_code",
                    "raw_weight",
                ]
            )
        official["index_name"] = official["index_code"].map(names).fillna(
            official["index_code"]
        )
        return official

    def _run_months(
        self,
        target_months: list[date],
        *,
        batch_size: int | None,
        index_codes: Sequence[str] | None,
        result_key: str,
    ) -> dict[str, Any]:
        codes = self._resolve_index_codes(index_codes)
        if not target_months or not codes:
            return {
                result_key: 0,
                "processed_months": [],
                "processed_index_count": len(codes),
                "message": "无可处理的月份或已登记跨市场指数",
            }

        self._ensure_table_exists()
        month_batch = max(
            int(batch_size or self.DEFAULT_BACKFILL_BATCH_MONTHS), 1
        )
        total_rows = 0
        processed_months: list[str] = []
        batch_audits: list[dict[str, Any]] = []
        for offset in range(0, len(target_months), month_batch):
            months = target_months[offset : offset + month_batch]
            official = self._load_sources(months, codes)
            calculated = self.calculator.calculate(official, months, codes)
            self._validate_output(calculated, months, codes)
            inserted = self._atomic_replace_scope(calculated, months, codes)
            total_rows += inserted
            processed_months.extend(value.isoformat() for value in months)
            batch_audits.append(
                {
                    "months": [value.isoformat() for value in months],
                    **self.calculator.last_audit,
                }
            )
            self.logger.info(
                "跨市场指数A股代理成分月批次完成: %s ~ %s, indices=%s, rows=%s",
                min(months),
                max(months),
                len(codes),
                inserted,
            )

        self.stats["processed_records"] += total_rows
        self.stats["success_records"] += total_rows
        return {
            result_key: total_rows,
            "processed_months": processed_months,
            "processed_index_count": len(codes),
            "method_version": self.calculator.METHOD_VERSION,
            "constituent_scope": self.calculator.CONSTITUENT_SCOPE,
            "dependency_freshness": self._dependency_freshness(codes),
            "run_completed_at": datetime.now().astimezone().isoformat(),
            "batch_audits": batch_audits,
            "pit_semantics": (
                "validate the latest complete official cross-market index snapshot "
                "visible by obs_date, normalize security aliases, retain only SH/SZ "
                "members, and renormalize weights inside that A-share subset"
            ),
            "interpretation_limit": (
                "proxy fundamentals describe only the A-share subset; ETF returns "
                "and implementation still belong to the full tracked index"
            ),
        }

    def _atomic_replace_scope(
        self,
        frame: pd.DataFrame,
        obs_dates: Sequence[date],
        index_codes: Sequence[str],
    ) -> int:
        columns = self.calculator.OUTPUT_COLUMNS
        dates = sorted({pd.Timestamp(value).date() for value in obs_dates})
        codes = sorted({str(value) for value in index_codes})
        data = frame.reindex(columns=columns).copy()
        if not data.empty:
            data["obs_date"] = pd.to_datetime(data["obs_date"]).dt.date
            for column in ("source_effective_date", "source_available_date"):
                data[column] = pd.to_datetime(data[column]).dt.date

        return self._atomic_replace_months(
            data,
            dates,
            columns,
            ("obs_date", "index_code", "ts_code", "method_version"),
            scope_filters={
                "index_code": codes,
                "method_version": self.calculator.METHOD_VERSION,
            },
            required_scope_columns=("index_code",),
        )

    def _dependency_freshness(
        self, index_codes: Sequence[str]
    ) -> dict[str, Any]:
        frame = self.context.query_dataframe(
            """
            SELECT MAX(trade_date)::date AS index_weight_max_trade_date
            FROM rawdata.index_weight
            WHERE index_code = ANY(%s)
            """,
            (list(index_codes),),
        )
        return {} if frame is None or frame.empty else frame.iloc[0].to_dict()


__all__ = ["PITETFIndexAShareProxyMembersMonthlyManager"]
