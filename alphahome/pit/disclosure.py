"""Public financial disclosure events; receipt timestamps are never publication dates."""

from __future__ import annotations

import hashlib
import json
from typing import Iterable

import pandas as pd

FINANCIAL_PIT_CONTRACT = "public_disclosure_v2"
PUBLIC_AVAILABILITY_BASIS = "public_disclosure_reconstructed"
SOURCE_PRIORITY = {"report": 1, "express": 2, "forecast": 3}
DISCLOSURE_COLUMNS = (
    "source_ann_date",
    "source_f_ann_date",
    "source_update_time",
    "source_version_hash",
    "availability_basis",
    "pit_contract_version",
)
INDICATOR_DISCLOSURE_COLUMNS = (
    "income_ann_date",
    "balance_ann_date",
    "source_available_date",
    "availability_basis",
    "pit_contract_version",
)


def public_date_sql(alias: str = "") -> str:
    """Never backdate a version when the vendor's actual date precedes ann_date."""
    prefix = f"{alias}." if alias else ""
    return f"GREATEST({prefix}ann_date, COALESCE({prefix}f_ann_date, {prefix}ann_date))"


def _content_hash(row: pd.Series) -> str:
    # Sort field names and discard the input index: reversing source row order
    # must produce exactly the same winner. SQL source hashes take precedence.
    excluded = {"source_version_hash", "is_extended", "year", "quarter"}
    values = {
        key: None if pd.isna(value) else str(value)
        for key, value in sorted(row.items())
        if key not in excluded and not key.startswith("_")
    }
    payload = json.dumps(values, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def normalize_disclosure_events(
    data: pd.DataFrame, *, mode: str = "public_history"
) -> pd.DataFrame:
    """Retain each public version and choose same-event rows deterministically.

    ann_date is the date this version became public. source_ann_date retains
    the vendor's initial date; source_f_ann_date retains its actual date.
    source_update_time is a last-update witness, NOT a first receipt witness.
    The source must retain the original payload for historical reconstruction.
    """
    if mode != "public_history":
        raise ValueError("system_asof requires an append-only first-receipt ledger")
    if data.empty:
        return data.copy()
    work = data.copy()
    if "data_source" not in work:
        work["data_source"] = "report"
    for target, source in (
        ("source_ann_date", "ann_date"),
        ("source_f_ann_date", "f_ann_date"),
        ("source_update_time", "update_time"),
    ):
        if target not in work:
            work[target] = work[source] if source in work else None
        elif source in work:
            # Concatenating report and express/forecast creates these columns
            # for every row, even when only the report supplied provenance.
            work[target] = work[target].where(work[target].notna(), work[source])
    original = pd.to_datetime(work["source_ann_date"], errors="coerce")
    actual = pd.to_datetime(work["source_f_ann_date"], errors="coerce")
    # Express/forecast use their own public dates; f_ann_date belongs to reports.
    actual = actual.where(work["data_source"].eq("report"), original)
    work["ann_date"] = pd.concat([original, actual], axis=1).max(axis=1).dt.date
    for column in ("source_ann_date", "source_f_ann_date", "end_date"):
        work[column] = pd.to_datetime(work[column], errors="coerce").dt.date
    work["source_update_time"] = pd.to_datetime(
        work["source_update_time"], errors="coerce", utc=True, format="mixed"
    ).dt.tz_localize(None)
    work["availability_basis"] = PUBLIC_AVAILABILITY_BASIS
    work["pit_contract_version"] = FINANCIAL_PIT_CONTRACT
    if "source_version_hash" not in work:
        work["source_version_hash"] = None
    missing_hash = work["source_version_hash"].isna()
    if missing_hash.any():
        work.loc[missing_hash, "source_version_hash"] = work.loc[missing_hash].apply(
            _content_hash, axis=1
        )
    keys = ["ts_code", "end_date", "ann_date", "data_source"]
    work = work.dropna(subset=keys)
    return (
        work.sort_values(
            keys + ["source_update_time", "source_version_hash"],
            kind="mergesort",
            na_position="first",
        )
        .drop_duplicates(keys, keep="last")
        .reset_index(drop=True)
    )


def require_disclosure_columns(columns: Iterable[str]) -> None:
    missing = sorted(set(DISCLOSURE_COLUMNS) - set(columns))
    if missing:
        raise RuntimeError(
            f"migration_required: financial disclosure columns {missing}"
        )


def validate_public_inputs(
    frame: pd.DataFrame, as_of_date: str, *, available_column: str = "ann_date"
) -> None:
    """Reject unverified legacy rows and future source events before consumption."""
    if frame.empty:
        return
    required = {available_column, "pit_contract_version", "availability_basis"}
    if not required <= set(frame):
        raise ValueError(
            "financial_pit_rebuild_required: missing disclosure provenance"
        )
    if not frame["pit_contract_version"].eq(FINANCIAL_PIT_CONTRACT).all():
        raise ValueError("financial_pit_rebuild_required: unverified legacy versions")
    if not frame["availability_basis"].eq(PUBLIC_AVAILABILITY_BASIS).all():
        raise ValueError("Unsupported financial availability basis")
    dates = pd.to_datetime(frame[available_column], errors="coerce")
    if dates.isna().any() or dates.gt(pd.Timestamp(as_of_date)).any():
        raise ValueError("Financial source event is missing or later than as_of_date")
    if available_column != "ann_date" and "ann_date" in frame:
        observations = pd.to_datetime(frame["ann_date"], errors="coerce")
        if observations.isna().any() or observations.gt(pd.Timestamp(as_of_date)).any():
            raise ValueError(
                "Financial observation is missing or later than as_of_date"
            )
