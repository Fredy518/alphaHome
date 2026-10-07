"""Select whole, dated classification records without inventing a vintage."""
from __future__ import annotations

import pandas as pd

CLASSIFICATION_COLUMNS = tuple(
    f"industry_{kind}{level}"
    for kind in ("level", "code") for level in (1, 2, 3)
)
AMBIGUOUS_QUALITY = "invalid"  # Existing native CHECK permits this; reason identifies ambiguity.
AMBIGUOUS_REASON = "same_start_classification_conflict_public_vintage_unverified"


def select_latest_classification(
    frame: pd.DataFrame, as_of_date, *, active_only: bool = True
) -> pd.DataFrame:
    """One row per stock; different latest-start payloads remain unknown.

    Exact classification duplicates are harmless. Their longest retained end
    interval selects a whole original row, preserving its null fields. Dates
    bound the retained evidence; they do not certify public revision vintages.
    """
    if frame is None or frame.empty:
        return pd.DataFrame()
    required = {"ts_code", "in_date", *CLASSIFICATION_COLUMNS}
    if active_only:
        required.add("out_date")
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"industry_evidence_missing:{','.join(missing)}")
    source = frame.copy()
    starts = pd.to_datetime(source["in_date"], errors="coerce").dt.normalize()
    if starts.isna().any() or source["ts_code"].isna().any():
        raise ValueError("industry_evidence_invalid_key")
    cutoff = pd.Timestamp(as_of_date).normalize()
    allowed = starts.le(cutoff)
    ends = pd.Series(pd.NaT, index=source.index, dtype="datetime64[ns]")
    if "out_date" in source:
        ends = pd.to_datetime(source["out_date"], errors="coerce").dt.normalize()
        if (source["out_date"].notna() & ends.isna()).any():
            raise ValueError("industry_evidence_invalid_end")
    if active_only:
        allowed &= ends.isna() | ends.gt(cutoff)
    source = source.loc[allowed].copy()
    if source.empty:
        return pd.DataFrame()
    source["_evidence_start"] = starts.loc[allowed]
    source["_evidence_end"] = ends.loc[allowed]
    result = []
    for code, group in source.groupby("ts_code", sort=True):
        latest = group.loc[group["_evidence_start"].eq(group["_evidence_start"].max())]
        payloads = latest[list(CLASSIFICATION_COLUMNS)].drop_duplicates()
        ambiguous = len(payloads) > 1
        row = latest.sort_values("_evidence_end", na_position="last").iloc[-1].to_dict()
        row.pop("_evidence_start", None)
        row.pop("_evidence_end", None)
        for column in CLASSIFICATION_COLUMNS:
            if pd.isna(row[column]):
                row[column] = None
        if ambiguous:
            row.update({column: None for column in CLASSIFICATION_COLUMNS})
        row["data_quality"] = AMBIGUOUS_QUALITY if ambiguous else "normal"
        row["classification_ambiguous"] = ambiguous
        result.append(row)
    return pd.DataFrame.from_records(result)
