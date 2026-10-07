"""AlphaDB read-only inputs for representative industry clusters."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .engine import ClusterConfig, build_features, quality_checked_classification
from .maintenance import stable_hash

MEMBER_METHOD = "official_then_disclosed_etf_holdings_v1"


def query_frame(conn, sql, params=()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[x.name for x in cur.description])


def load_inputs(conn, universe: pd.DataFrame, start, end, cache: Path, config: ClusterConfig | None = None) -> dict:
    """Cache one consistent database snapshot; reruns consume the same inputs."""
    start, end = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
    config = config or ClusterConfig()
    history_days = max(config.maximum_structure_age_days, config.maximum_classification_age_days) + 7
    quote_history_days = max(560, 2 * max(config.windows) + 30)
    codes = sorted(universe.index_code.unique())
    request = {"codes": codes, "start": str(start.date()), "end": str(end.date()),
               "member_method": MEMBER_METHOD, "price_basis": "official_previous_close_v1",
               "source_snapshot_history_days": history_days,
               "quote_history_days": quote_history_days, "benchmark": config.benchmark}
    request_hash = stable_hash(request)
    cache = cache / request_hash[:16]
    receipt_path = cache / "receipt.json"
    names = ["members", "classification", "quotes", "calendar", "index_metadata"]
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt["request_hash"] != request_hash:
            raise ValueError("input cache belongs to a different universe/range")
        frames = {name: pd.read_csv(cache / (name + ".csv.gz"), float_precision="round_trip", dtype={"index_code": str, "ts_code": str,
                   "industry_code1": str, "industry_code2": str}) for name in names}
    else:
        cache.mkdir(parents=True, exist_ok=True)
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout='60s'")
        structure_start = start - pd.Timedelta(days=history_days)
        members = query_frame(conn, """
            SELECT obs_date,index_code,index_name,ts_code,weight,
                   source_effective_date,source_available_date,source_quality,
                   is_eligible,weight_basis,created_at,updated_at
            FROM pit.pit_etf_index_members_monthly
            WHERE obs_date BETWEEN %s AND %s AND index_code=ANY(%s)
              AND method_version=%s AND constituent_scope='full_index' AND NOT is_proxy
            ORDER BY obs_date,index_code,ts_code
        """, (structure_start.date(), end.date(), codes, MEMBER_METHOD))
        stocks = sorted(members.ts_code.unique())
        classification = query_frame(conn, """
            SELECT obs_date,ts_code,industry_code1,industry_code2,industry_level1,industry_level2,
                   data_quality,created_at,updated_at
            FROM pit.pit_industry_classification
            WHERE obs_date BETWEEN %s AND %s AND data_source='sw' AND ts_code=ANY(%s)
            ORDER BY obs_date,ts_code
        """, (structure_start.date(), end.date(), stocks))
        quote_start = start - pd.Timedelta(days=quote_history_days)
        quotes = query_frame(conn, """
            SELECT ts_code AS index_code,trade_date,close,pre_close,pct_change,update_time
            FROM rawdata.index_factor_pro
            WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date,ts_code
        """, (sorted(set(codes + [config.benchmark])), quote_start.date(), end.date()))
        calendar = query_frame(conn, """
            SELECT cal_date AS trade_date FROM rawdata.others_calendar
            WHERE exchange='SSE' AND is_open=1 AND cal_date BETWEEN %s AND %s
            ORDER BY cal_date
        """, (quote_start.date(), end.date()))
        metadata = query_frame(conn, """
            SELECT c.index_code,b.name,b.list_date,
                   (SELECT MIN(e.list_date) FROM rawdata.fund_etf_basic e
                    WHERE e.index_code=c.index_code) AS first_etf_list_date
            FROM unnest(%s::text[]) AS c(index_code)
            LEFT JOIN rawdata.index_basic b ON b.ts_code=c.index_code
            ORDER BY c.index_code
        """, (codes,))
        frames = dict(members=members, classification=classification, quotes=quotes,
                      calendar=calendar, index_metadata=metadata)
        for name, frame in frames.items():
            frame.to_csv(cache / (name + ".csv.gz"), index=False, compression="gzip")
        receipt = {"request": request, "request_hash": request_hash,
                   "queried_at": datetime.now(timezone.utc).isoformat(),
                   "sources": ["pit.pit_etf_index_members_monthly", "pit.pit_industry_classification",
                               "rawdata.index_factor_pro", "rawdata.index_basic", "rawdata.fund_etf_basic",
                               "rawdata.others_calendar"],
                   "record_kind": "historical_reconstruction", "rows": {k: len(v) for k, v in frames.items()}}
        receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
    for frame in frames.values():
        for col in frame:
            if col in ("obs_date", "trade_date", "source_effective_date", "source_available_date",
                       "list_date", "first_etf_list_date"):
                frame[col] = pd.to_datetime(frame[col])
    frames["members"]["weight"] = pd.to_numeric(frames["members"].weight).astype(float)
    frames["members"]["is_eligible"] = frames["members"].is_eligible.astype(str).str.lower().eq("true")
    for col in ("close", "pre_close", "pct_change"):
        frames["quotes"][col] = pd.to_numeric(frames["quotes"][col], errors="coerce").astype(float)
    frames["receipt"] = receipt
    frames["cache_dir"] = str(cache.resolve())
    frames["content_hash"] = stable_hash({
        name: hashlib.sha256((cache / (name + ".csv.gz")).read_bytes()).hexdigest() for name in names})
    return frames


def daily_returns(inputs: dict) -> tuple[pd.DataFrame, dict]:
    """Use the supplied previous close even when yesterday's row is absent."""
    quotes = inputs["quotes"].copy()
    days = pd.DatetimeIndex(inputs["calendar"].trade_date)
    if quotes.duplicated(["index_code", "trade_date"]).any() or days.duplicated().any():
        raise ValueError("duplicate daily keys")
    quotes = quotes.loc[quotes.trade_date.isin(days)].copy()
    close = quotes.pivot(index="trade_date", columns="index_code", values="close").reindex(days)
    prev = quotes.pivot(index="trade_date", columns="index_code", values="pre_close").reindex(days)
    reported = quotes.pivot(index="trade_date", columns="index_code", values="pct_change").reindex(days) / 100
    provided = close.div(prev).sub(1).where(close.gt(0) & prev.gt(0))
    contiguous = close.pct_change(fill_method=None).where(close.gt(0) & close.shift().gt(0))
    conflict = provided.notna() & reported.notna() & provided.sub(reported).abs().gt(0.00002)
    provided = provided.mask(conflict)
    # A rejected provided value must not be silently reintroduced by fallback.
    fallback = contiguous.where(prev.isna() | prev.le(0))
    result = provided.combine_first(fallback)
    result = result.replace([np.inf, -np.inf], np.nan)
    return result, {"official_previous_close_returns": int(provided.notna().sum().sum()),
                    "contiguous_close_fallback_returns": int((provided.isna() & fallback.notna()).sum().sum()),
                    "reported_return_conflicts_excluded": int(conflict.sum().sum()),
                    "recovered_vs_close_row_difference": int((provided.notna() & contiguous.isna()).sum().sum()),
                    "missing_days_forward_filled": 0}


def snapshot_features(inputs, returns, universe, asof, config: ClusterConfig):
    date = pd.Timestamp(asof).normalize()
    members = inputs["members"].loc[inputs["members"].obs_date.le(date)
                  & inputs["members"].obs_date.ge(date - pd.Timedelta(days=config.maximum_structure_age_days))].copy()
    classification = inputs["classification"].loc[inputs["classification"].obs_date.le(date)
                  & inputs["classification"].obs_date.ge(date - pd.Timedelta(days=config.maximum_classification_age_days))].copy()
    classification = classification.sort_values(["ts_code", "obs_date"]).drop_duplicates("ts_code", keep="last")
    classification = quality_checked_classification(classification)
    metadata = inputs["index_metadata"].set_index("index_code")
    valid_class = classification.loc[classification[["industry_code1", "industry_code2"]].notna().all(axis=1)]
    classified = set(valid_class.ts_code)
    accepted, reasons, structure_sources = [], [], []
    member_groups = {code: group for code, group in members.groupby("index_code")}
    for code in universe.index_code:
        why = []
        history = member_groups.get(code)
        group = None
        if history is not None:
            # Reject an incomplete snapshot as a whole; never retain a partial vector.
            for obs, candidate in history.groupby("obs_date", sort=True):
                eligible = (candidate.is_eligible.all()
                    and candidate.source_available_date.notna().all() and candidate.source_effective_date.notna().all()
                    and candidate.source_available_date.le(date).all() and candidate.source_effective_date.le(date).all()
                    and candidate.source_effective_date.ge(date - pd.Timedelta(days=config.maximum_structure_age_days)).all()
                    and np.isfinite(candidate.weight).all() and candidate.weight.ge(0).all()
                    and abs(candidate.weight.sum() - 1) <= 1e-6)
                if eligible:
                    group = candidate
        available_from = metadata.loc[code, "list_date"]
        if pd.isna(available_from):
            available_from = metadata.loc[code, "first_etf_list_date"]
        if pd.isna(available_from):
            why.append("missing_index_or_first_carrier_launch_date")
        elif available_from > date:
            why.append("not_yet_launched")
        if group is None or group.empty:
            why.append("missing_monthly_constituents")
        else:
            if not group.is_eligible.all():
                why.append("source_ineligible")
            if not group.ts_code.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all():
                why.append("non_a_share_constituents")
            if not np.isfinite(group.weight).all() or group.weight.lt(0).any() or abs(group.weight.sum() - 1) > 1e-6:
                why.append("invalid_weight_sum")
            if group.source_available_date.isna().any() or group.source_effective_date.isna().any():
                why.append("missing_source_date")
            if group.source_available_date.gt(date).any() or group.source_effective_date.gt(date).any():
                why.append("future_source_date")
            if not config.allow_partial_classification and not set(group.ts_code).issubset(classified):
                why.append("missing_industry_classification")
        if why:
            reasons.append({"asof_date": str(date.date()), "index_code": code, "reason": "|".join(why)})
        else:
            accepted.append(group)
            structure_sources.append({"asof_date": str(date.date()), "index_code": code,
                                      "constituent_obs_date": str(group.obs_date.max().date()),
                                      "source_effective_date": str(group.source_effective_date.min().date()),
                                      "source_available_date": str(group.source_available_date.max().date()),
                                      "structure_age_days": int((date - group.source_effective_date.min()).days),
                                      "used_prior_snapshot": bool(group.obs_date.max() < date)})
    if not accepted and config.selection_data_policy == "strict":
        raise ValueError(f"no usable structure at {date.date()}")
    current = pd.concat(accepted, ignore_index=True) if accepted else inputs["members"].iloc[:0].copy()
    # Preserve an available L1 even when only that stock's L2 is unknown.
    features = build_features(current, classification if config.allow_partial_classification else valid_class,
                              returns, date, config)
    return (features, pd.DataFrame(reasons, columns=["asof_date", "index_code", "reason"]),
            pd.DataFrame(structure_sources, columns=["asof_date", "index_code", "constituent_obs_date",
                                                    "source_effective_date", "source_available_date",
                                                    "structure_age_days", "used_prior_snapshot"]))
