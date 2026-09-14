from __future__ import annotations

import hashlib

import pandas as pd

from .config import Settings
from .data import DataBundle
from .errors import DataUnavailable
from .fixed_income_pipeline import compute_fixed_income_date
from .storage import atomic_json, atomic_parquet, code_fingerprint
from .validation import choose_dates


def run_fixed_income_backfill(settings: Settings, bundle: DataBundle, start, end,
                              frequency="daily", progress=print, selected_products=None,
                              model_family="fixed_income_plus"):
    calendar = pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date))
    requested = choose_dates(calendar, start, end, frequency)
    if requested.empty:
        raise DataUnavailable("EMPTY_DATE_RANGE", "No requested trading dates")
    previous, records, scenario_records = None, [], []
    for day in requested:
        result, scenarios = compute_fixed_income_date(
            settings,
            bundle,
            day,
            day + pd.Timedelta(days=1),
            previous,
            selected_products=selected_products,
        )
        records.append(result)
        if not scenarios.empty:
            scenario_records.append(scenarios)
        valid = result.loc[result.status.isin(["ok", "degraded"])]
        if not valid.empty:
            previous = pd.concat([previous, valid], ignore_index=True) if previous is not None else valid
            previous = previous.sort_values("valuation_date").drop_duplicates("master_code", keep="last")
        progress(f"fixed-income backfill {day.date()}: {result.status.value_counts().to_dict()}", flush=True)
    history = pd.concat(records, ignore_index=True)
    identity = hashlib.sha256(
        f"{settings.fingerprint}|{bundle.fingerprint}|{start}|{end}|{frequency}|{sorted(selected_products or [])}".encode()
    ).hexdigest()
    directory = settings.path("output_dir") / "history" / f"{model_family}_{identity[:24]}"
    directory.mkdir(parents=True, exist_ok=True)
    atomic_parquet(directory / "estimates.parquet", history)
    atomic_parquet(directory / "scenarios.parquet", pd.concat(scenario_records, ignore_index=True)
                   if scenario_records else pd.DataFrame())
    weekly_dates = choose_dates(calendar, start, end, "weekly").strftime("%Y-%m-%d")
    atomic_parquet(directory / "weekly.parquet", history.loc[history.valuation_date.isin(weekly_dates)])
    atomic_json(directory / "manifest.json", {"model_family": model_family,
        "input_hash": bundle.fingerprint, "config_hash": settings.fingerprint,
        "code_hash": code_fingerprint(settings.root), "start": str(pd.Timestamp(start).date()),
        "end": str(pd.Timestamp(end).date()), "frequency": frequency, "rows": len(history),
        "final_holdout_opened": False})
    return directory
