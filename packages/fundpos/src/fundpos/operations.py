from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from filelock import FileLock

from .config import Settings
from .constants import CATEGORIES
from .data import DataBundle
from .errors import DataUnavailable
from .pipeline import advance_state, run_estimate
from .storage import atomic_json, atomic_parquet, code_fingerprint
from .validation import choose_dates


def completed_valuation_date(calendar, now=None, timezone="Asia/Shanghai"):
    """Conservative daily convention: previous completed exchange session.

    Today is excluded even after close, since NAV publication normally occurs later.
    Data gaps do not move this target backwards; they must appear as stale results.
    """
    now = pd.Timestamp.now(tz=timezone) if now is None else pd.Timestamp(now)
    today = (
        now.tz_convert(timezone).tz_localize(None).normalize() if now.tzinfo else now.normalize()
    )
    days = pd.DatetimeIndex(calendar).normalize().sort_values().unique()
    completed = days[days < today]
    if completed.empty:
        raise DataUnavailable("NO_COMPLETED_SESSION", str(today.date()))
    if (today - completed[-1]).days > 14:
        raise DataUnavailable(
            "STALE_CALENDAR", "Exchange calendar does not cover the current observation period"
        )
    return completed[-1]


def run_backfill(
    settings: Settings,
    bundle: DataBundle,
    start,
    end,
    frequency="daily",
    progress=print,
    *,
    categories=CATEGORIES,
    selected_products: set[str] | None = None,
    scope_version="configured",
):
    calendar = pd.DatetimeIndex(pd.to_datetime(bundle["calendar"].date))
    requested = choose_dates(calendar, start, end, frequency)
    if requested.empty:
        raise DataUnavailable("EMPTY_DATE_RANGE", "No requested trading dates")
    model = settings.values["model"]
    calculation = requested
    if model["smooth_penalty"]:
        calculation = choose_dates(calendar, pd.Timestamp(start) - pd.Timedelta(days=90), end)
    records, paths, previous = [], [], None
    for day in calculation:
        run = run_estimate(
            settings,
            bundle,
            day,
            day + pd.Timedelta(days=1),
            previous,
            publish=False,
            categories=categories,
            selected_products=selected_products,
            scope_version=scope_version,
        )
        result = pd.read_parquet(run / "estimates.parquet")
        previous = advance_state(previous, result)
        if day in requested:
            records.append(result)
            paths.append(str(run.resolve()))
            progress(f"backfill {day.date()}: {result.status.value_counts().to_dict()}", flush=True)
    identity = f"{str(requested[0].date())}_{str(requested[-1].date())}_{model['name']}_{settings.fingerprint[:10]}_{bundle.fingerprint[:10]}_{code_fingerprint(settings.root)[:10]}"
    directory = settings.path("output_dir") / "history" / identity
    directory.mkdir(parents=True, exist_ok=True)
    columns = list(dict.fromkeys(col for frame in records for col in frame.columns))
    dtypes = {}
    for col in columns:
        observed = [
            frame[col].dropna() for frame in records if col in frame and frame[col].notna().any()
        ]
        dtypes[col] = (
            "Float64" if observed and pd.api.types.is_number(observed[0].iloc[0]) else "string"
        )
    history = pd.concat(
        [frame.reindex(columns=columns).astype(dtypes) for frame in records], ignore_index=True
    )
    atomic_parquet(directory / "estimates.parquet", history)
    # Weekly output uses the last *actual exchange day*; a missing final-day result is not replaced by an earlier day.
    week_ends = choose_dates(calendar, start, end, "weekly").strftime("%Y-%m-%d")
    weekly = history.loc[history.valuation_date.isin(week_ends)]
    atomic_parquet(directory / "weekly.parquet", weekly)
    atomic_json(
        directory / "manifest.json",
        {
            "run_dirs": paths,
            "input_hash": bundle.fingerprint,
            "config_hash": settings.fingerprint,
            "code_hash": code_fingerprint(settings.root),
            "frequency": frequency,
            "history_kind": "announcement_date_reconstruction",
            "rows": len(history),
        },
    )
    return directory


def record_observation(settings: Settings, run: Path, now=None):
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    current = pd.Timestamp.now(tz="Asia/Shanghai") if now is None else pd.Timestamp(now)
    today = current.tz_localize(None).normalize() if current.tzinfo else current.normalize()
    day = pd.Timestamp(manifest["valuation_date"])
    source = manifest["provenance"]
    if source.get("synthetic") or source.get("universe_scope") != "all_active_equity_metadata":
        raise DataUnavailable(
            "OBSERVATION_SCOPE", "Live observation requires the full real fund universe"
        )
    snapshot = source.get("snapshot_path")
    if not snapshot or not Path(snapshot).exists():
        raise DataUnavailable("OBSERVATION_INPUT_MISSING", "A preserved input snapshot is required")
    calendar = DataBundle.load(Path(snapshot))["calendar"].date
    if day != completed_valuation_date(calendar, current):
        raise DataUnavailable(
            "OBSERVATION_NOT_LATEST", "A replay of an earlier date cannot count as live observation"
        )
    created = pd.Timestamp(manifest["created_at"])
    if (created.tz_localize(None).normalize() != today) or not 0 <= (today - day).days <= 7:
        raise DataUnavailable(
            "OBSERVATION_NOT_LIVE", "Historical backfills cannot count as live observation days"
        )
    output = settings.path("output_dir")
    path = output / "observation.json"
    with FileLock(str(output / ".observation.lock"), timeout=0):
        ledger = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"days": {}}
        ledger["days"][manifest["valuation_date"]] = {
            "recorded_at": current.isoformat(),
            "run_id": manifest["run_id"],
            "complete": manifest["complete"],
            "status_counts": manifest["status_counts"],
            "code_hash": manifest["code_hash"],
        }
        # Continuity against the actual calendar is verified with the saved daily input.
        qualifying = []
        if snapshot and Path(snapshot).exists():
            past = pd.DatetimeIndex(calendar)
            past = past[past <= day].sort_values()
            for d in reversed(past):
                entry = ledger["days"].get(str(d.date()))
                if (
                    not entry
                    or not entry["complete"]
                    or entry["code_hash"] != manifest["code_hash"]
                ):
                    break
                qualifying.append(str(d.date()))
        ledger.update(
            consecutive_complete_trading_days=len(qualifying),
            required_trading_days=10,
            status="observation_days_passed_pending_operational_review"
            if len(qualifying) >= 10
            else "observing",
            observation_dates=sorted(qualifying),
        )
        atomic_json(path, ledger)
    return ledger
