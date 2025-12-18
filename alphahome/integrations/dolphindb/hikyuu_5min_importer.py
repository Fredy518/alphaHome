from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

from alphahome.common.logging_utils import get_logger

from .manager import DolphinDBManager

logger = get_logger(__name__)


@dataclass(frozen=True)
class Hikyuu5MinImporterConfig:
    hikyuu_data_dir: str
    db_path: str = "dfs://kline_5min"
    table_name: str = "kline_5min"
    chunk_rows: int = 200_000
    price_scale: float = 1000.0
    amount_scale: float = 10.0


def _normalize_ts_code(ts_code: str) -> str:
    ts_code = (ts_code or "").strip()
    if not ts_code:
        raise ValueError("ts_code is empty")
    if "." not in ts_code:
        raise ValueError(f"ts_code must include suffix like .SH/.SZ/.BJ: {ts_code!r}")
    code, suffix = ts_code.split(".", 1)
    if not code.isdigit():
        raise ValueError(f"Invalid ts_code digits: {ts_code!r}")
    suffix = suffix.upper()
    if suffix not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"Unsupported ts_code suffix: {suffix!r} (ts_code={ts_code!r})")
    return f"{code}.{suffix}"


def market_5min_h5_path(hikyuu_data_dir: str, market: str) -> str:
    """Return `{market}_5min.h5` under Hikyuu data dir (typically E:/stock)."""
    market = (market or "").strip().lower()
    if market not in {"sh", "sz", "bj"}:
        raise ValueError(f"Unsupported market: {market!r}")
    root = os.path.abspath(os.path.expanduser(hikyuu_data_dir))
    return os.path.join(root, f"{market}_5min.h5")


def ts_code_to_hikyuu_symbol(ts_code: str) -> str:
    """Map `000001.SZ` -> `SZ000001` (Hikyuu dataset symbol under `/data`)."""
    ts_code = _normalize_ts_code(ts_code)
    code, suffix = ts_code.split(".", 1)
    return f"{suffix}{code}"


def _parse_date_like(value: Optional[str]) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    return pd.to_datetime(value, errors="raise")


def normalize_hikyuu_5min_records(
    records: np.ndarray,
    *,
    ts_code: str,
    price_scale: float = 1000.0,
    amount_scale: float = 10.0,
) -> pd.DataFrame:
    """Convert Hikyuu HDF5 minute kline records into DolphinDB schema DataFrame."""
    ts_code = _normalize_ts_code(ts_code)
    if records is None or len(records) == 0:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "trade_time",
                "month",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
            ]
        )

    names = set(records.dtype.names or [])
    required = {
        "closePrice",
        "datetime",
        "highPrice",
        "lowPrice",
        "openPrice",
        "transAmount",
        "transCount",
    }
    missing = required - names
    if missing:
        raise ValueError(f"Hikyuu records missing fields: {sorted(missing)}")

    dt_int = records["datetime"].astype("int64")
    trade_time = pd.to_datetime(dt_int.astype(str), format="%Y%m%d%H%M", errors="coerce")

    df = pd.DataFrame(
        {
            "ts_code": ts_code,
            "trade_time": trade_time,
            "open": records["openPrice"].astype("float64") / float(price_scale),
            "high": records["highPrice"].astype("float64") / float(price_scale),
            "low": records["lowPrice"].astype("float64") / float(price_scale),
            "close": records["closePrice"].astype("float64") / float(price_scale),
            "vol": records["transCount"].astype("int64"),
            "amount": records["transAmount"].astype("float64") / float(amount_scale),
        }
    )

    df = df.dropna(subset=["trade_time"])
    df["month"] = df["trade_time"].dt.strftime("%Y%m").astype("int32")

    df = df.sort_values(["ts_code", "trade_time"], kind="mergesort")
    df = df.drop_duplicates(["ts_code", "trade_time"], keep="last")

    return df[
        [
            "ts_code",
            "trade_time",
            "month",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "amount",
        ]
    ].reset_index(drop=True)


def apply_incremental_filter(
    df: pd.DataFrame, *, after: Optional[pd.Timestamp]
) -> pd.DataFrame:
    """Filter rows strictly newer than `after`."""
    if df is None or df.empty or after is None:
        return df
    after = pd.to_datetime(after, errors="coerce")
    if pd.isna(after):
        return df
    return df[df["trade_time"] > after]


class HikyuuKline5MinImporter:
    """Import Hikyuu 5-min HDF5 (sh_5min.h5/sz_5min.h5/bj_5min.h5) into DolphinDB."""

    def __init__(self, ddb: DolphinDBManager, config: Hikyuu5MinImporterConfig):
        self.ddb = ddb
        self.config = config

    def _dataset_for_ts_code(self, ts_code: str):
        import h5py

        ts_code = _normalize_ts_code(ts_code)
        code, suffix = ts_code.split(".", 1)
        market = suffix.lower()
        h5_path = market_5min_h5_path(self.config.hikyuu_data_dir, market)
        if not os.path.exists(h5_path):
            raise FileNotFoundError(f"Hikyuu 5min H5 file not found: {h5_path}")
        symbol = f"{suffix}{code}"

        h5 = h5py.File(h5_path, "r")
        try:
            group = h5.get("data")
            if group is None:
                raise KeyError(f"Missing group 'data' in {h5_path}")
            ds = group.get(symbol)
            if ds is None:
                raise KeyError(f"Dataset not found: /data/{symbol} (file={h5_path})")
            return h5, ds, h5_path, symbol
        except Exception:
            h5.close()
            raise

    def import_many(
        self,
        ts_codes: Iterable[str],
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        incremental: bool = False,
        dry_run: bool = False,
    ) -> dict:
        start_ts = _parse_date_like(start)
        end_ts = _parse_date_like(end)

        total_rows = 0
        total_inserted = 0
        files: List[str] = []
        started_at = datetime.now().isoformat(timespec="seconds")

        chunk_rows = max(int(self.config.chunk_rows), 1)

        for raw_code in ts_codes:
            ts_code = _normalize_ts_code(raw_code)
            h5, ds, h5_path, symbol = self._dataset_for_ts_code(ts_code)
            files.append(h5_path)
            try:
                max_existing: Optional[pd.Timestamp] = None
                if incremental and not dry_run:
                    max_existing = self.ddb.get_max_trade_time(
                        db_path=self.config.db_path,
                        table_name=self.config.table_name,
                        ts_code=ts_code,
                    )

                n = int(ds.shape[0])
                if n <= 0:
                    logger.info("Skip %s (empty dataset)", ts_code)
                    continue

                imported_rows = 0
                for i in range(0, n, chunk_rows):
                    # Incremental fast-skip: the dataset is time-sorted, so if the chunk's last datetime
                    # is not newer than existing max, skip without loading/converting the whole chunk.
                    if max_existing is not None:
                        try:
                            last_idx = min(i + chunk_rows - 1, n - 1)
                            last_dt_int = int(ds[last_idx]["datetime"])
                            cutoff_int = int(max_existing.strftime("%Y%m%d%H%M"))
                            if last_dt_int <= cutoff_int:
                                continue
                        except Exception:
                            pass

                    records = ds[i : i + chunk_rows]
                    df = normalize_hikyuu_5min_records(
                        records,
                        ts_code=ts_code,
                        price_scale=self.config.price_scale,
                        amount_scale=self.config.amount_scale,
                    )

                    if start_ts is not None:
                        df = df[df["trade_time"] >= start_ts]
                    if end_ts is not None:
                        df = df[df["trade_time"] <= end_ts]
                    df = apply_incremental_filter(df, after=max_existing)

                    if df.empty:
                        continue

                    total_rows += len(df)
                    imported_rows += len(df)

                    if dry_run:
                        continue

                    inserted = self.ddb.append_dataframe(
                        df, db_path=self.config.db_path, table_name=self.config.table_name
                    )
                    try:
                        total_inserted += int(inserted)
                    except Exception:
                        total_inserted += len(df)

                logger.info("Imported %s rows for %s (/data/%s)", imported_rows, ts_code, symbol)
            finally:
                h5.close()

        return {
            "started_at": started_at,
            "hikyuu_data_dir": self.config.hikyuu_data_dir,
            "db_path": self.config.db_path,
            "table_name": self.config.table_name,
            "dry_run": dry_run,
            "incremental": incremental,
            "total_rows": total_rows,
            "total_inserted": total_inserted,
            "files": sorted(set(files)),
        }
