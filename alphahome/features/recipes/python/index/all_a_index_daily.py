"""Self-built all-A free-float weighted total-return index.

The recipe reproduces the reviewed ETF research definition with AlphaDB as its
only runtime data source.  It stores reusable index facts and data-quality
atoms; portfolio state, budgets and trading rules remain outside AlphaHome.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import numpy as np
import pandas as pd

from alphahome.features.registry import feature_register
from alphahome.features.storage.python_feature import PythonFeatureTable
from alphahome.features.storage.refresh_log import log_mv_refresh

METHOD_VERSION = "all_a_free_float_total_return_v1"
PRIMARY_VARIANT = "strict_free"
SERIES_IDS = {
    PRIMARY_VARIANT: "AH_ALL_A_FREE_FLOAT_TR_V1",
    "float_fallback": "AH_ALL_A_FLOAT_FALLBACK_TR_V1",
}
BSE_FIRST_SESSION = pd.Timestamp("2021-11-15")
NEW_LISTING_BUFFER_SESSIONS = 10

# These are identity repairs, not extra constituents.  They were independently
# checked in the original research and are absent from rawdata.stock_code_mapping.
MANUAL_ALIAS_ROWS = (
    ("000022.SZ", "001872.SZ", "2018-12-26"),
    ("000043.SZ", "001914.SZ", "2019-12-16"),
    ("300114.SZ", "302132.SZ", "2025-02-17"),
    ("430489.BJ", "920489.BJ", "2025-05-06"),
    ("830799.BJ", "920799.BJ", "2025-05-06"),
    ("831445.BJ", "920445.BJ", "2025-05-06"),
    ("833819.BJ", "920819.BJ", "2025-05-06"),
    ("834682.BJ", "920682.BJ", "2025-05-06"),
    ("839167.BJ", "920167.BJ", "2025-05-06"),
)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _records_to_frame(rows: Iterable[Any]) -> pd.DataFrame:
    rows = list(rows)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(row) for row in rows])


def _manual_aliases() -> pd.DataFrame:
    return pd.DataFrame(
        MANUAL_ALIAS_ROWS,
        columns=["ts_code_old", "ts_code_new", "switch_date"],
    )


def _canonical_rows(
    frame: pd.DataFrame,
    aliases: dict[str, str],
    mapping: pd.DataFrame,
    value_columns: list[str],
    audit_label: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Merge old/new security codes without selecting on realized returns."""

    if frame.empty:
        return frame.assign(security=pd.Series(dtype=str)), {
            "part": audit_label,
            "raw_rows": 0,
            "duplicate_rows": 0,
            "duplicate_identity_days": 0,
        }
    result = frame.copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"])
    result["security"] = result["ts_code"].map(aliases).fillna(result["ts_code"])
    switch_dates = mapping.set_index("ts_code_new")["switch_date"]
    switch = result["security"].map(switch_dates)
    is_old_code = result["ts_code"].ne(result["security"])
    prefer_old_code = result["trade_date"].lt(switch)
    result["priority"] = is_old_code.ne(prefer_old_code).astype(int)

    duplicate = result[result.duplicated(["security", "trade_date"], keep=False)]
    audit: dict[str, Any] = {
        "part": audit_label,
        "raw_rows": int(len(result)),
        "duplicate_rows": int(len(duplicate)),
        "duplicate_identity_days": 0,
    }
    if not duplicate.empty:
        grouped = duplicate.groupby(["security", "trade_date"], observed=True)
        audit["duplicate_identity_days"] = int(grouped.ngroups)
        for column in value_columns:
            numeric = duplicate.assign(
                **{column: pd.to_numeric(duplicate[column], errors="coerce")}
            )
            span = numeric.groupby(["security", "trade_date"], observed=True)[
                column
            ].agg(lambda values: values.max() - values.min())
            conflicts = int(span.gt(1e-7).sum())
            audit[f"{column}_conflicts"] = conflicts
            if column in {"open", "close", "pre_close"}:
                _require(
                    conflicts == 0,
                    f"alias price conflicts: {audit_label}/{column}/{conflicts}",
                )

    result = result.sort_values(["trade_date", "security", "priority", "ts_code"])
    result = (
        result.groupby(["trade_date", "security"], sort=False, as_index=False)[
            value_columns
        ]
        .first()
        .sort_values(["trade_date", "security"])
        .reset_index(drop=True)
    )
    _require(
        not result.duplicated(["security", "trade_date"]).any(),
        "identity de-duplication failed",
    )
    return result, audit


def _reference_open_level(
    previous_level: float,
    weights: np.ndarray,
    data_eligible: np.ndarray,
    open_price: np.ndarray,
    pre_close: np.ndarray,
) -> float:
    quoted = np.isfinite(open_price) & (open_price > 0) & data_eligible
    held = weights > 0
    invalid = held & quoted & (~np.isfinite(pre_close) | (pre_close <= 0))
    _require(
        not invalid.any(),
        f"held stock has invalid opening pre_close: "
        f"{np.flatnonzero(invalid)[:8].tolist()}",
    )
    gross = np.ones(len(weights))
    good = quoted & np.isfinite(pre_close) & (pre_close > 0)
    gross[good] = open_price[good] / pre_close[good]
    change = float(np.dot(weights, gross - 1.0))
    _require(np.isfinite(change) and change > -1.0, "invalid aggregate index open")
    return previous_level * (1.0 + change)


def _day_step(
    previous_price: np.ndarray,
    previous_shares: np.ndarray,
    previous_float: np.ndarray,
    eligible: np.ndarray,
    data_eligible: np.ndarray,
    close: np.ndarray,
    pre_close: np.ndarray,
    free_share: np.ndarray,
    float_share: np.ndarray,
    cash: np.ndarray,
    bonus: np.ndarray,
) -> dict[str, np.ndarray | float]:
    """Calculate one session using only prior state and same-day observations."""

    capital = np.where(
        eligible & (previous_price > 0) & (previous_shares > 0),
        previous_price * previous_shares,
        0.0,
    )
    _require(float(capital.sum()) > 0, "empty index denominator")
    weights = capital / capital.sum()
    quoted = np.isfinite(close) & (close > 0) & data_eligible
    invalid = (capital > 0) & quoted & (~np.isfinite(pre_close) | (pre_close <= 0))
    _require(
        not invalid.any(),
        f"held stock has invalid pre_close: {np.flatnonzero(invalid)[:8].tolist()}",
    )

    reference_gross = np.ones(len(previous_price))
    good_return = quoted & np.isfinite(pre_close) & (pre_close > 0)
    reference_gross[good_return] = close[good_return] / pre_close[good_return]
    active_cash = np.where(data_eligible & (previous_price > 0), cash, 0.0)

    cash_fraction = np.divide(
        active_cash,
        previous_price,
        out=np.zeros_like(active_cash),
        where=previous_price > 0,
    )
    _require(
        (cash_fraction >= 0).all() and (cash_fraction < 1).all(),
        "invalid cash distribution fraction",
    )

    reference_return = float(np.dot(weights, reference_gross - 1.0))
    _require(
        np.isfinite(reference_return) and reference_return > -1.0,
        "invalid aggregate index return",
    )

    next_price = previous_price.copy()
    not_quoted = data_eligible & ~quoted & (previous_price > 0)
    next_price[not_quoted] = (previous_price[not_quoted] - active_cash[not_quoted]) / (
        1.0 + bonus[not_quoted]
    )
    next_price[quoted] = close[quoted]

    next_shares = previous_shares.copy()
    next_float = previous_float.copy()
    next_shares[data_eligible] *= 1.0 + bonus[data_eligible]
    next_float[data_eligible] *= 1.0 + bonus[data_eligible]
    fresh_shares = data_eligible & np.isfinite(free_share) & (free_share > 0)
    fresh_float = data_eligible & np.isfinite(float_share) & (float_share > 0)
    next_shares[fresh_shares] = free_share[fresh_shares]
    next_float[fresh_float] = float_share[fresh_float]

    return {
        "reference_return": reference_return,
        "weights": weights,
        "capital": capital,
        "quoted": quoted,
        "fresh_shares": fresh_shares,
        "next_price": next_price,
        "next_shares": next_shares,
        "next_float": next_float,
    }


def _copy_value(value: Any) -> Any:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _non_seed_month_starts(frame: pd.DataFrame) -> pd.DataFrame:
    """Return true calendar-month starts, excluding the index seed month."""

    dated = frame.sort_values("trade_date").assign(
        period=lambda value: pd.to_datetime(value["trade_date"]).dt.to_period("M")
    )
    month_starts = dated.groupby("period", sort=True).head(1)
    return month_starts.loc[~month_starts["is_seed"]]


class _AuditedAtomicFullRefreshFeature(PythonFeatureTable):
    """Compute first, then replace the visible table in one transaction."""

    refresh_strategy = "full"
    supported_strategies = ("full",)
    _build_details: dict[str, Any]

    async def refresh(self, strategy: str | None = None) -> dict[str, Any]:
        actual_strategy = (
            self.refresh_strategy if strategy in (None, "default") else strategy
        )
        if actual_strategy != "full":
            raise ValueError(f"{self.name} only supports an atomic full refresh")
        if not await self.exists():
            await self.create(if_not_exists=False)

        started_at = datetime.now(timezone.utc)
        started_clock = time.monotonic()
        self._build_details = {}
        try:
            frame = await self.compute("19000101", "20991231")
            _require(
                frame is not None and not frame.empty, "feature build returned no rows"
            )

            import asyncpg

            connection = await asyncpg.connect(
                self._db_manager.connection_string,
                command_timeout=7200,
            )
            try:
                columns = list(frame.columns)
                records = [
                    tuple(_copy_value(value) for value in row)
                    for row in frame.itertuples(index=False, name=None)
                ]
                async with connection.transaction():
                    await connection.execute(f"TRUNCATE TABLE {self.full_name}")
                    await connection.copy_records_to_table(
                        self.view_name,
                        schema_name=self.schema,
                        records=records,
                        columns=columns,
                    )
            finally:
                await connection.close()

            duration = time.monotonic() - started_clock
            await log_mv_refresh(
                self._db_manager,
                view_name=self.view_name,
                schema_name=self.schema,
                refresh_strategy="full_atomic",
                success=True,
                duration_seconds=duration,
                row_count=len(frame),
                details=self._build_details,
            )
            return {
                "status": "success",
                "view_name": self.view_name,
                "view_schema": self.schema,
                "full_name": self.full_name,
                "row_count": len(frame),
                "duration_seconds": duration,
                "refresh_strategy": "full_atomic",
                "strategy": "full",
                "details": self._build_details,
            }
        except Exception as exc:
            duration = time.monotonic() - started_clock
            await log_mv_refresh(
                self._db_manager,
                view_name=self.view_name,
                schema_name=self.schema,
                refresh_strategy="full_atomic",
                success=False,
                duration_seconds=duration,
                row_count=0,
                error_message=f"{type(exc).__name__}: {exc}",
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
                details=self._build_details or None,
            )
            raise


@feature_register
class AllAIndexDailyFeature(_AuditedAtomicFullRefreshFeature):
    """Daily self-built all-A total-return index and quality facts."""

    name = "all_a_index_daily"
    materialized_view_name = "all_a_index_daily"
    description = "自建全A自由流通市值加权全收益近似指数（日频）"
    category = "index"
    date_column = "trade_date"
    primary_keys = ("series_id", "trade_date")
    source_tables = [
        "rawdata.stock_daily",
        "rawdata.stock_dailybasic",
        "rawdata.stock_basic",
        "rawdata.stock_code_mapping",
        "rawdata.stock_dividend",
        "rawdata.others_calendar",
    ]
    quality_checks = {
        "primary_key": ["series_id", "trade_date"],
        "calendar_complete": True,
        "positive_index_level": True,
        "monthly_open_complete": True,
        "causal_lagged_weight": True,
    }

    def get_create_sql(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS features.all_a_index_daily (
            series_id VARCHAR(48) NOT NULL,
            variant VARCHAR(32) NOT NULL,
            trade_date DATE NOT NULL,
            open DOUBLE PRECISION,
            close DOUBLE PRECISION NOT NULL,
            total_return DOUBLE PRECISION NOT NULL,
            is_seed BOOLEAN NOT NULL,
            constituents INTEGER,
            eligible_names INTEGER,
            eligible_without_price INTEGER,
            missing_free_names INTEGER,
            unknown_float_names INTEGER,
            free_cap_coverage_lower DOUBLE PRECISION,
            known_free_cap_10k DOUBLE PRECISION,
            missing_free_float_cap_upper_10k DOUBLE PRECISION,
            quoted_held_names INTEGER,
            unquoted_held_weight DOUBLE PRECISION,
            fallback_weight DOUBLE PRECISION,
            bse_weight DOUBLE PRECISION,
            top_weight DOUBLE PRECISION,
            source_data_as_of DATE NOT NULL,
            methodology_version VARCHAR(64) NOT NULL,
            calculation_run_id UUID NOT NULL,
            calculated_at TIMESTAMP WITH TIME ZONE NOT NULL,
            PRIMARY KEY (series_id, trade_date),
            CONSTRAINT all_a_index_variant_check
                CHECK (variant IN ('strict_free', 'float_fallback')),
            CONSTRAINT all_a_index_close_check CHECK (close > 0),
            CONSTRAINT all_a_index_return_check CHECK (total_return > -1)
        )
        """.strip()

    def get_post_create_sqls(self) -> list[str]:
        return [
            "CREATE INDEX IF NOT EXISTS idx_all_a_index_daily_trade_date "
            "ON features.all_a_index_daily (trade_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_all_a_index_daily_variant_date "
            "ON features.all_a_index_daily (variant, trade_date DESC)",
            "COMMENT ON TABLE features.all_a_index_daily IS "
            "'自建全A指数事实；不是官方万得指数，也不包含ETF策略状态'",
            "COMMENT ON COLUMN features.all_a_index_daily.open IS "
            "'每个自然月首个交易日的指数开盘参考点位；其他交易日为NULL'",
            "COMMENT ON COLUMN features.all_a_index_daily.free_cap_coverage_lower IS "
            "'自由流通市值覆盖下界；存在无法估计的流通股本时为NULL'",
            "COMMENT ON COLUMN features.all_a_index_daily.source_data_as_of IS "
            "'本次构建所用股票日线和每日指标的共同数据水位'",
        ]

    async def _source_bounds(self) -> dict[str, date]:
        rows = await self._db_manager.fetch(
            """
            SELECT
                (SELECT MIN(trade_date) FROM rawdata.stock_daily) AS price_start,
                (SELECT MAX(trade_date) FROM rawdata.stock_daily) AS price_end,
                (SELECT MIN(trade_date) FROM rawdata.stock_dailybasic) AS basic_start,
                (SELECT MAX(trade_date) FROM rawdata.stock_dailybasic) AS basic_end,
                (SELECT MAX(ex_date) FROM rawdata.stock_dividend
                  WHERE div_proc = '实施') AS dividend_end
            """
        )
        _require(bool(rows), "cannot read source watermarks")
        result = dict(rows[0])
        _require(
            all(
                result.get(key) is not None
                for key in ("price_start", "price_end", "basic_start", "basic_end")
            ),
            "source watermark is missing",
        )
        return result

    async def _calendar(self, start: date, end: date) -> pd.DatetimeIndex:
        rows = await self._db_manager.fetch(
            """
            SELECT cal_date
            FROM rawdata.others_calendar
            WHERE exchange = 'SSE'
              AND is_open = 1
              AND cal_date >= $1
              AND cal_date <= $2
            ORDER BY cal_date
            """,
            start,
            end + timedelta(days=400),
        )
        frame = _records_to_frame(rows)
        _require(not frame.empty, "exchange calendar is empty")
        calendar = pd.DatetimeIndex(pd.to_datetime(frame["cal_date"])).sort_values()
        _require(not calendar.has_duplicates, "duplicate exchange sessions")
        return calendar

    async def _master_and_mapping(
        self, calendar: pd.DatetimeIndex
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
        master = _records_to_frame(
            await self._db_manager.fetch(
                """
                SELECT ts_code, name, fullname, market, exchange, curr_type,
                       list_status, list_date, delist_date
                FROM rawdata.stock_basic
                ORDER BY ts_code
                """
            )
        )
        mapping = _records_to_frame(
            await self._db_manager.fetch(
                """
                SELECT ts_code_old, ts_code_new, switch_date
                FROM rawdata.stock_code_mapping
                ORDER BY ts_code_old
                """
            )
        )
        mapping["origin"] = "rawdata.stock_code_mapping"
        manual = _manual_aliases().assign(origin="reviewed_identity_repair")
        mapping = pd.concat([mapping, manual], ignore_index=True)
        mapping["switch_date"] = pd.to_datetime(mapping["switch_date"])
        _require(not mapping["ts_code_old"].duplicated().any(), "ambiguous old code")
        _require(not mapping["ts_code_new"].duplicated().any(), "ambiguous new code")
        aliases = dict(zip(mapping["ts_code_old"], mapping["ts_code_new"]))
        _require(
            not set(aliases).intersection(aliases.values()),
            "chained identity mapping requires explicit resolution",
        )
        _require(
            set(aliases.values()).issubset(set(master["ts_code"])),
            "mapped security is absent from stock master",
        )

        master = master[
            master["curr_type"].eq("CNY")
            & master["exchange"].isin(["SSE", "SZSE", "BSE"])
        ].copy()
        _require(not master["ts_code"].duplicated().any(), "duplicate stock master")
        _require(
            not master["ts_code"].str.match(r"^(200|201|900)").any(),
            "B shares leaked into A-share master",
        )
        master["quote_from"] = pd.to_datetime(master["list_date"])
        master["eligible_until"] = pd.to_datetime(master["delist_date"]).fillna(
            pd.Timestamp("2099-12-31")
        )
        bse = master["exchange"].eq("BSE")
        master.loc[bse, "quote_from"] = master.loc[bse, "quote_from"].clip(
            lower=BSE_FIRST_SESSION
        )
        _require(master["quote_from"].notna().all(), "unknown listing boundary")

        eligible_from = master["quote_from"].copy()
        needs_buffer = master["quote_from"].ge(calendar.min()) & ~(
            bse & master["quote_from"].eq(BSE_FIRST_SESSION)
        )
        positions = (
            calendar.searchsorted(
                pd.DatetimeIndex(master.loc[needs_buffer, "quote_from"]), side="left"
            )
            + NEW_LISTING_BUFFER_SESSIONS
        )
        _require(
            (positions < len(calendar)).all(), "calendar cannot resolve IPO buffer"
        )
        eligible_from.loc[needs_buffer] = calendar[positions].to_numpy()
        master["eligible_from"] = eligible_from
        _require(
            master["eligible_from"].ge(master["quote_from"]).all(),
            "invalid IPO inclusion boundary",
        )
        delisted = master["list_status"].eq("D")
        _require(
            master.loc[delisted, "delist_date"].notna().all(),
            "delisted security lacks delisting date",
        )
        master = master.sort_values("ts_code").reset_index(drop=True)
        master["stock_id"] = np.arange(len(master), dtype=int)
        return master, mapping, aliases

    async def _distributions(
        self,
        master: pd.DataFrame,
        aliases: dict[str, str],
        start: date,
        end: date,
    ) -> tuple[dict[pd.Timestamp, pd.DataFrame], dict[str, int]]:
        frame = _records_to_frame(
            await self._db_manager.fetch(
                """
                SELECT ts_code, ann_date, imp_ann_date, ex_date,
                       cash_div_tax::double precision AS cash_div_tax,
                       stk_div::double precision AS stk_div
                FROM rawdata.stock_dividend
                WHERE div_proc = '实施'
                  AND ex_date >= $1
                  AND ex_date <= $2
                ORDER BY ex_date, ts_code
                """,
                start,
                end,
            )
        )
        if frame.empty:
            return {}, {"eligible": 0, "late_or_unknown": 0}
        for column in ("ann_date", "imp_ann_date", "ex_date"):
            frame[column] = pd.to_datetime(frame[column])
        frame["known_date"] = frame["imp_ann_date"].fillna(frame["ann_date"])
        frame["known_on_ex_date"] = frame["known_date"].notna() & frame[
            "known_date"
        ].le(frame["ex_date"])
        frame["security"] = frame["ts_code"].map(aliases).fillna(frame["ts_code"])
        frame = frame[~frame["security"].str.match(r"^(200|201|900)")].copy()
        duplicate = frame[frame.duplicated(["security", "ex_date"], keep=False)]
        if not duplicate.empty:
            spans = duplicate.groupby(["security", "ex_date"])[
                ["cash_div_tax", "stk_div"]
            ].agg(lambda values: values.max() - values.min())
            _require(
                spans.fillna(0).le(1e-8).all().all(),
                "conflicting distribution aliases",
            )
        frame = frame.sort_values(
            ["known_on_ex_date", "known_date"], ascending=[False, True]
        ).drop_duplicates(["security", "ex_date"])
        frame["cash_div_tax"] = frame["cash_div_tax"].fillna(0.0)
        frame["stk_div"] = frame["stk_div"].fillna(0.0)
        _require(
            frame[["cash_div_tax", "stk_div"]].ge(0).all().all(),
            "negative distribution field",
        )
        frame = frame.merge(
            master[["ts_code", "stock_id", "quote_from", "eligible_until"]],
            left_on="security",
            right_on="ts_code",
            how="left",
            suffixes=("_raw", "_master"),
        )
        missing = frame.loc[frame["stock_id"].isna(), "security"].drop_duplicates()
        _require(
            missing.empty,
            f"distribution security lacks identity: {missing.head().tolist()}",
        )
        frame["eligible_event"] = frame["ex_date"].ge(frame["quote_from"]) & frame[
            "ex_date"
        ].lt(frame["eligible_until"])
        eligible = frame[frame["eligible_event"]]
        known = eligible[eligible["known_on_ex_date"]]
        by_date = {
            pd.Timestamp(key): part for key, part in known.groupby("ex_date", sort=True)
        }
        return by_date, {
            "eligible": int(len(eligible)),
            "late_or_unknown": int((~eligible["known_on_ex_date"]).sum()),
        }

    async def _daily_batch(self, start: date, end: date) -> pd.DataFrame:
        rows = await self._db_manager.fetch(
            """
            WITH prices AS (
                SELECT ts_code, trade_date,
                       open::double precision AS open,
                       close::double precision AS close,
                       pre_close::double precision AS pre_close
                FROM rawdata.stock_daily
                WHERE trade_date >= $1 AND trade_date <= $2
            ), basics AS (
                SELECT ts_code, trade_date,
                       free_share::double precision AS free_share,
                       float_share::double precision AS float_share
                FROM rawdata.stock_dailybasic
                WHERE trade_date >= $1 AND trade_date <= $2
            )
            SELECT COALESCE(prices.ts_code, basics.ts_code) AS ts_code,
                   COALESCE(prices.trade_date, basics.trade_date) AS trade_date,
                   prices.open, prices.close, prices.pre_close,
                   basics.free_share, basics.float_share
            FROM prices
            FULL OUTER JOIN basics USING (ts_code, trade_date)
            ORDER BY trade_date, ts_code
            """,
            start,
            end,
        )
        return _records_to_frame(rows)

    async def compute(self, start_date: str, end_date: str) -> pd.DataFrame:
        del start_date, end_date  # this path-dependent feature always rebuilds in full
        watermarks = await self._source_bounds()
        source_start = max(watermarks["price_start"], watermarks["basic_start"])
        source_end = min(watermarks["price_end"], watermarks["basic_end"])
        _require(source_start <= source_end, "invalid source date range")

        calendar_full = await self._calendar(source_start, source_end)
        calendar = calendar_full[
            (calendar_full >= pd.Timestamp(source_start))
            & (calendar_full <= pd.Timestamp(source_end))
        ]
        _require(len(calendar) > 1, "insufficient exchange calendar")
        master, mapping, aliases = await self._master_and_mapping(calendar_full)
        distributions, distribution_stats = await self._distributions(
            master, aliases, source_start, source_end
        )

        stock_lookup = master.set_index("ts_code")["stock_id"]
        n_stocks = len(master)
        quote_from = master["quote_from"].to_numpy(dtype="datetime64[ns]")
        eligible_from = master["eligible_from"].to_numpy(dtype="datetime64[ns]")
        eligible_until = master["eligible_until"].to_numpy(dtype="datetime64[ns]")
        bse = master["exchange"].eq("BSE").to_numpy()

        price = np.zeros(n_stocks)
        free = np.zeros(n_stocks)
        floating = np.zeros(n_stocks)
        levels = {variant: 1000.0 for variant in SERIES_IDS}
        first_sessions = set(
            pd.Series(calendar, index=calendar.to_period("M")).groupby(level=0).first()
        )
        rows: list[dict[str, Any]] = []
        alias_audits: list[dict[str, Any]] = []
        seeded = False
        run_id = uuid.uuid4()
        calculated_at = datetime.now(timezone.utc)

        for year in range(source_start.year, source_end.year + 1):
            batch_start = max(source_start, date(year, 1, 1))
            batch_end = min(source_end, date(year, 12, 31))
            frame = await self._daily_batch(batch_start, batch_end)
            if frame.empty:
                continue
            frame = frame[
                pd.to_datetime(frame["trade_date"]).isin(calendar)
                & ~frame["ts_code"].str.match(r"^(200|201|900)")
            ].copy()
            frame, audit = _canonical_rows(
                frame,
                aliases,
                mapping,
                ["open", "close", "pre_close", "free_share", "float_share"],
                str(year),
            )
            alias_audits.append(audit)
            frame["stock_id"] = frame["security"].map(stock_lookup)
            unknown = frame.loc[frame["stock_id"].isna(), "security"].drop_duplicates()
            _require(
                unknown.empty,
                f"unresolved historical stock identity: {unknown.head().tolist()}",
            )

            for session, group in frame.groupby("trade_date", sort=True):
                session = pd.Timestamp(session)
                timestamp = np.datetime64(session, "ns")
                data_eligible = (quote_from <= timestamp) & (timestamp < eligible_until)
                eligible = (eligible_from <= timestamp) & (timestamp < eligible_until)

                close = np.full(n_stocks, np.nan)
                pre_close = np.full(n_stocks, np.nan)
                opening = np.full(n_stocks, np.nan)
                fresh_free = np.full(n_stocks, np.nan)
                fresh_float = np.full(n_stocks, np.nan)
                stock_ids = group["stock_id"].to_numpy(dtype=int)
                close[stock_ids] = pd.to_numeric(group["close"], errors="coerce")
                pre_close[stock_ids] = pd.to_numeric(
                    group["pre_close"], errors="coerce"
                )
                opening[stock_ids] = pd.to_numeric(group["open"], errors="coerce")
                fresh_free[stock_ids] = pd.to_numeric(
                    group["free_share"], errors="coerce"
                )
                fresh_float[stock_ids] = pd.to_numeric(
                    group["float_share"], errors="coerce"
                )

                quoted = data_eligible & np.isfinite(close) & (close > 0)
                if not seeded:
                    price[quoted] = close[quoted]
                    valid_free = (
                        data_eligible & np.isfinite(fresh_free) & (fresh_free > 0)
                    )
                    valid_float = (
                        data_eligible & np.isfinite(fresh_float) & (fresh_float > 0)
                    )
                    free[valid_free] = fresh_free[valid_free]
                    floating[valid_float] = fresh_float[valid_float]
                    for variant, series_id in SERIES_IDS.items():
                        rows.append(
                            {
                                "series_id": series_id,
                                "variant": variant,
                                "trade_date": session.date(),
                                "open": None,
                                "close": 1000.0,
                                "total_return": 0.0,
                                "is_seed": True,
                                "constituents": None,
                                "eligible_names": None,
                                "eligible_without_price": None,
                                "missing_free_names": None,
                                "unknown_float_names": None,
                                "free_cap_coverage_lower": None,
                                "known_free_cap_10k": None,
                                "missing_free_float_cap_upper_10k": None,
                                "quoted_held_names": None,
                                "unquoted_held_weight": None,
                                "fallback_weight": None,
                                "bse_weight": None,
                                "top_weight": None,
                                "source_data_as_of": source_end,
                                "methodology_version": METHOD_VERSION,
                                "calculation_run_id": run_id,
                                "calculated_at": calculated_at,
                            }
                        )
                    seeded = True
                    continue

                cash = np.zeros(n_stocks)
                bonus = np.zeros(n_stocks)
                events = distributions.get(session)
                if events is not None:
                    event_ids = events["stock_id"].to_numpy(dtype=int)
                    cash[event_ids] = events["cash_div_tax"].to_numpy(dtype=float)
                    bonus[event_ids] = events["stk_div"].to_numpy(dtype=float)

                true_free = free.copy()
                results: dict[str, dict[str, Any]] = {}
                for variant, series_id in SERIES_IDS.items():
                    used_free = (
                        true_free
                        if variant == PRIMARY_VARIANT
                        else np.where(true_free > 0, true_free, floating)
                    )
                    result = _day_step(
                        price,
                        used_free,
                        floating,
                        eligible,
                        data_eligible,
                        close,
                        pre_close,
                        fresh_free,
                        fresh_float,
                        cash,
                        bonus,
                    )
                    results[variant] = result
                    weights = result["weights"]
                    capital = result["capital"]
                    held = capital > 0

                    index_open = None
                    if session in first_sessions:
                        index_open = _reference_open_level(
                            levels[variant],
                            weights,
                            data_eligible,
                            opening,
                            pre_close,
                        )
                    levels[variant] *= 1.0 + float(result["reference_return"])

                    unpriced = eligible & (price <= 0)
                    missing_free = eligible & (price > 0) & (true_free <= 0)
                    unknown_float = missing_free & (floating <= 0)
                    known_cap = float(
                        np.sum(np.where(eligible & (price > 0), price * true_free, 0.0))
                    )
                    missing_float_cap = float(
                        np.sum(np.where(missing_free, price * floating, 0.0))
                    )
                    denominator = known_cap + missing_float_cap
                    coverage = (
                        known_cap / denominator
                        if denominator > 0
                        and not unknown_float.any()
                        and not unpriced.any()
                        else None
                    )
                    rows.append(
                        {
                            "series_id": series_id,
                            "variant": variant,
                            "trade_date": session.date(),
                            "open": index_open,
                            "close": levels[variant],
                            "total_return": float(result["reference_return"]),
                            "is_seed": False,
                            "constituents": int(held.sum()),
                            "eligible_names": int(eligible.sum()),
                            "eligible_without_price": int(unpriced.sum()),
                            "missing_free_names": int(missing_free.sum()),
                            "unknown_float_names": int(unknown_float.sum()),
                            "free_cap_coverage_lower": coverage,
                            "known_free_cap_10k": known_cap,
                            "missing_free_float_cap_upper_10k": missing_float_cap,
                            "quoted_held_names": int((held & result["quoted"]).sum()),
                            "unquoted_held_weight": float(
                                weights[~result["quoted"]].sum()
                            ),
                            "fallback_weight": float(weights[true_free <= 0].sum()),
                            "bse_weight": float(weights[bse].sum()),
                            "top_weight": float(weights.max()),
                            "source_data_as_of": source_end,
                            "methodology_version": METHOD_VERSION,
                            "calculation_run_id": run_id,
                            "calculated_at": calculated_at,
                        }
                    )

                primary = results[PRIMARY_VARIANT]
                price = primary["next_price"]
                free = primary["next_shares"]
                floating = primary["next_float"]

            self.logger.info(
                "自建全A已计算至 %s，累计输出 %s 行",
                batch_end,
                len(rows),
            )

        output = pd.DataFrame(rows)
        _require(not output.empty, "all-A index output is empty")
        expected_dates = [session.date() for session in calendar]
        for variant, group in output.groupby("variant", sort=False):
            actual_dates = group.sort_values("trade_date")["trade_date"].tolist()
            _require(
                actual_dates == expected_dates,
                f"{variant} output misses exchange sessions",
            )
            _require(group["close"].gt(0).all(), f"{variant} has nonpositive level")
            non_seed_month_starts = _non_seed_month_starts(group)
            _require(
                non_seed_month_starts["open"].notna().all()
                and non_seed_month_starts["open"].gt(0).all(),
                f"{variant} is missing a monthly opening level",
            )

        digest_columns = [
            "series_id",
            "trade_date",
            "close",
            "total_return",
            "constituents",
            "free_cap_coverage_lower",
        ]
        output_digest = hashlib.sha256(
            pd.util.hash_pandas_object(
                output[digest_columns], index=False
            ).values.tobytes()
        ).hexdigest()
        primary = output[output["variant"].eq(PRIMARY_VARIANT)]
        self._build_details = {
            "calculation_run_id": str(run_id),
            "methodology_version": METHOD_VERSION,
            "source_watermarks": {key: str(value) for key, value in watermarks.items()},
            "source_data_as_of": str(source_end),
            "first_trade_date": str(source_start),
            "last_trade_date": str(source_end),
            "series_ids": SERIES_IDS,
            "rows_per_series": int(len(calendar)),
            "canonical_master_names": int(len(master)),
            "manual_identity_repairs": len(MANUAL_ALIAS_ROWS),
            "alias_identity_days_merged": int(
                sum(item.get("duplicate_identity_days", 0) for item in alias_audits)
            ),
            "distribution_events": distribution_stats,
            "latest_primary_constituents": int(primary.iloc[-1]["constituents"]),
            "latest_primary_coverage_lower": primary.iloc[-1][
                "free_cap_coverage_lower"
            ],
            "output_sha256": output_digest,
            "definition_limit": (
                "Current AlphaDB historical vintage; not an exact licensed Wind "
                "index or a daily archived vendor PIT vintage."
            ),
        }
        return output.sort_values(["series_id", "trade_date"]).reset_index(drop=True)
