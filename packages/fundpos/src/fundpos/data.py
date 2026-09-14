from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import psycopg

from .config import Settings
from .constants import SW_CODES
from .convertible_dominant import convertible_total_return_levels
from .errors import DataUnavailable
from .factors import cash_returns, returns_from_prices, returns_from_prices_on_calendar
from .fixed_income_data import (
    apply_report_evidence,
    classify_bond_disclosures,
    recover_asset_control_announcements,
    recover_report_announcements,
)
from .normalization import (
    historical_membership_fallback,
    normalize_holdings,
    separate_nav_availability,
)
from .pit import dates, recover_holding_announcements
from .storage import atomic_json, atomic_parquet, file_hash, frame_hash

FIXED_INCOME_INDEX_CODES = {
    "N11099": "rate_short",
    "N11075": "rate_long",
    "N31199": "credit_short",
    "N31203": "credit_long",
    "N31078": "convertible_bond",
}

# The vendor fund metadata uses the exchange-style suffix for 000846 while the
# official constituent history uses the CSI suffix.  This is a single audited
# alias, not a generic suffix rewrite rule.
TRACKING_INDEX_ALIASES = {"000846.SH": "000846.CSI"}

# CNY total-return counterpart of 930933.CSI.  The price index remains accepted
# only as a dated legacy supplement; new v3 snapshots use the official total-
# return series maintained by AlphaHome.
HK_TOTAL_RETURN_INDEX_CODE = "H20933"

# Broad non-convertible bond return used by the equity-family attribution model.
# N11009 is the interest-and-reinvestment version of the official CSI Aggregate
# Bond Index.  It replaces the legacy workbook series that ended in July 2024.
BOND_TOTAL_RETURN_INDEX_CODE = "N11009"


def financing_cost_returns(dr007: pd.DataFrame, repo_rates: pd.DataFrame,
                           calendar: pd.DatetimeIndex) -> pd.DataFrame:
    """Preserve hashed DR007 history and extend it with prior-known live FR007."""
    columns = ["date", "annual_rate_pct", "source", "return_basis", "source_hash",
               "evidence_status", "priority"]
    historical = dates(dr007, ("date",)).copy()
    if not historical.empty:
        historical = historical.assign(
            source="rawdata.macro_dr007_history:L001619493",
            return_basis="prior_known_DR007_ACT365_CNY_conditional_vendor_cache",
            source_hash=historical.get("source_workbook_sha256"),
            evidence_status=historical.get(
                "evidence_status",
                pd.Series("historical_vendor_cache_no_live_api", index=historical.index),
            ), priority=0)
    live = dates(repo_rates, ("date",)).copy()
    if not live.empty:
        live = live.rename(columns={"fr007": "annual_rate_pct"}).assign(
            source="rawdata.macro_repo_rate:fr007",
            return_basis="prior_known_FR007_ACT365_CNY_live",
            source_hash=None, evidence_status="live_alphadb_fr007_prior_day_proxy", priority=1)
    pieces = [frame[columns] for frame in (historical, live) if not frame.empty]
    combined = (pd.concat(pieces, ignore_index=True).sort_values(["date", "priority"])
                .drop_duplicates("date", keep="first") if pieces
                else pd.DataFrame(columns=columns))
    result = cash_returns(combined[["date", "annual_rate_pct"]], calendar)
    result["asset"], result["ann_date"] = "financing_cost", result.date
    metadata_columns = ["source", "return_basis", "source_hash", "evidence_status"]
    metadata = combined.set_index("date")[metadata_columns].copy()
    metadata["source_hash"] = metadata.source_hash.fillna("")
    used = metadata.reindex(metadata.index.union(calendar).sort_values()).ffill()
    used = used.reindex(calendar).shift(1)
    used.loc[result["return"].isna().to_numpy(), :] = None
    for column in metadata_columns:
        result[column] = used[column].to_numpy()
    result["source_hash"] = result.source_hash.replace("", None)
    return result


def canonicalize_convertible_holding_codes(holdings: pd.DataFrame,
                                           master: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Resolve exchange suffixes with the official CB master and remove source twins."""
    out = holdings.copy()
    stats = {"input_rows": len(out), "output_rows": len(out), "rewritten_codes": 0,
             "duplicate_rows_removed": 0, "unresolved_codes": 0,
             "source": "rawdata.cbond_basic:unique_numeric_code"}
    if out.empty:
        return out, stats
    if "ts_code" not in master or "security_code" not in out:
        raise DataUnavailable("CBOND_CODE_MASTER_SCHEMA", "ts_code and security_code required")
    codes = master.ts_code.dropna().astype(str).str.upper().drop_duplicates()
    lookup = pd.DataFrame({"canonical": codes})
    lookup["numeric_code"] = lookup.canonical.str.extract(r"(\d{6})", expand=False)
    unique = lookup.groupby("numeric_code").canonical.agg(list)
    mapping = {key: values[0] for key, values in unique.items() if key and len(values) == 1}
    out["security_code_original"] = out.security_code
    numeric = out.security_code.astype("string").str.extract(r"(\d{6})", expand=False)
    canonical = numeric.map(mapping)
    resolved = canonical.notna()
    out.loc[resolved, "security_code"] = canonical.loc[resolved]
    stats["rewritten_codes"] = int(
        (resolved & out.security_code_original.astype("string").ne(out.security_code)).sum()
    )
    stats["unresolved_codes"] = int((~resolved).sum())
    keys = [key for key in (
        "fund_code", "report_date", "security_code", "quantity", "market_value", "weight", "rank_no"
    ) if key in out]
    before = len(out)
    out["_canonical_match"] = out.security_code_original.astype("string").eq(out.security_code)
    order = [key for key in ("fund_code", "report_date", "rank_no", "security_code",
                             "_canonical_match", "cbond_code_raw") if key in out]
    ascending = [True] * len(order)
    if "_canonical_match" in order:
        ascending[order.index("_canonical_match")] = False
    out = out.sort_values(order, ascending=ascending).drop_duplicates(keys, keep="first")
    out = out.drop(columns="_canonical_match").reset_index(drop=True)
    stats["output_rows"] = len(out)
    stats["duplicate_rows_removed"] = before - len(out)
    return out, stats

TABLES = (
    "funds",
    "classification",
    "nav",
    "calendar",
    "factors",
    "holdings",
    "disclosed",
    "membership",
    "prices",
    "allocations",
    "asset_reports",
    "constraints",
    "stock_groups",
    "turnover",
    "financial_reports",
    "membership_fallback",
    "bond_holdings",
    "cbond_holdings",
    "cbond_prices",
    "bond_allocations",
    "fixed_income_factors",
    "contract_evidence",
    "nav_observations",
    "report_evidence",
    "tracking_index_history",
    "index_membership",
    "industry_market_weights",
)


@dataclass
class DataBundle:
    frames: dict[str, pd.DataFrame]
    provenance: dict = field(default_factory=dict)

    def __getitem__(self, name: str) -> pd.DataFrame:
        return self.frames.get(name, pd.DataFrame()).copy()

    @property
    def fingerprint(self) -> str:
        import hashlib

        hashes = {key: frame_hash(value) for key, value in sorted(self.frames.items())}
        return hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()

    def save(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        hashes = {}
        for name, frame in self.frames.items():
            path = directory / f"{name}.parquet"
            atomic_parquet(path, frame)
            hashes[name] = file_hash(path)
        # Arrow may normalize object/list dtypes.  The persisted content is the
        # reproducibility boundary, so fingerprint the round-tripped frames.
        persisted = {
            name: pd.read_parquet(directory / f"{name}.parquet")
            for name in self.frames
        }
        canonical_fingerprint = DataBundle(persisted, self.provenance).fingerprint
        atomic_json(
            directory / "manifest.json",
            {
                "provenance": self.provenance,
                "files": hashes,
                "fingerprint": canonical_fingerprint,
                "fingerprint_basis": "parquet_roundtrip_frames_v1",
            },
        )

    @classmethod
    def load(cls, directory: Path) -> DataBundle:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        frames = {}
        for name, expected in manifest["files"].items():
            if name not in TABLES:
                raise DataUnavailable("INPUT_SCHEMA", name)
            path = directory / f"{name}.parquet"
            if file_hash(path) != expected:
                raise DataUnavailable("INPUT_HASH_CHANGED", str(path))
            frames[name] = pd.read_parquet(path)
        result = cls(frames, manifest["provenance"])
        if result.fingerprint != manifest["fingerprint"]:
            raise DataUnavailable("INPUT_FINGERPRINT_CHANGED", "Snapshot content mismatch")
        return result


class AlphaDB:
    """Only SELECT statements, read-only transactions, bounded waits, no credential logs."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def connect(self):
        path = Path(self.settings.values["data"]["alphahome_config"]).expanduser()
        config = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else {}
        dsn = os.environ.get("FUNDPOS_DATABASE_URL") or config.get("database", {}).get("url")
        if not dsn:
            raise DataUnavailable(
                "NO_DATABASE_CONFIG", "Configure FUNDPOS_DATABASE_URL or AlphaHome config"
            )
        seconds = self.settings.values["data"]["statement_timeout_seconds"]
        return psycopg.connect(
            dsn,
            connect_timeout=10,
            options=f"-c default_transaction_read_only=on -c statement_timeout={int(seconds * 1000)}",
        )

    def query(self, sql: str, params=None) -> pd.DataFrame:
        if not sql.lstrip().upper().startswith(("SELECT", "WITH")):
            raise ValueError("Read-only query required")
        try:
            with self.connect() as connection, connection.cursor() as cursor:
                cursor.execute(sql, params)
                columns = [item.name for item in cursor.description]
                records = cursor.fetchall()
            frame = pd.DataFrame(records, columns=columns)
            for col in frame:
                if (
                    frame[col].dtype == object
                    and frame[col]
                    .dropna()
                    .head(1)
                    .map(lambda x: type(x).__name__ == "Decimal")
                    .any()
                ):
                    frame[col] = pd.to_numeric(frame[col])
            return frame
        except psycopg.Error as exc:
            # psycopg's full error text can contain a DSN/SQL parameter. Do not print it.
            raise DataUnavailable("DATABASE_QUERY_FAILED", type(exc).__name__) from None

    def universe(self, category_codes: list[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        category_codes = category_codes or ["TSJJ020106", "TSJJ020306", "TSJJ020303"]
        funds = self.query(
            """SELECT ts_code fund_code, fund_name, invest_type category,
            COALESCE(fee_mode_main_ts_code,ts_code) master_code, share_class, found_date,
            liquidation_date, investment_scope, investment_objective, investment_strategy,
            active_passive, invest_region, tracking_index_ts_code, is_etf_feeder,
            etf_target_ts_code, latest_change_date
            FROM rawdata.fund_basic_ext WHERE ts_code IN
              (SELECT ts_code FROM rawdata.fund_classification_member WHERE attr_code=ANY(%s))""",
            (category_codes,),
        )
        classification = self.query(
            """SELECT ts_code fund_code, attr_code, attr_name category, in_date, out_date,
            latest_change_date, source_table_name source
            FROM rawdata.fund_classification_member WHERE attr_code=ANY(%s)""",
            (category_codes,),
        )
        return dates(funds, ("found_date", "liquidation_date")), dates(
            classification, ("in_date", "out_date")
        )

    def load(
        self, start, end, fund_codes: list[str] | None = None, *, fetch_prices=True
    ) -> DataBundle:
        start = pd.Timestamp(start).normalize()
        end = pd.Timestamp(end).normalize()
        warmup = start - pd.Timedelta(days=250)
        report_start = start - pd.Timedelta(days=600)
        category_codes = self.settings.values.get("universe", {}).get("category_codes")
        funds, classification = self.universe(category_codes)
        if fund_codes:
            funds = funds.loc[
                funds.fund_code.isin(fund_codes) | funds.master_code.isin(fund_codes)
            ].copy()
        codes = funds.fund_code.tolist()
        if not codes:
            raise DataUnavailable("EMPTY_FUND_SET", "No matching products in configured metadata")
        classification = classification.loc[classification.fund_code.isin(codes)]
        frames = {"funds": funds, "classification": classification}
        frames["calendar"] = self.query(
            """SELECT cal_date date FROM rawdata.others_calendar
            WHERE exchange='SSE' AND is_open=1 AND cal_date BETWEEN %s AND %s ORDER BY cal_date""",
            (warmup.date(), end.date()),
        )
        nav = self.query(
            """SELECT ts_code fund_code, nav_date date, ann_date, adj_nav, net_asset,
            total_netasset FROM rawdata.fund_nav WHERE ts_code=ANY(%s)
            AND nav_date BETWEEN %s AND %s ORDER BY ts_code,nav_date""",
            (codes, warmup.date(), end.date()),
        )
        frames["nav"] = separate_nav_availability(nav)
        frames["financial_reports"] = dates(self.query(
            """SELECT ts_code fund_code,report_date,ann_date,net_asset,unit_net_asset
            FROM rawdata.fund_financial_quarterly_ext WHERE ts_code=ANY(%s)
            AND report_date BETWEEN %s AND %s""",
            (codes, report_start.date(), end.date())), ("report_date", "ann_date"))
        sw = self.query(
            """SELECT trade_date date, ts_code asset, close,float_mv FROM rawdata.index_swdaily
            WHERE ts_code=ANY(%s) AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
            (list(SW_CODES), warmup.date(), end.date()),
        )
        market = sw[["date", "asset", "float_mv"]].dropna().copy()
        totals = market.groupby("date").float_mv.transform("sum")
        market["weight"] = market.float_mv / totals.where(totals.gt(0))
        market["source"] = "rawdata.index_swdaily:industry_float_mv"
        frames["industry_market_weights"] = market[
            ["date", "asset", "weight", "source"]
        ]
        sw_returns = returns_from_prices(sw[["date", "asset", "close"]])
        sw_returns["source"] = "rawdata.index_swdaily"
        sw_returns["return_basis"] = "SW_official_price_index_CNY"
        factors = sw_returns[
            ["date", "start_date", "asset", "return", "source", "return_basis"]
        ].copy()
        rates = self.query(
            """SELECT date,fr007 annual_rate_pct FROM rawdata.macro_repo_rate
            WHERE date BETWEEN %s AND %s ORDER BY date""",
            ((warmup - pd.Timedelta(days=14)).date(), end.date()),
        )
        rates = dates(rates, ("date",))
        calendar = pd.DatetimeIndex(pd.to_datetime(frames["calendar"].date))
        factors = pd.concat([factors, cash_returns(rates, calendar)], ignore_index=True)
        hk_prices = self.query(
            """SELECT trade_date date,close FROM rawdata.index_performance
            WHERE index_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
            (
                HK_TOTAL_RETURN_INDEX_CODE,
                (warmup - pd.Timedelta(days=14)).date(),
                end.date(),
            ),
        )
        if not hk_prices.empty:
            hk_prices["asset"] = "hk"
            hk_returns = returns_from_prices_on_calendar(hk_prices, calendar)
            hk_returns["source"] = (
                f"rawdata.index_performance:{HK_TOTAL_RETURN_INDEX_CODE}"
            )
            hk_returns["return_basis"] = (
                "CSIndex_official_total_return_CNY_HK_calendar"
            )
            factors = pd.concat([factors, hk_returns[factors.columns]], ignore_index=True)
        bond_prices = self.query(
            """SELECT trade_date date,close FROM rawdata.index_performance
            WHERE index_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
            (
                BOND_TOTAL_RETURN_INDEX_CODE,
                (warmup - pd.Timedelta(days=14)).date(),
                end.date(),
            ),
        )
        if not bond_prices.empty:
            bond_prices["asset"] = "bond"
            bond_returns = returns_from_prices_on_calendar(bond_prices, calendar)
            bond_returns["source"] = (
                f"rawdata.index_performance:{BOND_TOTAL_RETURN_INDEX_CODE}"
            )
            bond_returns["return_basis"] = (
                "CSIndex_official_interest_reinvestment_total_return_CNY"
            )
            factors = pd.concat([factors, bond_returns[factors.columns]], ignore_index=True)
        for asset, ticker in [("hk", "930933.CSI"), ("bond", "CBA00601.CS")]:
            benchmark = self.query(
                """SELECT trade_date date, close FROM rawdata.index_factor_pro
                WHERE ts_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date""",
                (ticker, warmup.date(), end.date()),
            )
            if not benchmark.empty:
                benchmark["asset"] = asset
                benchmark = returns_from_prices(benchmark)
                benchmark["source"] = f"rawdata.index_factor_pro:{ticker}"
                benchmark["return_basis"] = "CNY_benchmark"
                factors = pd.concat([factors, benchmark[factors.columns]], ignore_index=True)
        frames["factors"] = factors
        frames["factors"]["ann_date"] = pd.to_datetime(frames["factors"].date)
        holdings = self.query(
            """SELECT ts_code fund_code, report_date, ann_date,
            security_ts_code security_code, security_code_raw, security_name, market_value, quantity,
            nav_ratio_pct/100.0 weight, rank_no, source_table_name source
            FROM rawdata.fund_stock_holding_detail WHERE ts_code=ANY(%s)
            AND report_date BETWEEN %s AND %s""",
            (codes, report_start.date(), end.date()),
        )
        disclosed = self.query(
            """SELECT ts_code fund_code,end_date report_date,ann_date,
            symbol security_code,mkv market_value,amount quantity FROM rawdata.fund_portfolio
            WHERE ts_code=ANY(%s) AND end_date BETWEEN %s AND %s""",
            (codes, report_start.date(), end.date()),
        )
        frames["disclosed"] = dates(disclosed, ("report_date", "ann_date"))
        holdings = recover_holding_announcements(holdings, disclosed)
        frames["asset_reports"] = dates(
            self.query(
            """SELECT ts_code fund_code,report_date,ann_date,
            stock_nav_ratio_pct/100.0 stock_weight,net_asset_value aum,stock_market_value,
            bond_nav_ratio_pct/100.0 bond_weight,bond_market_value,
            fixed_income_nav_ratio_pct/100.0 fixed_income_weight,fixed_income_investment,
            total_asset_value,derivative_value,repo_sold_value,repo_sold_nav_ratio_pct/100.0 repo_sold_weight,
            other_asset_value,other_asset_nav_ratio_pct/100.0 other_asset_weight,
            source_table_name source FROM rawdata.fund_asset_alloc
            WHERE ts_code=ANY(%s) AND report_date BETWEEN %s AND %s""",
                (codes, report_start.date(), end.date()),
            ),
            ("report_date", "ann_date"),
        )
        frames["asset_reports"] = recover_asset_control_announcements(
            frames["asset_reports"], frames["financial_reports"]
        )
        holdings = normalize_holdings(holdings, frames["asset_reports"])
        frames["holdings"] = holdings
        frames["allocations"] = dates(
            self.query(
                """SELECT ts_code fund_code,report_date,ann_date,
            industry_name,nav_ratio_pct/100.0 weight FROM rawdata.fund_industry_alloc
            WHERE ts_code=ANY(%s) AND report_date BETWEEN %s AND %s""",
                (codes, report_start.date(), end.date()),
            ),
            ("report_date", "ann_date"),
        )
        frames["bond_holdings"] = dates(
            self.query(
                """SELECT ts_code fund_code,report_date,ann_date,bond_code_raw,
                bond_ts_code security_code,bond_name security_name,quantity,market_value,
                nav_ratio_pct/100.0 weight,rank_no,bond_type,is_convertible_period,
                source_table_name source FROM rawdata.fund_bond_holding_detail
                WHERE ts_code=ANY(%s) AND report_date BETWEEN %s AND %s""",
                (codes, report_start.date(), end.date()),
            ),
            ("report_date", "ann_date"),
        )
        cbond = self.query(
            """SELECT ts_code fund_code,report_date,cbond_code_raw,
            cbond_ts_code security_code,cbond_name security_name,quantity,market_value,
            nav_ratio_pct/100.0 weight,rank_no,source_table_name source
            FROM rawdata.fund_cbond_holding_detail WHERE ts_code=ANY(%s)
            AND report_date BETWEEN %s AND %s""",
            (codes, report_start.date(), end.date()),
        )
        cbond_master = self.query("SELECT ts_code FROM rawdata.cbond_basic")
        cbond, cbond_code_stats = canonicalize_convertible_holding_codes(cbond, cbond_master)
        cbond["ann_date"] = pd.NaT
        cbond["announcement_source"] = "source_has_no_announcement_field"
        frames["cbond_holdings"] = dates(cbond, ("report_date", "ann_date"))
        frames["cbond_prices"] = pd.DataFrame(
            columns=[
                "date",
                "security_code",
                "adjusted_close",
                "return",
                "ann_date",
                "source",
                "return_basis",
                "evidence_status",
                "close",
                "bond_value",
                "bond_over_rate",
                "cb_value",
                "cb_over_rate",
            ]
        )
        cbond_codes = cbond.security_code.dropna().unique().tolist()
        if fetch_prices and cbond_codes:
            cbond_daily = self.query(
                """SELECT trade_date date,ts_code security_code,pre_close,close,pct_chg,
                bond_value,bond_over_rate,cb_value,cb_over_rate
                FROM rawdata.cbond_daily WHERE ts_code=ANY(%s)
                AND trade_date BETWEEN %s AND %s ORDER BY ts_code,trade_date""",
                (cbond_codes, report_start.date(), end.date()),
            )
            frames["cbond_prices"] = convertible_total_return_levels(cbond_daily)
        frames["bond_allocations"] = dates(
            self.query(
                """SELECT ts_code fund_code,report_date,ann_date,bond_category,
                market_value,nav_ratio_pct/100.0 weight,bond_market_ratio_pct/100.0 bond_weight,
                source_table_name source FROM rawdata.fund_bond_alloc
                WHERE ts_code=ANY(%s) AND report_date BETWEEN %s AND %s""",
                (codes, report_start.date(), end.date()),
            ),
            ("report_date", "ann_date"),
        )
        frames["asset_reports"] = recover_report_announcements(
            frames["asset_reports"], frames["disclosed"]
        )
        frames["bond_allocations"] = recover_report_announcements(
            frames["bond_allocations"], frames["disclosed"]
        )
        frames["bond_holdings"] = recover_report_announcements(
            frames["bond_holdings"], frames["disclosed"]
        )
        frames["cbond_holdings"] = recover_report_announcements(
            frames["cbond_holdings"], frames["disclosed"]
        )
        frames["asset_reports"] = classify_bond_disclosures(
            frames["asset_reports"], frames["bond_allocations"]
        )
        factor_columns = [
            "date", "start_date", "asset", "return", "ann_date", "source",
            "return_basis", "source_hash", "evidence_status",
        ]
        fixed_prices = self.query(
            """SELECT trade_date date,index_code asset,close,source_url
            FROM rawdata.index_performance WHERE index_code=ANY(%s)
            AND trade_date BETWEEN %s AND %s ORDER BY index_code,trade_date""",
            (list(FIXED_INCOME_INDEX_CODES), warmup.date(), end.date()),
        )
        if not fixed_prices.empty:
            fixed_prices["asset"] = fixed_prices.asset.map(FIXED_INCOME_INDEX_CODES)
            fixed_prices = returns_from_prices(fixed_prices)
            fixed_prices["ann_date"] = fixed_prices.date
            fixed_prices["source"] = fixed_prices.asset.map(
                {value: f"rawdata.index_performance:{key}"
                 for key, value in FIXED_INCOME_INDEX_CODES.items()}
            )
            fixed_prices["return_basis"] = (
                "CSIndex_official_interest_reinvestment_total_return_CNY"
            )
            fixed_prices["source_hash"] = None
            fixed_prices["evidence_status"] = "official_total_return_series"
        else:
            fixed_prices = pd.DataFrame(columns=factor_columns)
        dr007 = self.query(
            """SELECT trade_date date,dr007_pct annual_rate_pct,
            availability_date_proxy ann_date,source_workbook_sha256,evidence_status
            FROM rawdata.macro_dr007_history WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date""",
            ((warmup - pd.Timedelta(days=14)).date(), end.date()),
        )
        repo_rates = self.query(
            """SELECT date,fr007 FROM rawdata.macro_repo_rate
            WHERE date BETWEEN %s AND %s ORDER BY date""",
            ((warmup - pd.Timedelta(days=14)).date(), end.date()),
        )
        dr007_hashes = sorted(set(dr007.source_workbook_sha256.dropna()))
        if len(dr007_hashes) > 1:
            raise DataUnavailable(
                "DR007_SOURCE_VERSION_CONFLICT", "Multiple workbook hashes in one snapshot"
            )
        financing_cost = financing_cost_returns(dr007, repo_rates, calendar)
        frames["fixed_income_factors"] = pd.concat(
            [fixed_prices[factor_columns], financing_cost[factor_columns]],
            ignore_index=True,
        )
        frames["contract_evidence"] = pd.DataFrame(
            columns=["fund_code", "ann_date", "effective_date", "document_hash", "source_uri"]
        )
        frames["nav_observations"] = pd.DataFrame(
            columns=["fund_code", "date", "first_observed_at", "source"]
        )
        # Current fund metadata is useful for a separately labelled conditional
        # comparator, but it is never promoted to strict PIT evidence.  Formal
        # publication still requires dated contract history.
        tracking = funds.loc[
            funds.tracking_index_ts_code.notna(),
            ["fund_code", "tracking_index_ts_code", "found_date"],
        ].copy()
        tracking["index_code"] = tracking.tracking_index_ts_code.replace(
            TRACKING_INDEX_ALIASES
        )
        tracking["effective_date"] = tracking.found_date
        tracking["ann_date"] = tracking.found_date
        tracking["source"] = "rawdata.fund_basic_ext:current_snapshot_conditional"
        tracking["evidence_status"] = "conditional_current_metadata"
        tracking["strict_pit"] = False
        tracking["ann_date_method"] = "fund_found_date_lower_bound_not_mapping_publication"
        frames["tracking_index_history"] = tracking[
            [
                "fund_code", "index_code", "effective_date", "ann_date", "source",
                "evidence_status", "strict_pit", "ann_date_method",
            ]
        ]
        tracking_codes = sorted(set(tracking.index_code.dropna()))
        members = self.query(
            """SELECT index_code,ts_code security_code,weight,obs_date as_of_date,
            source_available_date ann_date,weight_source,source_code,source_effective_date,
            source_quality,is_eligible,is_proxy,method_version
            FROM pit.pit_etf_index_members_monthly WHERE index_code=ANY(%s)
            AND obs_date BETWEEN %s AND %s AND is_eligible=true""",
            (tracking_codes, report_start.date(), end.date()),
        ) if tracking_codes else pd.DataFrame()
        if not members.empty:
            members["source"] = (
                "pit.pit_etf_index_members_monthly:" + members.weight_source.astype(str)
            )
        frames["index_membership"] = members
        frames["membership"] = dates(
            self.query(
                """SELECT ts_code security_code,l1_code industry,
            in_date,out_date FROM rawdata.index_swmember WHERE l1_code=ANY(%s)""",
                (list(SW_CODES),),
            ),
            ("in_date", "out_date"),
        )
        membership_events = self.query(
            """SELECT ts_code security_code,trade_date in_date,industry_source,industry_l1
            FROM rawdata.stock_industry_versioned WHERE industry_source='SWHY'
            AND ts_code=ANY(%s) AND trade_date<=%s""",
            (holdings.security_code.dropna().unique().tolist(), end.date()))
        frames["membership_fallback"] = historical_membership_fallback(membership_events)
        frames["prices"] = pd.DataFrame(columns=["date", "security_code", "adjusted_close"])
        securities = holdings.security_code.dropna().unique().tolist() if not holdings.empty else []
        a_securities = [s for s in securities if not s.endswith(".HK")]
        if fetch_prices and a_securities:
            frames["prices"] = dates(
                self.query(
                    """SELECT p.trade_date date,p.ts_code security_code,
                p.close*a.adj_factor adjusted_close FROM rawdata.stock_daily p
                JOIN rawdata.stock_adjfactor a ON p.ts_code=a.ts_code AND p.trade_date=a.trade_date
                WHERE p.ts_code=ANY(%s) AND p.trade_date BETWEEN %s AND %s""",
                    (a_securities, report_start.date(), end.date()),
                ),
                ("date",),
            )
        frames["constraints"] = pd.DataFrame(
            columns=[
                "fund_code",
                "ann_date",
                "effective_date",
                "stock_lower",
                "stock_upper",
                "hk_upper_equity",
                "source",
            ]
        )
        bundle = DataBundle(
            frames,
            {
                "provider": "alphadb",
                "start": str(start.date()),
                "end": str(end.date()),
                "loaded_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
                "history_kind": "announcement_date_reconstruction",
                "taxonomy": "SW2021",
                "universe_scope": "explicit_subset" if fund_codes else "all_configured_metadata",
                "requested_funds": fund_codes or [],
                "sql_mode": "read_only",
                "price_policy": "A-share adjusted CNY; HK personalized returns require an explicit CNY supplement",
                "classification_out_date": "first_excluded_date",
                "cbond_code_normalization": cbond_code_stats,
            },
        )
        bundle = self.apply_supplements(bundle)
        bundle.frames = apply_report_evidence(bundle.frames)
        return bundle

    def load_universe_audit(
        self, start, end, fund_codes: list[str] | None = None
    ) -> DataBundle:
        """Load only PIT scope evidence; do not scan prices or security holdings."""
        start, end = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
        category_codes = self.settings.values.get("universe", {}).get("category_codes")
        funds, classification = self.universe(category_codes)
        if fund_codes:
            funds = funds.loc[
                funds.fund_code.isin(fund_codes) | funds.master_code.isin(fund_codes)
            ].copy()
        codes = funds.fund_code.tolist()
        if not codes:
            raise DataUnavailable("EMPTY_FUND_SET", "No matching products in configured metadata")
        classification = classification.loc[classification.fund_code.isin(codes)]
        assets = dates(
            self.query(
                """SELECT ts_code fund_code,report_date,ann_date,
                stock_nav_ratio_pct/100.0 stock_weight,
                bond_nav_ratio_pct/100.0 bond_weight,
                net_asset_value aum,total_asset_value
                FROM rawdata.fund_asset_alloc WHERE ts_code=ANY(%s)
                AND report_date BETWEEN %s AND %s""",
                (codes, start.date(), end.date()),
            ),
            ("report_date", "ann_date"),
        )
        allocations = dates(
            self.query(
                """SELECT ts_code fund_code,report_date,ann_date,bond_category,
                nav_ratio_pct/100.0 weight FROM rawdata.fund_bond_alloc
                WHERE ts_code=ANY(%s) AND report_date BETWEEN %s AND %s""",
                (codes, start.date(), end.date()),
            ),
            ("report_date", "ann_date"),
        )
        frames = {name: pd.DataFrame() for name in TABLES}
        frames.update(
            funds=funds,
            classification=classification,
            asset_reports=classify_bond_disclosures(assets, allocations),
            bond_allocations=allocations,
            constraints=pd.DataFrame(
                columns=[
                    "fund_code",
                    "ann_date",
                    "effective_date",
                    "verified",
                    "fixed_income_primary",
                    "allows_equity_or_cbond",
                    "stock_denominator",
                    "stock_upper",
                ]
            ),
        )
        bundle = DataBundle(
            frames,
            {
                "provider": "alphadb",
                "start": str(start.date()),
                "end": str(end.date()),
                "loaded_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
                "history_kind": "strict_announcement_date_universe_audit",
                "taxonomy": "SW2021",
                "universe_scope": "explicit_subset" if fund_codes else "all_configured_metadata",
                "requested_funds": fund_codes or [],
                "sql_mode": "read_only",
                "detail_policy": "scope_evidence_only_no_security_holdings_or_prices",
            },
        )
        bundle = self.apply_supplements(bundle)
        bundle.frames = apply_report_evidence(bundle.frames)
        return bundle

    def apply_supplements(self, bundle: DataBundle) -> DataBundle:
        directory = self.settings.root / self.settings.values["data"]["supplements_dir"]
        manifest = directory / "manifest.json"
        if not manifest.exists():
            return bundle
        entries = json.loads(manifest.read_text(encoding="utf-8"))
        used = []
        for entry in entries.get("files", []):
            name = entry["table"]
            if name not in TABLES or name in ("funds", "classification"):
                raise DataUnavailable("SUPPLEMENT_SCHEMA", name)
            path = (directory / entry["file"]).resolve()
            if not path.is_relative_to(directory.resolve()) or file_hash(path) != entry["sha256"]:
                raise DataUnavailable("SUPPLEMENT_HASH", str(path.name))
            if not entry.get("source") or not entry.get("verified_at"):
                raise DataUnavailable("SUPPLEMENT_PROVENANCE", path.name)
            data = (
                pd.read_parquet(path)
                if path.suffix == ".parquet"
                else pd.read_csv(path, dtype={"fund_code": str, "security_code": str})
            )
            data = dates(
                data, ("date", "ann_date", "report_date", "effective_date", "in_date", "out_date")
            )
            keys = entry["keys"]
            base = bundle.frames.get(name, pd.DataFrame())
            if entry.get("priority", "fill_gaps") == "fill_gaps" and not base.empty:
                value_field = (
                    "return"
                    if name in ("factors", "fixed_income_factors")
                    else ("ann_date" if "ann_date" in base else None)
                )
                valid_base = base.loc[base[value_field].notna()] if value_field else base
                present = pd.MultiIndex.from_frame(valid_base[keys])
                data = data.loc[~pd.MultiIndex.from_frame(data[keys]).isin(present)]
            combined = data.copy() if base.empty else pd.concat([base, data], ignore_index=True)
            bundle.frames[name] = combined.drop_duplicates(keys, keep="last").reset_index(drop=True)
            used.append(
                {key: entry[key] for key in ("table", "file", "sha256", "source", "verified_at")}
            )
        bundle.provenance["supplements"] = used
        return bundle
