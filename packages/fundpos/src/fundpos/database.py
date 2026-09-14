from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg

from .config import Settings
from .constants import ASSETS, FIXED_INCOME_OUTPUTS
from .data import DataBundle
from .errors import DataUnavailable, ProtocolError
from .storage import file_hash, frame_hash


def _json(value):
    return json.dumps(_json_value(value), ensure_ascii=False, allow_nan=False, default=str)


def _missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, (dict, list, tuple, set)):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _json_value(value):
    if _missing(value):
        return None
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_value(item) for item in value]
    if isinstance(value, (pd.Timestamp, Path)):
        return str(value)
    if hasattr(value, "tolist") and callable(value.tolist):
        converted = value.tolist()
        if converted is not value:
            return _json_value(converted)
    if hasattr(value, "item") and callable(value.item):
        return _json_value(value.item())
    return value


def _db_value(value):
    if _missing(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item") and callable(value.item):
        return value.item()
    return value


_OWNER_EXCEPTION_REQUIRED = {
    "authorized",
    "authorization_type",
    "authorized_at",
    "policy_version",
    "policy_sha256",
    "reason",
    "waived_gates",
    "allow_partial_groups",
}


def _owner_exception(payload):
    """Validate an explicit project-owner acceptance exception.

    The exception changes publication governance only. It never rewrites source
    metrics, evidence grades, or the final-holdout flag.
    """
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ProtocolError("acceptance_override must be an object")
    missing = _OWNER_EXCEPTION_REQUIRED - payload.keys()
    if missing:
        raise ProtocolError(
            "Incomplete project-owner acceptance override: "
            + ", ".join(sorted(missing))
        )
    if payload["authorized"] is not True:
        raise ProtocolError("Project-owner acceptance override is not authorized")
    if payload["authorization_type"] != "project_owner_explicit":
        raise ProtocolError("Unsupported acceptance override authority")
    if not isinstance(payload["waived_gates"], list) or not payload["waived_gates"]:
        raise ProtocolError("Acceptance override must list waived gates")
    if not isinstance(payload["allow_partial_groups"], bool):
        raise ProtocolError("allow_partial_groups must be boolean")
    return payload


def _exposure_quality(row, asset: str):
    if asset in {"hk", *ASSETS[3:]}:
        return row.get("stock_quality")
    if asset == "convertible_bond":
        quality = row.get("cbond_quality")
        if row.get("cbond_reliability") not in {
            None,
            "agreement_qualified",
            "disclosure_consistent_proxy_qualified",
        }:
            return "diagnostic"
        return quality
    return row.get(f"{asset}_quality")


def _scenario_exposure_records(
    scenarios: pd.DataFrame, estimates: pd.DataFrame
) -> list[dict]:
    """Normalize successful financing scenarios to a database long table."""

    if scenarios.empty:
        return []
    primary_by_product = {
        str(row["master_code"]): row.get("primary_scenario")
        for row in estimates.to_dict("records")
    }
    scenario_codes = {
        str(product_id): set(group["scenario"].astype(str))
        for product_id, group in scenarios.groupby("master_code")
    }
    invalid_primary = sorted(
        product_id
        for product_id, codes in scenario_codes.items()
        if str(primary_by_product.get(product_id)) not in codes
    )
    if invalid_primary:
        raise DataUnavailable(
            "RUN_SCHEMA",
            "Missing primary financing scenario for: " + ", ".join(invalid_primary[:5]),
        )
    identity_columns = {
        "fund_code",
        "master_code",
        "valuation_date",
        "scenario",
    }
    records = []
    for row in scenarios.to_dict("records"):
        product_id = str(row["master_code"])
        scenario_code = str(row["scenario"])
        for asset_code, value in row.items():
            if asset_code in identity_columns or _missing(value):
                continue
            try:
                exposure = float(value)
            except (TypeError, ValueError) as exc:
                raise DataUnavailable(
                    "RUN_SCHEMA",
                    f"Non-numeric scenario value: {scenario_code}/{asset_code}",
                ) from exc
            records.append(
                {
                    "valuation_date": row["valuation_date"],
                    "product_id": product_id,
                    "scenario_code": scenario_code,
                    "asset_code": str(asset_code),
                    "denominator": "fund_nav",
                    "exposure": exposure,
                    "is_primary": primary_by_product.get(product_id) == scenario_code,
                    "diagnostic_only": True,
                }
            )
    return records


def _fetch_frame(cursor, sql: str, parameters=()) -> pd.DataFrame:
    cursor.execute(sql, parameters)
    columns = [column.name for column in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def _same_value(left, right, *, tolerance=1e-12) -> bool:
    if _missing(left) and _missing(right):
        return True
    if _missing(left) or _missing(right):
        return False
    if isinstance(left, bool) or isinstance(right, bool):
        return bool(left) == bool(right)
    if isinstance(left, (int, float, Decimal)) and isinstance(
        right, (int, float, Decimal)
    ):
        left_number, right_number = float(left), float(right)
        return abs(left_number - right_number) <= tolerance * max(
            1.0, abs(left_number), abs(right_number)
        )
    if isinstance(left, (date, datetime, pd.Timestamp)) or isinstance(
        right, (date, datetime, pd.Timestamp)
    ):
        return str(left)[:19] == str(right)[:19] or str(left)[:10] == str(right)[:10]
    left_text, right_text = str(left), str(right)
    if re.match(r"^\d{4}-\d{2}-\d{2}", left_text) and re.match(
        r"^\d{4}-\d{2}-\d{2}", right_text
    ):
        return left_text[:19] == right_text[:19] or left_text[:10] == right_text[:10]
    return left_text == right_text


def _compare_frames(
    expected: pd.DataFrame,
    actual: pd.DataFrame,
    *,
    keys: list[str],
    columns: list[str],
) -> dict:
    def keyed(frame):
        result = {}
        for row in frame.to_dict("records"):
            key = tuple(str(row[name])[:10] if name.endswith("date") else str(row[name]) for name in keys)
            result[key] = row
        return result

    expected_rows, actual_rows = keyed(expected), keyed(actual)
    missing = sorted(set(expected_rows) - set(actual_rows))
    extra = sorted(set(actual_rows) - set(expected_rows))
    mismatches = []
    for key in sorted(set(expected_rows) & set(actual_rows)):
        for column in columns:
            left, right = expected_rows[key].get(column), actual_rows[key].get(column)
            if not _same_value(left, right):
                mismatches.append(
                    {
                        "key": list(key),
                        "column": column,
                        "local": _json_value(left),
                        "database": _json_value(right),
                    }
                )
    return {
        "expected_rows": len(expected_rows),
        "actual_rows": len(actual_rows),
        "missing_rows": len(missing),
        "extra_rows": len(extra),
        "value_mismatches": len(mismatches),
        "examples": [
            *({"kind": "missing", "key": list(key)} for key in missing[:3]),
            *({"kind": "extra", "key": list(key)} for key in extra[:3]),
            *mismatches[:4],
        ],
    }


class FundposDatabase:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.schema = settings.values.get("database", {}).get("schema", "fundpos")
        if self.schema != "fundpos":
            raise ValueError("Only the reviewed fundpos schema is supported")

    def connect(self, *, read_only=False, migration=False):
        path = Path(self.settings.values["data"]["alphahome_config"]).expanduser()
        config = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else {}
        if migration:
            dsn = os.environ.get("FUNDPOS_MIGRATION_DATABASE_URL") or config.get(
                "fundpos_migration_database", {}
            ).get("url")
        elif read_only:
            dsn = (
                os.environ.get("FUNDPOS_READ_DATABASE_URL")
                or os.environ.get("FUNDPOS_DATABASE_URL")
                or config.get("database", {}).get("url")
            )
        else:
            dsn = os.environ.get("FUNDPOS_WRITE_DATABASE_URL") or config.get(
                "fundpos_database", {}
            ).get("url")
        if not dsn:
            code = (
                "NO_FUNDPOS_MIGRATION_CONFIG"
                if migration
                else ("NO_DATABASE_CONFIG" if read_only else "NO_FUNDPOS_WRITE_CONFIG")
            )
            raise DataUnavailable(
                code,
                "Use a separate fundpos migration/write connection; source access remains read-only",
            )
        timeout = self.settings.values.get("database", {}).get("statement_timeout_seconds", 60)
        return psycopg.connect(dsn, connect_timeout=10,
            options=f"-c default_transaction_read_only={'on' if read_only else 'off'} -c statement_timeout={int(timeout*1000)}")

    @property
    def migration_files(self):
        directory = self.settings.root / self.settings.values.get("database", {}).get("migrations_dir", "migrations")
        return sorted(directory.glob("[0-9][0-9][0-9]_*.sql"))

    def migration_plan(self):
        applied = {}
        with self.connect(read_only=True) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('fundpos.schema_migration')")
            if cursor.fetchone()[0]:
                cursor.execute("SELECT version,sha256 FROM fundpos.schema_migration")
                applied = dict(cursor.fetchall())
        rows = []
        for path in self.migration_files:
            digest = file_hash(path)
            status = "pending" if path.name not in applied else ("applied" if applied[path.name] == digest else "hash_mismatch")
            rows.append({"version": path.name, "sha256": digest, "status": status})
        return rows

    def apply_migrations(self):
        plan = self.migration_plan()
        if any(x["status"] == "hash_mismatch" for x in plan):
            raise ProtocolError("Applied migration hash differs from local file")
        lock = int(self.settings.values.get("database", {}).get("lock_key", 66757110))
        with self.connect(migration=True) as connection:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
                for item in plan:
                    if item["status"] == "pending":
                        path = self.settings.root / self.settings.values.get("database", {}).get("migrations_dir", "migrations") / item["version"]
                        cursor.execute(path.read_text(encoding="utf8"))
                        cursor.execute("INSERT INTO fundpos.schema_migration(version,sha256) VALUES(%s,%s)",
                                       (item["version"], item["sha256"]))
        return self.migration_plan()

    def preflight_run(self, run: Path):
        manifest_path = run / "manifest.json"
        estimates_path = run / "estimates.parquet"
        aggregates_path = run / "aggregates.parquet"
        if not all(p.exists() for p in (manifest_path, estimates_path, aggregates_path)):
            raise DataUnavailable("RUN_INCOMPLETE", "manifest, estimates and aggregates required")
        manifest = json.loads(manifest_path.read_text(encoding="utf8"))
        estimates, aggregates = pd.read_parquet(estimates_path), pd.read_parquet(aggregates_path)
        required = {"fund_code", "master_code", "valuation_date", "status", "category"}
        if not required.issubset(estimates) or estimates.duplicated(["master_code", "valuation_date"]).any():
            raise DataUnavailable("RUN_SCHEMA", "Unique product/date estimates required")
        if not estimates.valuation_date.eq(manifest["valuation_date"]).all():
            raise DataUnavailable("RUN_DATE_MISMATCH", manifest["valuation_date"])
        aggregate_keys = {"valuation_date", "category", "weighting", "status"}
        if not aggregate_keys.issubset(aggregates) or aggregates.duplicated(
            ["valuation_date", "category", "weighting"]
        ).any():
            raise DataUnavailable("RUN_SCHEMA", "Unique date/category/weighting groups required")
        family = manifest.get("model_family", "equity")
        scenario_path = run / "scenarios.parquet"
        fixed_income = family in {"fixed_income_plus", "convertible_dominant"}
        if fixed_income and not scenario_path.exists():
            raise DataUnavailable(
                "RUN_INCOMPLETE", "fixed-income runs require scenarios.parquet"
            )
        scenarios = (
            pd.read_parquet(scenario_path)
            if scenario_path.exists()
            else pd.DataFrame()
        )
        if not scenarios.empty:
            scenario_keys = {
                "fund_code",
                "master_code",
                "valuation_date",
                "scenario",
            }
            if not scenario_keys.issubset(scenarios) or scenarios.duplicated(
                ["master_code", "valuation_date", "scenario"]
            ).any():
                raise DataUnavailable(
                    "RUN_SCHEMA", "Unique product/date/financing scenarios required"
                )
            if not scenarios.valuation_date.eq(manifest["valuation_date"]).all():
                raise DataUnavailable("RUN_DATE_MISMATCH", manifest["valuation_date"])
        assets = (
            FIXED_INCOME_OUTPUTS
            if fixed_income
            else ASSETS
        )
        missing_assets = [a for a in assets if a not in estimates]
        if missing_assets:
            raise DataUnavailable("RUN_SCHEMA", ", ".join(missing_assets))
        identity = "|".join(str(manifest.get(k)) for k in
            ("valuation_date", "information_cutoff", "model_family", "scope_version",
             "config_hash", "input_hash", "code_hash"))
        logical_key = hashlib.sha256(identity.encode()).hexdigest()
        return {
            "manifest": manifest,
            "estimates": estimates,
            "aggregates": aggregates,
            "scenarios": scenarios,
            "assets": assets,
            "logical_run_key": logical_key,
            "manifest_sha256": file_hash(manifest_path),
            "estimate_rows": len(estimates),
            "aggregate_rows": len(aggregates),
            "scenario_rows": len(scenarios),
        }

    def _ingest_snapshot_evidence(
        self, cursor, run_id, manifest, estimates, run_evidence
    ):
        snapshot_value = manifest.get("provenance", {}).get("snapshot_path")
        if not snapshot_value:
            return {"snapshot_tables": 0, "disclosure_reports": 0, "holdings": 0}
        snapshot = Path(snapshot_value)
        if not (snapshot / "manifest.json").exists():
            raise DataUnavailable("SNAPSHOT_MISSING", str(snapshot))
        bundle = DataBundle.load(snapshot)
        if bundle.fingerprint != manifest["input_hash"]:
            raise DataUnavailable("SNAPSHOT_RUN_MISMATCH", bundle.fingerprint)
        snapshot_manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf8"))
        evidence_ids = {}
        for table, digest in snapshot_manifest["files"].items():
            evidence_id = f"input:{table}:{digest}"
            evidence_ids[table] = evidence_id
            cursor.execute(
                """INSERT INTO fundpos.evidence_snapshot
                (evidence_id,evidence_type,source_name,source_uri,content_sha256,
                 first_observed_at,normalization_version,local_path,metadata)
                VALUES(%s,%s,%s,%s,%s,%s,'v3',%s,%s::jsonb)
                ON CONFLICT DO NOTHING""",
                (
                    evidence_id,
                    f"normalized_{table}",
                    manifest.get("provenance", {}).get("provider", "local_snapshot"),
                    manifest.get("provenance", {}).get("provider"),
                    digest,
                    manifest.get("provenance", {}).get("loaded_at"),
                    str((snapshot / f"{table}.parquet").resolve()),
                    _json({"snapshot_fingerprint": bundle.fingerprint}),
                ),
            )
            cursor.execute(
                """INSERT INTO fundpos.run_evidence(run_id,evidence_id,evidence_role)
                VALUES(%s,%s,'normalized_input') ON CONFLICT DO NOTHING""",
                (run_id, evidence_id),
            )

        scope_version = manifest.get("scope_version", "configured")
        for row in estimates.to_dict("records"):
            pools = []
            if not _missing(row.get("contract_pool")) and bool(row.get("contract_pool")):
                pools.append("contract_pool")
            if not _missing(row.get("style_pool")) and bool(row.get("style_pool")):
                pools.append("style_pool")
            if not _missing(row.get("comparison_group")):
                pools.append(str(row["comparison_group"]))
            if not pools:
                pools.append("configured_scope")
            reasons = [value for value in str(row.get("reason") or "").split(";") if value]
            for pool in pools:
                cursor.execute(
                    """INSERT INTO fundpos.product_scope_history
                    (scope_version,product_id,representative_share,category,pool_kind,
                     valid_from,available_at,evidence_id,quality_status,reasons)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT DO NOTHING""",
                    (
                        scope_version,
                        row["master_code"],
                        row["fund_code"],
                        row["category"],
                        pool,
                        row["valuation_date"],
                        manifest["information_cutoff"],
                        run_evidence,
                        row["status"],
                        _json(reasons),
                    ),
                )

        funds = bundle["funds"]
        master = (
            funds.drop_duplicates("fund_code").set_index("fund_code").master_code.to_dict()
            if not funds.empty
            else {}
        )
        scope_version = manifest.get("scope_version", "configured")
        share_history_count = 0
        if not funds.empty:
            evidence_id = evidence_ids["funds"]
            for row in funds.to_dict("records"):
                if _missing(row.get("fund_code")) or _missing(row.get("found_date")):
                    continue
                product_id = master.get(row["fund_code"], row["fund_code"])
                cursor.execute(
                    """INSERT INTO fundpos.product_share_history
                    (scope_version,product_id,share_code,representative,share_class,
                     valid_from,valid_to,available_at,evidence_id,quality_status)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (
                        scope_version,
                        product_id,
                        row["fund_code"],
                        row["fund_code"] == product_id,
                        _db_value(row.get("share_class")),
                        row["found_date"],
                        _db_value(row.get("liquidation_date")),
                        manifest["information_cutoff"],
                        evidence_id,
                        (
                            "family_incomplete"
                            if not _missing(row.get("family_metadata_missing"))
                            and bool(row.get("family_metadata_missing"))
                            else "snapshot_observed"
                        ),
                    ),
                )
                share_history_count += 1
        classification_count = 0
        classification = bundle["classification"]
        if not classification.empty:
            evidence_id = evidence_ids["classification"]
            for row in classification.to_dict("records"):
                if any(
                    _missing(row.get(key))
                    for key in ("fund_code", "category", "in_date")
                ):
                    continue
                cursor.execute(
                    """INSERT INTO fundpos.product_classification_history
                    (scope_version,product_id,share_code,category,valid_from,valid_to,
                     available_at,evidence_id,quality_status)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'snapshot_observed')
                    ON CONFLICT DO NOTHING""",
                    (
                        scope_version,
                        master.get(row["fund_code"], row["fund_code"]),
                        row["fund_code"],
                        row["category"],
                        row["in_date"],
                        _db_value(row.get("out_date")),
                        manifest["information_cutoff"],
                        evidence_id,
                    ),
                )
                classification_count += 1
        asset_reports = bundle["asset_reports"]
        disclosure_count = 0
        if not asset_reports.empty:
            evidence_id = evidence_ids["asset_reports"]
            for row in asset_reports.to_dict("records"):
                if _missing(row.get("fund_code")) or _missing(row.get("report_date")):
                    continue
                report_id = "control:" + hashlib.sha256(
                    f"{evidence_id}|{row['fund_code']}|{row['report_date']}".encode()
                ).hexdigest()
                complete_control = not _missing(row.get("bond_allocation_complete")) and bool(
                    row.get("bond_allocation_complete")
                )
                completeness = "reconciled_asset_control" if complete_control else "partial_or_unverified"
                aum = _db_value(row.get("aum"))
                ordinary_weight = _db_value(row.get("ordinary_bond_weight"))
                cbond_weight = _db_value(row.get("convertible_bond_weight"))
                cursor.execute(
                    """INSERT INTO fundpos.disclosure_report
                    (report_id,product_id,representative_share,report_date,report_type,
                     announcement_date,first_observed_at,evidence_id,stock_value,
                     ordinary_bond_value,convertible_bond_value,net_asset_value,
                     total_asset_value,completeness_status,control_difference,metadata)
                    VALUES(%s,%s,%s,%s,'asset_control',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT DO NOTHING""",
                    (
                        report_id,
                        master.get(row["fund_code"], row["fund_code"]),
                        row["fund_code"],
                        row["report_date"],
                        _db_value(row.get("ann_date")),
                        manifest.get("provenance", {}).get("loaded_at"),
                        evidence_id,
                        _db_value(row.get("stock_market_value")),
                        None if aum is None or ordinary_weight is None else aum * ordinary_weight,
                        None if aum is None or cbond_weight is None else aum * cbond_weight,
                        aum,
                        _db_value(row.get("total_asset_value")),
                        completeness,
                        _db_value(row.get("bond_allocation_gap")),
                        _json(
                            {
                                "stock_weight": row.get("stock_weight"),
                                "bond_weight": row.get("bond_weight"),
                                "ordinary_bond_weight": row.get("ordinary_bond_weight"),
                                "convertible_bond_weight": row.get("convertible_bond_weight"),
                                "announcement_source": row.get("announcement_source"),
                            }
                        ),
                    ),
                )
                disclosure_count += 1

        constraints = bundle["constraints"]
        contract_count = 0
        if not constraints.empty:
            evidence_id = evidence_ids["constraints"]
            fields = {
                "stock": ("stock_lower", "stock_upper"),
                "convertible_bond": ("cbond_lower", "cbond_upper"),
                "financing": ("financing_lower", "financing_upper"),
                "gross_assets": (None, "gross_assets_upper"),
                "hk_share_of_equity": (None, "hk_upper_equity"),
            }
            for row in constraints.to_dict("records"):
                if any(
                    _missing(row.get(key))
                    for key in ("fund_code", "ann_date", "effective_date")
                ):
                    continue
                for name, (lower_field, upper_field) in fields.items():
                    lower = _db_value(row.get(lower_field)) if lower_field else None
                    upper = _db_value(row.get(upper_field)) if upper_field else None
                    if lower is None and upper is None:
                        continue
                    denominator = {
                        "stock": row.get("stock_denominator") or "unconfirmed",
                        "convertible_bond": "fund_nav",
                        "financing": "fund_nav",
                        "gross_assets": "fund_nav",
                        "hk_share_of_equity": "equity_weight",
                    }[name]
                    cursor.execute(
                        """INSERT INTO fundpos.contract_constraint
                        (product_id,constraint_name,effective_date,announcement_date,
                         lower_bound,upper_bound,denominator,evidence_id,verified,original_text)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                        (
                            master.get(row["fund_code"], row["fund_code"]),
                            name,
                            row["effective_date"],
                            row["ann_date"],
                            lower,
                            upper,
                            denominator,
                            evidence_id,
                            bool(row.get("verified", False)),
                            _db_value(row.get("original_text")),
                        ),
                    )
                    contract_count += 1

        canonical = []
        for table, asset_type in (
            ("holdings", "equity"),
            ("bond_holdings", "ordinary_bond"),
            ("cbond_holdings", "convertible_bond"),
        ):
            frame = bundle[table]
            if frame.empty or not {"fund_code", "report_date", "security_code"}.issubset(frame):
                continue
            frame = frame.copy()
            frame["asset_type"] = asset_type
            if table == "bond_holdings":
                convertible = pd.Series(False, index=frame.index)
                for field in ("bond_type", "security_name"):
                    if field in frame:
                        convertible |= frame[field].astype(str).str.contains("可转|可交换", na=False)
                if "is_convertible_period" in frame:
                    convertible |= frame.is_convertible_period.eq(True)
                frame.loc[convertible, "asset_type"] = "convertible_bond"
            frame["source_table"] = table
            canonical.append(frame)
        holding_count = 0
        if canonical:
            holdings = pd.concat(canonical, ignore_index=True, sort=False)
            holdings = holdings.loc[
                holdings.security_code.notna() & holdings.market_value.notna()
            ].copy()
            # Prefer the general bond disclosure when the convertible-only table
            # contains the same security.  This prevents double counting.
            priority = holdings.source_table.map(
                {"holdings": 0, "bond_holdings": 0, "cbond_holdings": 1}
            )
            holdings = (
                holdings.assign(_priority=priority)
                .sort_values("_priority")
                .drop_duplicates(
                    ["fund_code", "report_date", "security_code", "asset_type"],
                    keep="first",
                )
            )
            digest = frame_hash(
                holdings[
                    ["fund_code", "report_date", "security_code", "asset_type", "market_value"]
                ]
            )
            evidence_id = f"normalized_holdings:{digest}"
            cursor.execute(
                """INSERT INTO fundpos.evidence_snapshot
                (evidence_id,evidence_type,source_name,content_sha256,first_observed_at,
                 normalization_version,local_path,metadata)
                VALUES(%s,'standardized_disclosure_holdings','fundpos',%s,%s,'v3',%s,%s::jsonb)
                ON CONFLICT DO NOTHING""",
                (
                    evidence_id,
                    digest,
                    manifest.get("provenance", {}).get("loaded_at"),
                    str(snapshot.resolve()),
                    _json({"dedup_rule": "bond_detail_precedes_cbond_duplicate"}),
                ),
            )
            cursor.execute(
                """INSERT INTO fundpos.run_evidence(run_id,evidence_id,evidence_role)
                VALUES(%s,%s,'standardized_disclosure') ON CONFLICT DO NOTHING""",
                (run_id, evidence_id),
            )
            for (fund_code, report_date), report_rows in holdings.groupby(
                ["fund_code", "report_date"], dropna=False
            ):
                report_id = "holding:" + hashlib.sha256(
                    f"{evidence_id}|{fund_code}|{report_date}".encode()
                ).hexdigest()
                announcements = pd.to_datetime(
                    report_rows.get("ann_date", pd.Series(dtype="datetime64[ns]")),
                    errors="coerce",
                ).dropna().unique()
                announcement = pd.Timestamp(announcements[0]) if len(announcements) == 1 else None
                cursor.execute(
                    """INSERT INTO fundpos.disclosure_report
                    (report_id,product_id,representative_share,report_date,report_type,
                     announcement_date,first_observed_at,evidence_id,completeness_status,metadata)
                    VALUES(%s,%s,%s,%s,'standardized_holding',%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT DO NOTHING""",
                    (
                        report_id,
                        master.get(fund_code, fund_code),
                        fund_code,
                        report_date,
                        announcement,
                        manifest.get("provenance", {}).get("loaded_at"),
                        evidence_id,
                        "announcement_verified" if announcement is not None else "announcement_unverified",
                        _json({"source_tables": sorted(report_rows.source_table.unique())}),
                    ),
                )
                for row in report_rows.to_dict("records"):
                    cursor.execute(
                        """INSERT INTO fundpos.disclosure_holding
                        (report_id,security_code,asset_type,market_value,nav_weight,
                         announcement_date,announcement_source,industry_code,industry_effective_date)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                        (
                            report_id,
                            row["security_code"],
                            row["asset_type"],
                            _db_value(row["market_value"]),
                            _db_value(row.get("weight")),
                            _db_value(row.get("ann_date")),
                            (
                                row.get("announcement_source")
                                if not _missing(row.get("announcement_source"))
                                else (
                                    "source"
                                    if not _missing(row.get("ann_date"))
                                    else "unverified"
                                )
                            ),
                            None,
                            None,
                        ),
                    )
                    holding_count += 1
        return {
            "snapshot_tables": len(evidence_ids),
            "product_shares": share_history_count,
            "classification_periods": classification_count,
            "disclosure_reports": disclosure_count,
            "holdings": holding_count,
            "contracts": contract_count,
        }

    @staticmethod
    def _insert_scenario_exposures(cursor, run_id: str, records: list[dict]) -> None:
        if not records:
            return
        cursor.executemany(
            """INSERT INTO fundpos.fund_scenario_exposure
            (run_id,valuation_date,product_id,scenario_code,asset_code,
             denominator,exposure,is_primary,diagnostic_only)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING""",
            [
                (
                    run_id,
                    row["valuation_date"],
                    row["product_id"],
                    row["scenario_code"],
                    row["asset_code"],
                    row["denominator"],
                    row["exposure"],
                    row["is_primary"],
                    row["diagnostic_only"],
                )
                for row in records
            ],
        )

    def ingest_run(self, run: Path, *, commit=False):
        data = self.preflight_run(run)
        if not commit:
            hidden = {"manifest", "estimates", "aggregates", "scenarios", "assets"}
            return {k: v for k, v in data.items() if k not in hidden} | {
                "status": "dry_run_passed"
            }
        m, estimates, aggregates, scenarios, assets = (
            data["manifest"],
            data["estimates"],
            data["aggregates"],
            data["scenarios"],
            data["assets"],
        )
        run_id = m["run_id"]
        family = m.get("model_family", "equity")
        model_version = f"{family}:{m['configuration']['model']['name']}:{m['code_hash'][:16]}"
        protocol_filename = m.get("protocol_file") or {
            "fixed_income_plus": "v3_validation_protocol.json",
            "convertible_dominant": self.settings.values.get(
                "convertible_dominant", {}
            ).get("protocol_file", "v3_convertible_dominant_protocol.json"),
        }.get(family, "validation_protocol.json")
        protocol_path = self.settings.root / "config" / protocol_filename
        protocol_hash = file_hash(protocol_path)
        owner_exception = _owner_exception(m.get("acceptance_override"))
        formal_publication_eligible = bool(
            m.get("formal_publication", False)
            and m.get("acceptance_id")
            and (m.get("final_holdout_opened", False) or owner_exception is not None)
        )
        exposure_rows = []
        for row in estimates.to_dict("records"):
            for asset in assets:
                value = row.get(asset)
                quality = _exposure_quality(row, asset)
                exposure_rows.append((run_id, row["valuation_date"], row["master_code"], asset, "fund_nav",
                    None if _missing(value) else float(value), "unavailable" if _missing(value) else (quality or row["status"]),
                    bool((quality == "diagnostic") or row["status"] == "degraded")))
        scenario_exposures = _scenario_exposure_records(scenarios, estimates)
        group_assets = [a for a in assets if a in aggregates]
        lock = int(self.settings.values.get("database", {}).get("lock_key", 66757110))
        evidence_stats = {}
        with self.connect() as connection:
            try:
                with connection.transaction(), connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
                    cursor.execute(
                        "SELECT run_id FROM fundpos.estimation_run WHERE logical_run_key=%s",
                        (data["logical_run_key"],),
                    )
                    existing = cursor.fetchone()
                    if existing:
                        self._insert_scenario_exposures(
                            cursor, existing[0], scenario_exposures
                        )
                        cursor.execute(
                            """INSERT INTO fundpos.estimation_attempt
                            (logical_run_key,run_id,status,detail)
                            VALUES(%s,%s,'reused',%s::jsonb)""",
                            (
                                data["logical_run_key"],
                                existing[0],
                                _json({"requested_run_id": run_id}),
                            ),
                        )
                        return {
                            "status": "reused",
                            "run_id": existing[0],
                            "logical_run_key": data["logical_run_key"],
                            "estimate_rows": len(estimates),
                            "scenario_exposure_rows": len(scenario_exposures),
                        }
                    cursor.execute("INSERT INTO fundpos.estimation_attempt(logical_run_key,run_id,status) VALUES(%s,%s,'started')",
                                   (data["logical_run_key"], run_id))
                    cursor.execute("""INSERT INTO fundpos.model_registry(model_version,model_family,code_sha256,protocol_sha256,parameters,status,final_holdout_opened)
                        VALUES(%s,%s,%s,%s,%s::jsonb,'research',false) ON CONFLICT(model_version) DO NOTHING""",
                        (model_version, family, m["code_hash"], protocol_hash, _json(m["configuration"]["model"])))
                    cursor.execute("""INSERT INTO fundpos.estimation_run(run_id,logical_run_key,model_version,model_family,scope_version,
                        valuation_date,information_cutoff,input_sha256,config_sha256,code_sha256,manifest_sha256,source_data_through,status,
                        formal_publication_eligible,final_holdout_opened,manifest,created_at)
                        VALUES(
                            %s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,
                            %s,%s,%s,%s,
                            false,%s::jsonb,%s
                        )""",
                        (run_id, data["logical_run_key"], model_version, family, m.get("scope_version", "configured"),
                         m["valuation_date"], m["information_cutoff"], m["input_hash"], m["config_hash"], m["code_hash"],
                         data["manifest_sha256"], m.get("provenance", {}).get("end"), "ingested",
                         formal_publication_eligible, _json(m), m["created_at"]))
                    cursor.execute("CREATE TEMP TABLE stage_fund_estimate (LIKE fundpos.fund_estimate INCLUDING DEFAULTS) ON COMMIT DROP")
                    fund_rows = []
                    diagnostic_fields = {"fund_code", "master_code", "fund_name", "category", "valuation_date", "status", "reason"}
                    for row in estimates.to_dict("records"):
                        diagnostics = {
                            k: v
                            for k, v in row.items()
                            if k not in diagnostic_fields and k not in assets and not _missing(v)
                        }
                        fund_rows.append((run_id, row["valuation_date"], row["master_code"], row["fund_code"], row["category"],
                            row["status"], _db_value(row.get("reason")), _db_value(row.get("aum")),
                            _db_value(row.get("aum_date")), _db_value(row.get("stock_weight")),
                            _db_value(row.get("convertible_bond")), _db_value(row.get("ordinary_bond")),
                            _db_value(row.get("financing")), _db_value(row.get("stock_quality")),
                            _db_value(row.get("cbond_quality")), _db_value(row.get("ordinary_bond_quality")),
                            _db_value(row.get("proxy_ratio")), _db_value(row.get("holdings_report_date")),
                            _db_value(row.get("constraint_error")), _db_value(row.get("return_mae")),
                            _db_value(row.get("r2")), _db_value(row.get("condition_number")),
                            _json(diagnostics)))
                    cursor.executemany("""INSERT INTO stage_fund_estimate VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)""", fund_rows)
                    cursor.execute("INSERT INTO fundpos.fund_estimate SELECT * FROM stage_fund_estimate ON CONFLICT DO NOTHING")
                    cursor.executemany("""INSERT INTO fundpos.fund_exposure(run_id,valuation_date,product_id,asset_code,denominator,exposure,value_status,diagnostic_only)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", exposure_rows)
                    self._insert_scenario_exposures(
                        cursor, run_id, scenario_exposures
                    )
                    for row in aggregates.to_dict("records"):
                        key = f"{row['valuation_date']}:{row['category']}"
                        cursor.execute("""INSERT INTO fundpos.group_estimate VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING""", (run_id, key, row["category"], row["weighting"], row["status"],
                            row["universe_count"], row["valid_count"], row["count_coverage"],
                            _db_value(row.get("aum_coverage")), row["aum_known_count"],
                            _db_value(row.get("known_aum"))))
                        for asset in group_assets:
                            value = row.get(asset)
                            asset_status = row.get(f"{asset}_status", row["status"])
                            cursor.execute(
                                """INSERT INTO fundpos.group_exposure
                                (run_id,group_key,weighting,asset_code,denominator,
                                 exposure,value_status,universe_count,valid_count,
                                 count_coverage,aum_coverage,diagnostic_only)
                                VALUES(%s,%s,%s,%s,'fund_nav',%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT DO NOTHING""",
                                (
                                    run_id,
                                    key,
                                    row["weighting"],
                                    asset,
                                    None if _missing(value) else float(value),
                                    "unavailable" if _missing(value) else asset_status,
                                    row["universe_count"],
                                    row.get(f"{asset}_valid_count", row["valid_count"]),
                                    row.get(f"{asset}_count_coverage", row["count_coverage"]),
                                    _db_value(row.get(f"{asset}_aum_coverage", row.get("aum_coverage"))),
                                    asset_status != "complete",
                                ),
                            )
                    evidence_id = "run:" + data["manifest_sha256"]
                    cursor.execute("""INSERT INTO fundpos.evidence_snapshot(evidence_id,evidence_type,source_name,source_uri,
                        content_sha256,normalization_version,local_path,metadata) VALUES(%s,'run_manifest','fundpos',%s,%s,'v3',%s,%s::jsonb)
                        ON CONFLICT DO NOTHING""", (evidence_id, str(run), data["manifest_sha256"], str(run.resolve()),
                        _json({"input_hash": m["input_hash"], "run_id": run_id})))
                    cursor.execute(
                        """INSERT INTO fundpos.run_evidence(run_id,evidence_id,evidence_role)
                        VALUES(%s,%s,'run_manifest') ON CONFLICT DO NOTHING""",
                        (run_id, evidence_id),
                    )
                    evidence_stats = self._ingest_snapshot_evidence(
                        cursor, run_id, m, estimates, evidence_id
                    )
                    cursor.execute("UPDATE fundpos.estimation_attempt SET status='committed' WHERE logical_run_key=%s AND run_id=%s AND status='started'",
                                   (data["logical_run_key"], run_id))
            except Exception as exc:
                connection.rollback()
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """INSERT INTO fundpos.estimation_attempt
                            (logical_run_key,run_id,status,detail)
                            VALUES(%s,%s,'failed',%s::jsonb)""",
                            (
                                data["logical_run_key"],
                                run_id,
                                _json({"error_type": type(exc).__name__}),
                            ),
                        )
                    connection.commit()
                except psycopg.Error:
                    connection.rollback()
                raise
        return {"status": "committed", "run_id": run_id, "logical_run_key": data["logical_run_key"],
                "estimate_rows": len(estimates), "exposure_rows": len(exposure_rows),
                "scenario_exposure_rows": len(scenario_exposures),
                **evidence_stats}

    def preflight_validation(self, path: Path):
        content = json.loads(path.read_text(encoding="utf8"))
        accepted = {"complete", "passed", "conditional_complete"}
        if content.get("status") not in accepted:
            raise ProtocolError("Only completed or conditional-complete validation can be registered")
        required = {
            "model_family",
            "model_version",
            "phase",
            "scope_version",
            "truth_version",
            "protocol_hash",
            "metrics",
        }
        if not required.issubset(content):
            raise DataUnavailable("VALIDATION_SCHEMA", ", ".join(sorted(required - content.keys())))
        owner_exception = _owner_exception(content.get("acceptance_override"))
        if owner_exception is not None and not (
            content["status"] == "passed" and content["phase"] == "approval"
        ):
            raise ProtocolError(
                "Project-owner acceptance override requires status=passed and phase=approval"
            )
        validation_id = content.get("validation_id") or file_hash(path)
        evidence_hash = file_hash(path)
        return {
            "content": content,
            "validation_id": validation_id,
            "evidence_sha256": evidence_hash,
            "model_version": content["model_version"],
            "status": content["status"],
            "phase": content["phase"],
            "formal_publication_eligible": bool(
                (
                    content["status"] in {"complete", "passed"}
                    and content["phase"] == "final"
                    and content.get("final_holdout_opened", False)
                    and content.get("eligible_for_formal_promotion", True)
                )
                or owner_exception is not None
            ),
        }

    def ingest_validation(self, path: Path, *, commit=True):
        checked = self.preflight_validation(path)
        content = checked["content"]
        if not commit:
            return {key: value for key, value in checked.items() if key != "content"} | {
                "status": "dry_run"
            }
        code_hash = content.get("recalculation_code_hash") or content.get("code_hash")
        if not code_hash:
            raise DataUnavailable("VALIDATION_SCHEMA", "recalculation_code_hash")
        parameters = {
            "selected_candidate": content.get("selected_candidate"),
            "evidence_scope": content.get("evidence_scope"),
            "smoothing_validation_cadence": content.get("smoothing_validation_cadence"),
        }
        metrics_payload = {
            "models": content["metrics"],
            "individual_coverage_metrics": content.get("individual_coverage_metrics"),
            "diagnostic_group_metrics": content.get("diagnostic_group_metrics"),
            "all_asset_label_count": content.get("all_asset_label_count"),
            "all_industry_label_count": content.get("all_industry_label_count"),
            "common_label_count": content.get("common_label_count"),
            "common_industry_label_count": content.get("common_industry_label_count"),
            "evidence_warnings": content.get("evidence_warnings", []),
            "selection_assessment": content.get("selection_assessment"),
            "input_hash": content.get("input_hash"),
            "source_input_hash": content.get("source_input_hash"),
            "acceptance_override": content.get("acceptance_override"),
            "source_validation": content.get("source_validation"),
        }
        result_status = "committed"
        with self.connect() as connection, connection.transaction(), connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (int(self.settings.values.get("database", {}).get("lock_key", 66757110)) + 1,),
            )
            cursor.execute(
                """INSERT INTO fundpos.model_registry
                (model_version,model_family,code_sha256,protocol_sha256,parameters,status,final_holdout_opened)
                VALUES(%s,%s,%s,%s,%s::jsonb,%s,%s)
                ON CONFLICT(model_version) DO NOTHING""",
                (
                    content["model_version"],
                    content["model_family"],
                    code_hash,
                    content["protocol_hash"],
                    _json(parameters),
                    "conditional_research"
                    if content["status"] == "conditional_complete"
                    else "validated",
                    bool(content.get("final_holdout_opened", False)),
                ),
            )
            cursor.execute(
                """SELECT model_family,code_sha256,protocol_sha256
                FROM fundpos.model_registry WHERE model_version=%s""",
                (content["model_version"],),
            )
            registered = cursor.fetchone()
            if registered != (
                content["model_family"],
                code_hash,
                content["protocol_hash"],
            ):
                raise ProtocolError("Existing model version has different code or protocol")
            if content.get("acceptance_override") is not None:
                cursor.execute(
                    """UPDATE fundpos.model_registry
                    SET status='approved_with_exception'
                    WHERE model_version=%s""",
                    (content["model_version"],),
                )
            cursor.execute(
                "SELECT evidence_sha256 FROM fundpos.validation_result WHERE validation_id=%s",
                (checked["validation_id"],),
            )
            existing = cursor.fetchone()
            if existing:
                if existing[0] != checked["evidence_sha256"]:
                    raise ProtocolError("Existing validation id has different evidence")
                result_status = "reused"
            else:
                cursor.execute(
                    """INSERT INTO fundpos.validation_result
                    VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,now())""",
                    (
                        checked["validation_id"],
                        content["model_version"],
                        content["phase"],
                        content["scope_version"],
                        content["truth_version"],
                        content["status"],
                        _json(metrics_payload),
                        bool(content.get("final_holdout_opened", False)),
                        checked["evidence_sha256"],
                    ),
                )
        return {key: value for key, value in checked.items() if key != "content"} | {
            "status": result_status
        }

    def publish(self, run_id: str, validation_id: str, publication_key: str):
        with self.connect() as connection, connection.transaction(), connection.cursor() as cursor:
            cursor.execute("""SELECT r.formal_publication_eligible,v.status,v.phase,v.final_holdout_opened,
                EXISTS(SELECT 1 FROM fundpos.group_estimate g WHERE g.run_id=r.run_id)
                AND NOT EXISTS(SELECT 1 FROM fundpos.group_estimate g WHERE g.run_id=r.run_id AND g.status<>'complete'),
                v.metrics->'acceptance_override'
                FROM fundpos.estimation_run r JOIN fundpos.validation_result v ON v.model_version=r.model_version
                WHERE r.run_id=%s AND v.validation_id=%s""", (run_id, validation_id))
            row = cursor.fetchone()
            owner_exception = _owner_exception(row[5]) if row and row[5] else None
            standard_approval = bool(
                row
                and row[0]
                and row[1] in {"complete", "passed"}
                and row[2] == "final"
                and row[3]
                and row[4]
            )
            exception_approval = bool(
                row
                and row[0]
                and row[1] == "passed"
                and row[2] == "approval"
                and owner_exception is not None
                and (row[4] or owner_exception["allow_partial_groups"])
            )
            if not (standard_approval or exception_approval):
                raise ProtocolError(
                    "Publication requires final validation and complete count/AUM gates "
                    "or an explicit project-owner exception"
                )
            approval_mode = "project_owner_exception" if exception_approval else "standard"
            cursor.execute(
                """SELECT run_id,validation_id FROM fundpos.publication
                WHERE publication_key=%s AND is_current FOR UPDATE""",
                (publication_key,),
            )
            current = cursor.fetchone()
            if current == (run_id, validation_id):
                return {
                    "status": "reused",
                    "publication_key": publication_key,
                    "run_id": run_id,
                    "approval_mode": approval_mode,
                }
            cursor.execute("UPDATE fundpos.publication SET is_current=false WHERE publication_key=%s AND is_current", (publication_key,))
            cursor.execute(
                """INSERT INTO fundpos.publication
                (publication_key,run_id,validation_id) VALUES(%s,%s,%s)""",
                (publication_key, run_id, validation_id),
            )
            cursor.execute("""INSERT INTO fundpos.publication_event
                (publication_key,run_id,event_type,detail)
                VALUES(%s,%s,'published',%s::jsonb)""",
                (publication_key, run_id, _json({"approval_mode": approval_mode})))
            return {
                "status": "published",
                "publication_key": publication_key,
                "run_id": run_id,
                "approval_mode": approval_mode,
            }

    def revoke(self, publication_key: str, reason: str):
        with self.connect() as connection, connection.transaction(), connection.cursor() as cursor:
            cursor.execute("SELECT run_id FROM fundpos.publication WHERE publication_key=%s AND is_current FOR UPDATE", (publication_key,))
            row = cursor.fetchone()
            if not row:
                raise DataUnavailable("NO_CURRENT_PUBLICATION", publication_key)
            cursor.execute("UPDATE fundpos.publication SET is_current=false,revoked_at=now() WHERE publication_key=%s AND is_current", (publication_key,))
            cursor.execute("INSERT INTO fundpos.publication_event(publication_key,run_id,event_type,detail) VALUES(%s,%s,'revoked',%s::jsonb)",
                           (publication_key, row[0], _json({"reason": reason})))
            cursor.execute("""UPDATE fundpos.publication SET is_current=true WHERE publication_id=(SELECT publication_id
                FROM fundpos.publication WHERE publication_key=%s AND revoked_at IS NULL ORDER BY published_at DESC LIMIT 1)""",
                (publication_key,))

    def reconcile(self, run: Path):
        data = self.preflight_run(run)
        run_id = data["manifest"]["run_id"]
        with self.connect(read_only=True) as connection, connection.cursor() as cursor:
            database_funds = _fetch_frame(
                cursor,
                """SELECT valuation_date,product_id,representative_share,category,status,
                reason,aum,aum_date,stock_weight,convertible_bond_weight,
                ordinary_bond_weight,financing_weight,stock_quality,
                convertible_bond_quality,ordinary_bond_quality,proxy_ratio,
                holdings_report_date,constraint_error,return_mae,r2,condition_number
                FROM fundpos.fund_estimate WHERE run_id=%s""",
                (run_id,),
            )
            database_exposures = _fetch_frame(
                cursor,
                """SELECT valuation_date,product_id,asset_code,denominator,exposure,
                value_status,diagnostic_only FROM fundpos.fund_exposure WHERE run_id=%s""",
                (run_id,),
            )
            database_groups = _fetch_frame(
                cursor,
                """SELECT group_key,category,weighting,status,universe_count,valid_count,
                count_coverage,aum_coverage,aum_known_count,known_aum
                FROM fundpos.group_estimate WHERE run_id=%s""",
                (run_id,),
            )
            database_group_exposures = _fetch_frame(
                cursor,
                """SELECT group_key,weighting,asset_code,denominator,exposure,value_status,
                universe_count,valid_count,count_coverage,aum_coverage,diagnostic_only
                FROM fundpos.group_exposure WHERE run_id=%s""",
                (run_id,),
            )
            database_scenario_exposures = _fetch_frame(
                cursor,
                """SELECT valuation_date,product_id,scenario_code,asset_code,
                denominator,exposure,is_primary,diagnostic_only
                FROM fundpos.fund_scenario_exposure WHERE run_id=%s""",
                (run_id,),
            )

        estimates, aggregates, scenarios, assets = (
            data["estimates"],
            data["aggregates"],
            data["scenarios"],
            data["assets"],
        )
        fund_mapping = {
            "master_code": "product_id",
            "fund_code": "representative_share",
            "convertible_bond": "convertible_bond_weight",
            "ordinary_bond": "ordinary_bond_weight",
            "financing": "financing_weight",
            "cbond_quality": "convertible_bond_quality",
        }
        expected_funds = estimates.rename(columns=fund_mapping).copy()
        fund_columns = [
            "valuation_date",
            "product_id",
            "representative_share",
            "category",
            "status",
            "reason",
            "aum",
            "aum_date",
            "stock_weight",
            "convertible_bond_weight",
            "ordinary_bond_weight",
            "financing_weight",
            "stock_quality",
            "convertible_bond_quality",
            "ordinary_bond_quality",
            "proxy_ratio",
            "holdings_report_date",
            "constraint_error",
            "return_mae",
            "r2",
            "condition_number",
        ]
        for column in fund_columns:
            if column not in expected_funds:
                expected_funds[column] = None
        fund_check = _compare_frames(
            expected_funds,
            database_funds,
            keys=["valuation_date", "product_id"],
            columns=[column for column in fund_columns if column not in {"valuation_date", "product_id"}],
        )

        exposure_rows = []
        for row in estimates.to_dict("records"):
            for asset in assets:
                value = row.get(asset)
                quality = _exposure_quality(row, asset)
                exposure_rows.append(
                    {
                        "valuation_date": row["valuation_date"],
                        "product_id": row["master_code"],
                        "asset_code": asset,
                        "denominator": "fund_nav",
                        "exposure": None if _missing(value) else float(value),
                        "value_status": (
                            "unavailable" if _missing(value) else (quality or row["status"])
                        ),
                        "diagnostic_only": bool(
                            quality == "diagnostic" or row["status"] == "degraded"
                        ),
                    }
                )
        exposure_check = _compare_frames(
            pd.DataFrame(exposure_rows),
            database_exposures,
            keys=["valuation_date", "product_id", "asset_code", "denominator"],
            columns=["exposure", "value_status", "diagnostic_only"],
        )

        expected_groups = aggregates.copy()
        expected_groups["group_key"] = (
            expected_groups.valuation_date.astype(str).str[:10]
            + ":"
            + expected_groups.category.astype(str)
        )
        group_columns = [
            "category",
            "status",
            "universe_count",
            "valid_count",
            "count_coverage",
            "aum_coverage",
            "aum_known_count",
            "known_aum",
        ]
        group_check = _compare_frames(
            expected_groups,
            database_groups,
            keys=["group_key", "weighting"],
            columns=group_columns,
        )
        group_exposure_rows = []
        for row in aggregates.to_dict("records"):
            group_key = f"{str(row['valuation_date'])[:10]}:{row['category']}"
            for asset in [column for column in assets if column in aggregates]:
                value = row.get(asset)
                group_exposure_rows.append(
                    {
                        "group_key": group_key,
                        "weighting": row["weighting"],
                        "asset_code": asset,
                        "denominator": "fund_nav",
                        "exposure": None if _missing(value) else float(value),
                        "value_status": (
                            "unavailable"
                            if _missing(value)
                            else row.get(f"{asset}_status", row["status"])
                        ),
                        "universe_count": row["universe_count"],
                        "valid_count": row.get(f"{asset}_valid_count", row["valid_count"]),
                        "count_coverage": row.get(
                            f"{asset}_count_coverage", row["count_coverage"]
                        ),
                        "aum_coverage": row.get(
                            f"{asset}_aum_coverage", row.get("aum_coverage")
                        ),
                        "diagnostic_only": row.get(f"{asset}_status", row["status"])
                        != "complete",
                    }
                )
        group_exposure_check = _compare_frames(
            pd.DataFrame(group_exposure_rows),
            database_group_exposures,
            keys=["group_key", "weighting", "asset_code", "denominator"],
            columns=[
                "exposure",
                "value_status",
                "universe_count",
                "valid_count",
                "count_coverage",
                "aum_coverage",
                "diagnostic_only",
            ],
        )
        scenario_exposure_check = _compare_frames(
            pd.DataFrame(
                _scenario_exposure_records(scenarios, estimates),
                columns=[
                    "valuation_date",
                    "product_id",
                    "scenario_code",
                    "asset_code",
                    "denominator",
                    "exposure",
                    "is_primary",
                    "diagnostic_only",
                ],
            ),
            database_scenario_exposures,
            keys=[
                "valuation_date",
                "product_id",
                "scenario_code",
                "asset_code",
                "denominator",
            ],
            columns=["exposure", "is_primary", "diagnostic_only"],
        )

        checks = {
            "fund_estimate": fund_check,
            "fund_exposure": exposure_check,
            "group_estimate": group_check,
            "group_exposure": group_exposure_check,
            "fund_scenario_exposure": scenario_exposure_check,
        }
        database_passed = all(
            check["missing_rows"] == 0
            and check["extra_rows"] == 0
            and check["value_mismatches"] == 0
            for check in checks.values()
        )
        result = {
            "status": "passed" if database_passed else "failed",
            "run_id": run_id,
            "canonical_store": "alphadb.fundpos",
            "comparison_source": "immutable_run_parquet",
            "database_checks": checks,
        }
        return result

    def query_estimates(
        self,
        *,
        product_id: str | None = None,
        run_id: str | None = None,
        observed_at: str | pd.Timestamp | None = None,
        published: bool = False,
    ) -> pd.DataFrame:
        """Read long-form estimates by exact version, publication, or observed time."""
        if sum(value is not None for value in (run_id, observed_at)) + int(published) > 1:
            raise ValueError("Choose only one of run_id, observed_at, or published")
        parameters = []
        filters = []
        if product_id:
            filters.append("product_id=%s")
            parameters.append(product_id)
        if published:
            sql = "SELECT * FROM fundpos.published_current"
        elif run_id:
            sql = """SELECT r.model_family,r.model_version,r.valuation_date,
                r.information_cutoff,r.source_data_through,e.product_id,e.status,e.reason,
                x.asset_code,x.denominator,x.exposure,x.value_status,x.diagnostic_only,r.run_id
                FROM fundpos.estimation_run r
                JOIN fundpos.fund_estimate e ON e.run_id=r.run_id
                JOIN fundpos.fund_exposure x ON x.run_id=e.run_id
                 AND x.valuation_date=e.valuation_date AND x.product_id=e.product_id"""
            filters.append("r.run_id=%s")
            parameters.append(run_id)
            if product_id:
                filters[-2] = "e.product_id=%s"
        elif observed_at is not None:
            sql = """SELECT DISTINCT ON (e.product_id,r.model_family,x.asset_code,x.denominator)
                e.product_id,r.model_family,r.model_version,r.valuation_date,
                r.information_cutoff,r.source_data_through,e.status,e.reason,x.asset_code,
                x.denominator,x.exposure,x.value_status,x.diagnostic_only,r.run_id
                FROM fundpos.estimation_run r
                JOIN fundpos.fund_estimate e ON e.run_id=r.run_id
                JOIN fundpos.fund_exposure x ON x.run_id=e.run_id
                 AND x.valuation_date=e.valuation_date AND x.product_id=e.product_id"""
            filters.append("r.ingested_at<=%s")
            parameters.append(pd.Timestamp(observed_at).to_pydatetime())
            if product_id:
                filters[-2] = "e.product_id=%s"
            order = " ORDER BY e.product_id,r.model_family,x.asset_code,x.denominator,r.ingested_at DESC"
        else:
            sql = "SELECT * FROM fundpos.latest_available"
        if filters:
            sql += " WHERE " + " AND ".join(filters)
        if observed_at is not None and not published and run_id is None:
            sql += order
        with self.connect(read_only=True) as connection, connection.cursor() as cursor:
            return _fetch_frame(cursor, sql, tuple(parameters))
