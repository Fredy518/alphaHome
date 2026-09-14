from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pandas as pd

from .config import Settings
from .data import AlphaDB, DataBundle
from .storage import atomic_json, file_hash

AUDIT_TABLES = {
    "fund_basic_ext": ("found_date", "ts_code", None),
    "fund_nav": ("nav_date", "ts_code", "ann_date"),
    "fund_stock_holding_detail": ("report_date", "ts_code", "ann_date"),
    "fund_portfolio": ("end_date", "ts_code", "ann_date"),
    "fund_industry_alloc": ("report_date", "ts_code", "ann_date"),
    "fund_asset_alloc": ("report_date", "ts_code", "ann_date"),
    "fund_classification_member": ("in_date", "ts_code", None),
    "index_swdaily": ("trade_date", "ts_code", None),
    "index_swmember": ("in_date", "ts_code", None),
    "stock_industry_versioned": ("trade_date", "ts_code", None),
    "stock_daily": ("trade_date", "ts_code", None),
    "stock_adjfactor": ("trade_date", "ts_code", None),
    "hk_daily": ("trade_date", "ts_code", None),
    "macro_repo_rate": ("date", None, None),
}


def audit_database(settings: Settings, as_of, progress=print) -> dict:
    db = AlphaDB(settings)
    tables = []
    for table, (date_col, code_col, ann_col) in AUDIT_TABLES.items():
        progress(f"audit {table}", flush=True)
        distinct = f"count(DISTINCT {code_col})" if code_col else "NULL::bigint"
        ann = f"count(*) FILTER (WHERE {ann_col} IS NULL)" if ann_col else "NULL::bigint"
        try:
            query = f"SELECT count(*) rows,{distinct} instruments,min({date_col}) min_date,max({date_col}) max_date,{ann} missing_ann_date FROM rawdata.{table}"
            data = db.query(query).iloc[0].to_dict()
            tables.append({"table": f"rawdata.{table}", "status": "queried", **data})
        except Exception as exc:
            tables.append(
                {"table": f"rawdata.{table}", "status": "query_failed", "reason": str(exc)}
            )
    funds, classifications = db.universe()
    by_category = (
        funds.groupby(["category", "share_class"], dropna=False).size().reset_index(name="count")
    )
    result = {
        "audit_as_of": str(pd.Timestamp(as_of).date()),
        "executed_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "tables": tables,
        "metadata_rows": len(funds),
        "classification_rows": len(classifications),
        "metadata_counts": json.loads(by_category.to_json(orient="records", force_ascii=False)),
        "warning": "Full-table inventory is not proof of PIT coverage or model readiness",
    }
    path = settings.path("output_dir") / "audit" / f"database_{pd.Timestamp(as_of):%Y%m%d}.json"
    atomic_json(path, result)
    return result


def audit_bundle(bundle: DataBundle, as_of) -> dict:
    findings = []
    for name, frame in bundle.frames.items():
        for column in ("ann_date", "date", "report_date"):
            if column in frame:
                parsed = pd.to_datetime(frame[column], errors="coerce")
                findings.append(
                    {
                        "table": name,
                        "column": column,
                        "rows": len(frame),
                        "missing": int(parsed.isna().sum()),
                        "min": str(parsed.min().date()) if parsed.notna().any() else None,
                        "max": str(parsed.max().date()) if parsed.notna().any() else None,
                    }
                )
    factors = bundle["factors"]
    summary = (
        factors.groupby("asset")
        .agg(rows=("return", "count"), min_date=("date", "min"), max_date=("date", "max"))
        .reset_index()
        if not factors.empty
        else pd.DataFrame()
    )
    return {
        "audit_as_of": str(pd.Timestamp(as_of).date()),
        "input_hash": bundle.fingerprint,
        "provenance": bundle.provenance,
        "date_coverage": findings,
        "factor_coverage": json.loads(summary.to_json(orient="records", date_format="iso")),
        "holdings_verified_announcements": int(bundle["holdings"].ann_date.notna().sum())
        if not bundle["holdings"].empty
        else 0,
    }


def audit_legacy(settings: Settings, source: Path, rscript: Path) -> dict:
    paths = [
        source / name
        for name in (
            "functools.R",
            "mock_stk_portfolio2.R",
            "mock_indpos_evaluate2.R",
            "【研究】测算方法验证/Script.R",
        )
    ]
    report = {
        "source_directory": str(source),
        "source_hashes": {str(p.relative_to(source)): file_hash(p) for p in paths if p.exists()},
        "executed_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
    }
    command = [str(rscript), "--vanilla", str(settings.root / "scripts" / "legacy_probe.R")]
    process = subprocess.run(
        command,
        cwd=source,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    report["r_returncode"] = process.returncode
    # The probe only evaluates function definitions and emits numeric evidence, never credentials.
    report["numeric_evidence"] = process.stdout.strip().splitlines()
    report["r_warning_present"] = bool(process.stderr.strip())
    atomic_json(settings.path("output_dir") / "audit" / "legacy_reproduction.json", report)
    return report
