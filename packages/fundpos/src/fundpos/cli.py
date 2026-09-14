from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

import pandas as pd

from .aggregation import custom_portfolio
from .audit import audit_bundle, audit_database, audit_legacy
from .config import Settings
from .constants import ASSETS, CATEGORIES, FIXED_INCOME_OUTPUTS
from .data import AlphaDB, DataBundle
from .database import FundposDatabase
from .demo import synthetic_bundle
from .errors import DataUnavailable, ProtocolError
from .operations import completed_valuation_date, record_observation, run_backfill
from .pipeline import run_estimate
from .reporting import export_report
from .storage import atomic_json, atomic_parquet
from .universe import fixed_income_plus_universe, load_frozen_pilot, select_fixed_count
from .validation import run_validation


def parser():
    root = argparse.ArgumentParser(prog="fundpos", description="公募基金申万一级行业仓位估算")
    sub = root.add_subparsers(dest="command", required=True)
    for name in (
        "audit-data",
        "estimate",
        "backfill",
        "validate",
        "report",
        "demo",
        "audit-legacy",
        "audit-universe",
        "db-migrate",
        "ingest",
        "ingest-validation",
        "publish",
        "reconcile",
        "revoke-publication",
    ):
        p = sub.add_parser(name)
        p.add_argument("--config", default="config/default.toml")
        if name in ("audit-data", "estimate", "backfill", "validate", "audit-universe"):
            p.add_argument("--input", type=Path, help="经指纹核验的本地数据快照")
            p.add_argument("--funds", nargs="+", help="限定产品/份额代码；报告会标明样本范围")
            p.add_argument("--no-prices", action="store_true", help="审计或指数模型可跳过个股行情")
        if name == "audit-data":
            p.add_argument("--database-inventory", action="store_true")
            p.add_argument("--date", required=True)
            p.add_argument("--start", default="2022-01-01")
        if name == "estimate":
            p.add_argument("--date", required=True, help="YYYY-MM-DD 或 latest（最近已完成交易日）")
            p.add_argument("--cutoff", help="信息截止日期，默认估值日次日；latest 默认运行日")
            p.add_argument("--model", choices=("index", "personalized"))
            p.add_argument("--observe", action="store_true", help="将真实全量当日运行计入观察账本")
            p.add_argument(
                "--family",
                choices=("equity", "fixed_income_plus", "convertible_dominant"),
                default="equity",
            )
            p.add_argument("--scope", choices=("configured", "v3-pilot"), default="configured")
        if name == "backfill":
            p.add_argument("--start", required=True)
            p.add_argument("--end", required=True)
            p.add_argument("--frequency", choices=("daily", "weekly", "quarterly"), default="daily")
            p.add_argument("--model", choices=("index", "personalized"))
            p.add_argument(
                "--family",
                choices=("equity", "fixed_income_plus", "convertible_dominant"),
                default="equity",
            )
            p.add_argument("--scope", choices=("configured", "v3-pilot"), default="configured")
        if name == "validate":
            p.add_argument("--phase", choices=("development", "selection", "final"), required=True)
            p.add_argument(
                "--family",
                choices=("equity", "fixed_income_plus", "convertible_dominant"),
                default="equity",
            )
            p.add_argument("--scope", choices=("configured", "v3-pilot"), default="configured")
            p.add_argument(
                "--conditional-evidence",
                action="store_true",
                help="Run research-only development/selection with explicitly labelled conditional contract or tracking evidence",
            )
        if name == "report":
            p.add_argument("--run", type=Path, required=True)
            p.add_argument("--history", type=Path)
            p.add_argument("--fund", help="仅展示指定基金的历史序列")
            p.add_argument("--portfolio", type=Path, help="JSON数组：master_code、weight")
        if name == "audit-legacy":
            p.add_argument(
                "--source",
                type=Path,
                default=Path(
                    r"E:\GL工作文件\体系_投研框架\程序_研究项目\20220504_R基金行业仓位测算"
                ),
            )
            p.add_argument(
                "--rscript",
                type=Path,
                default=Path(r"C:\Program Files\R\R-4.4.2\bin\Rscript.exe"),
            )
        if name == "audit-universe":
            p.add_argument("--date", required=True)
            p.add_argument("--cutoff")
            p.add_argument("--select-pilot", action="store_true")
        if name == "db-migrate":
            p.add_argument("--apply", action="store_true")
        if name in ("ingest", "reconcile"):
            p.add_argument("--run", type=Path, required=True)
            if name == "ingest":
                p.add_argument("--commit", action="store_true")
        if name == "ingest-validation":
            p.add_argument("--validation", type=Path, required=True)
            p.add_argument("--commit", action="store_true")
        if name == "publish":
            p.add_argument("--run", type=Path, required=True)
            p.add_argument("--validation", type=Path, required=True)
            p.add_argument("--key", required=True)
        if name == "revoke-publication":
            p.add_argument("--key", required=True)
            p.add_argument("--reason", required=True)
    return root


def load_inputs(args, settings, start, end):
    if args.input:
        bundle = DataBundle.load(args.input.resolve())
        if args.funds:
            raise DataUnavailable(
                "INPUT_SCOPE_FIXED",
                "Snapshot scope is immutable; create another snapshot for --funds",
            )
        return bundle
    requested_funds = args.funds
    if not requested_funds and getattr(args, "scope", None) == "v3-pilot":
        pilot = load_frozen_pilot(settings.root, settings.values["universe"])
        if getattr(args, "family", None) == "equity":
            pilot = pilot.loc[pilot.category.eq("增强指数型")]
        elif getattr(args, "family", None) == "convertible_dominant":
            pilot = pilot.loc[pilot.selection_group.eq("convertible_dominant")]
        requested_funds = sorted(
            {
                str(code)
                for values in pilot.share_codes
                for code in values
            }
        )
    print("读取 AlphaDB（只读）；保存独立数据快照。", flush=True)
    bundle = AlphaDB(settings).load(
        start,
        end,
        requested_funds,
        fetch_prices=not args.no_prices and settings.values["data"]["fetch_stock_prices"],
    )
    directory = settings.path("data_dir") / "snapshots" / bundle.fingerprint[:24]
    bundle.provenance["snapshot_path"] = str(directory.resolve())
    try:
        stored = DataBundle.load(directory)
    except (FileNotFoundError, DataUnavailable):
        bundle.save(directory)
        stored = DataBundle.load(directory)
    print(f"snapshot: {directory}", flush=True)
    return stored


def execute(args):
    settings = Settings.load(args.config)
    selected_products = None
    if getattr(args, "scope", None) == "v3-pilot":
        pilot = load_frozen_pilot(settings.root, settings.values["universe"])
        if getattr(args, "family", None) == "equity":
            pilot = pilot.loc[pilot.category.eq("增强指数型")]
        elif getattr(args, "family", None) == "convertible_dominant":
            pilot = pilot.loc[pilot.selection_group.eq("convertible_dominant")]
        selected_products = set(pilot.master_code)
    if getattr(args, "model", None):
        settings = settings.with_model(name=args.model, prior_penalty=0.0, smooth_penalty=0.0)
    elif (
        getattr(args, "family", None) == "convertible_dominant"
        and args.command in {"estimate", "backfill"}
    ):
        dedicated = settings.values["convertible_dominant"]
        settings = settings.with_model(
            name=dedicated["default_model"],
            prior_penalty=1.0,
            smooth_penalty=0.1,
            cbond_exposure_mode="bounded_state_space_endpoint",
            cbond_state_penalty=dedicated["state_penalty"],
        )
    if args.command == "audit-legacy":
        return audit_legacy(settings, args.source, args.rscript)
    if args.command == "db-migrate":
        database = FundposDatabase(settings)
        return {"migrations": database.apply_migrations() if args.apply else database.migration_plan(),
                "mode": "apply" if args.apply else "plan"}
    if args.command == "ingest":
        return FundposDatabase(settings).ingest_run(args.run.resolve(), commit=args.commit)
    if args.command == "ingest-validation":
        return FundposDatabase(settings).ingest_validation(
            args.validation.resolve(), commit=args.commit
        )
    if args.command == "reconcile":
        return FundposDatabase(settings).reconcile(args.run.resolve())
    if args.command == "publish":
        database = FundposDatabase(settings)
        ingested = database.ingest_run(args.run.resolve(), commit=True)
        validation = database.ingest_validation(args.validation.resolve(), commit=True)
        database.publish(ingested["run_id"], validation["validation_id"], args.key)
        return {"status": "published", "publication_key": args.key, "run_id": ingested["run_id"]}
    if args.command == "revoke-publication":
        FundposDatabase(settings).revoke(args.key, args.reason)
        return {"status": "revoked", "publication_key": args.key}
    if args.command == "demo":
        values = copy.deepcopy(settings.values)
        values["project"].update(output_dir="outputs/demo", data_dir="data/demo")
        settings = Settings(settings.root, values)
        bundle, truth = synthetic_bundle()
        bundle.save(settings.path("data_dir") / "snapshot")
        atomic_parquet(settings.path("output_dir") / "known_weights.parquet", truth)
        history_dir = run_backfill(settings, bundle, "2023-12-18", "2023-12-29")
        history = pd.read_parquet(history_dir / "estimates.parquet")
        run = run_estimate(settings, bundle, "2023-12-29", "2023-12-30")
        return {
            "scope": "SYNTHETIC_DEMO_ONLY",
            "run": str(run),
            "history": str(history_dir),
            **export_report(settings.root, run, history=history),
        }
    if args.command == "report":
        run = args.run.resolve()
        history = pd.read_parquet(args.history) if args.history else None
        if args.fund and history is not None:
            history = history.loc[history.fund_code == args.fund]
        if args.portfolio:
            allocation = pd.DataFrame(json.loads(args.portfolio.read_text(encoding="utf-8")))
            estimates = pd.read_parquet(run / "estimates.parquet")
            manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
            assets = (
                FIXED_INCOME_OUTPUTS
                if manifest.get("model_family")
                in {"fixed_income_plus", "convertible_dominant"}
                else ASSETS
            )
            result = custom_portfolio(
                estimates, allocation, estimates.valuation_date.iloc[0], assets=assets
            )
            atomic_parquet(run / "custom_portfolio.parquet", result)
        return export_report(settings.root, run, history=history)
    if args.command == "audit-universe":
        day = pd.Timestamp(args.date)
        cutoff = pd.Timestamp(args.cutoff) if args.cutoff else day + pd.Timedelta(days=1)
        if args.input:
            bundle = DataBundle.load(args.input.resolve())
            if args.funds:
                raise DataUnavailable(
                    "INPUT_SCOPE_FIXED",
                    "Snapshot scope is immutable; create another snapshot for --funds",
                )
        else:
            print("读取 AlphaDB 范围与披露控制（只读轻量审计）。", flush=True)
            bundle = AlphaDB(settings).load_universe_audit(
                day - pd.Timedelta(days=700), day, args.funds
            )
            snapshot = (
                settings.path("data_dir")
                / "universe_snapshots"
                / bundle.fingerprint[:24]
            )
            bundle.provenance["snapshot_path"] = str(snapshot.resolve())
            try:
                stored = DataBundle.load(snapshot)
            except (FileNotFoundError, DataUnavailable):
                bundle.save(snapshot)
                stored = DataBundle.load(snapshot)
            bundle = stored
        scope = fixed_income_plus_universe(bundle["funds"], bundle["classification"],
            bundle["asset_reports"], bundle["constraints"], day, cutoff,
            min_age=settings.values["universe"]["min_age_days"],
            max_report_age=settings.values["fixed_income"]["max_report_age_days"],
            max_stock_each=settings.values["fixed_income"]["stock_upper"],
            max_stock_median=settings.values["fixed_income"]["style_stock_median"],
            min_bond_median=settings.values["fixed_income"]["style_bond_median"])
        directory = settings.path("output_dir") / "universe" / f"{day:%Y%m%d}_{bundle.fingerprint[:16]}"
        directory.mkdir(parents=True, exist_ok=True)
        atomic_parquet(directory / "scope.parquet", scope)
        frozen_pilot = load_frozen_pilot(settings.root, settings.values["universe"])
        scope_status = scope.drop_duplicates("master_code")
        current_columns = [
            "contract_pool",
            "style_pool",
            "in_fixed_income_plus",
            "comparison_group",
            "style_report_count",
            "observed_report_count",
            "observed_information_status",
            "eligibility_reasons",
        ]
        scope_status = scope_status[["master_code", *current_columns]].rename(
            columns={column: f"{column}_current" for column in current_columns}
        )
        frozen_status = frozen_pilot.merge(
            scope_status,
            on="master_code",
            how="left",
            validate="one_to_one",
        )
        frozen_status["model_family"] = frozen_status.category.map(
            lambda value: "equity" if value == "增强指数型" else "fixed_income_plus"
        )
        atomic_parquet(directory / "frozen_pilot_status.parquet", frozen_status)
        asset_reports = bundle["asset_reports"]
        result = {"status": "audited", "scope_rows": len(scope), "eligible": int(scope.in_fixed_income_plus.sum()),
                  "comparison": int(scope.comparison_group.notna().sum()), "input_hash": bundle.fingerprint,
                  "frozen_pilot_rows": len(frozen_status),
                  "frozen_fixed_income_rows": int(frozen_status.model_family.eq("fixed_income_plus").sum()),
                  "frozen_equity_rows": int(frozen_status.model_family.eq("equity").sum()),
                  "frozen_strict_fixed_income_rows": int(
                      frozen_status.in_fixed_income_plus_current.fillna(False).sum()
                  ),
                  "asset_report_rows": len(asset_reports),
                  "asset_report_announcement_coverage": (
                      float(asset_reports.ann_date.notna().mean()) if len(asset_reports) else 0.0
                  ),
                  "bond_control_reconciled": (
                      int(asset_reports.bond_allocation_complete.fillna(False).sum())
                      if "bond_allocation_complete" in asset_reports else 0
                  )}
        if args.select_pilot:
            protocol = json.loads((settings.root / "config/v3_validation_protocol.json").read_text(encoding="utf8"))
            pilot, gaps = select_fixed_count(scope, protocol["pilot_quotas"])
            atomic_parquet(directory / "candidate_pilot.parquet", pilot)
            atomic_parquet(directory / "candidate_pilot_gaps.parquet", gaps)
            result.update(candidate_pilot_rows=len(pilot), candidate_shortfall=int(gaps.shortfall.sum()))
        atomic_json(directory / "summary.json", result)
        return {"directory": str(directory), **result}
    if args.command == "audit-data":
        if args.database_inventory:
            return audit_database(settings, args.date)
        bundle = load_inputs(args, settings, args.start, args.date)
        result = audit_bundle(bundle, args.date)
        path = settings.path("output_dir") / "audit" / f"snapshot_{bundle.fingerprint[:24]}.json"
        atomic_json(path, result)
        return {"audit": str(path), **result}
    if args.command == "validate":
        if args.conditional_evidence and args.phase == "final":
            raise ProtocolError("Conditional evidence can never open the final holdout")
        if args.family == "convertible_dominant":
            from .convertible_dominant_validation import (
                ConvertibleDominantValidationState,
                run_convertible_dominant_validation,
            )

            state = ConvertibleDominantValidationState(
                settings, conditional=args.conditional_evidence
            )
        elif args.family == "fixed_income_plus":
            from .fixed_income_validation import (
                FixedIncomeValidationState,
                run_fixed_income_validation,
            )

            state = FixedIncomeValidationState(
                settings, conditional=args.conditional_evidence
            )
        else:
            from .validation import ValidationState

            state = (
                ValidationState(
                    settings,
                    protocol_filename="v3_enhanced_index_validation_protocol.json",
                    state_subdir=(
                        "validation_enhanced_index_conditional"
                        if args.conditional_evidence
                        else "validation_enhanced_index"
                    ),
                )
                if args.scope == "v3-pilot"
                else ValidationState(settings)
            )
        state.assert_open_allowed(args.phase)
        start, end = state.protocol[args.phase]
        # Complete holdings labels may arrive after the last report date.
        bundle = load_inputs(args, settings, start, pd.Timestamp(end) + pd.Timedelta(days=100))
        if args.family == "convertible_dominant":
            return {
                "validation": str(
                    run_convertible_dominant_validation(
                        settings,
                        bundle,
                        args.phase,
                        allow_conditional_evidence=args.conditional_evidence,
                    )
                )
            }
        if args.family == "fixed_income_plus":
            return {
                "validation": str(
                    run_fixed_income_validation(
                        settings,
                        bundle,
                        args.phase,
                        allow_conditional_evidence=args.conditional_evidence,
                    )
                )
            }
        if args.scope == "v3-pilot":
            from .enhanced_index import enhanced_index_prerequisites

            fund_codes = set(
                load_frozen_pilot(settings.root, settings.values["universe"])
                .loc[lambda frame: frame.category.eq("增强指数型"), "fund_code"]
            )
            prerequisites = enhanced_index_prerequisites(
                bundle,
                fund_codes,
                start,
                end,
                allow_conditional=args.conditional_evidence,
            )
            return {
                "validation": str(
                    run_validation(
                        settings,
                        bundle,
                        args.phase,
                        state=state,
                        categories=("增强指数型",),
                        selected_products=selected_products,
                        output_subdir=(
                            "validation_enhanced_index_conditional"
                            if args.conditional_evidence
                            else "validation_enhanced_index"
                        ),
                        require_full_universe=False,
                        extra_prerequisites=prerequisites,
                        allow_conditional_evidence=args.conditional_evidence,
                    )
                )
            }
        return {"validation": str(run_validation(settings, bundle, args.phase, state=state))}
    if args.command == "backfill":
        bundle = load_inputs(args, settings, args.start, args.end)
        if args.family in {"fixed_income_plus", "convertible_dominant"}:
            from .fixed_income_operations import run_fixed_income_backfill

            return {"history": str(run_fixed_income_backfill(
                settings, bundle, args.start, args.end, args.frequency,
                selected_products=selected_products, model_family=args.family))}
        return {
            "history": str(
                run_backfill(
                    settings,
                    bundle,
                    args.start,
                    args.end,
                    args.frequency,
                    categories=("增强指数型",) if args.scope == "v3-pilot" else CATEGORIES,
                    selected_products=selected_products,
                    scope_version=(
                        "fundpos_v3_enhanced_index_20260912"
                        if args.scope == "v3-pilot"
                        else "configured"
                    ),
                )
            )
        }
    today = pd.Timestamp.now(tz="Asia/Shanghai").tz_localize(None).normalize()
    end = today if args.date == "latest" else pd.Timestamp(args.date)
    bundle = load_inputs(args, settings, end - pd.Timedelta(days=180), end)
    day = completed_valuation_date(bundle["calendar"].date) if args.date == "latest" else end
    cutoff = (
        pd.Timestamp(args.cutoff)
        if args.cutoff
        else (today if args.date == "latest" else day + pd.Timedelta(days=1))
    )
    if day >= today and args.date == "latest":
        raise DataUnavailable("UNCOMPLETED_SESSION", str(day.date()))
    if args.family in {"fixed_income_plus", "convertible_dominant"}:
        from .fixed_income_pipeline import run_fixed_income_estimate

        run = run_fixed_income_estimate(
            settings,
            bundle,
            day,
            cutoff,
            selected_products=selected_products,
            model_family=args.family,
        )
    else:
        run = run_estimate(
            settings,
            bundle,
            day,
            cutoff,
            categories=("增强指数型",) if args.scope == "v3-pilot" else CATEGORIES,
            selected_products=selected_products,
            scope_version=(
                "fundpos_v3_enhanced_index_20260912"
                if args.scope == "v3-pilot"
                else "configured"
            ),
        )
    result = {
        "run": str(run),
        "status": json.loads((run / "manifest.json").read_text(encoding="utf-8"))["status_counts"],
    }
    if args.observe:
        result["observation"] = record_observation(settings, run)
    return result


def main():
    import sys

    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    args = parser().parse_args()
    try:
        result = execute(args)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    except (DataUnavailable, ProtocolError, ValueError, FileNotFoundError) as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, ensure_ascii=False))
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
