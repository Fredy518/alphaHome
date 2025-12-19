#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import sys
from typing import Iterable, List, Optional

from alphahome.common.config_manager import ConfigManager
from alphahome.common.logging_utils import setup_logging

from .manager import DolphinDBManager
from .schema import build_drop_database_script, build_kline_5min_init_script
from .hikyuu_5min_importer import (
    Hikyuu5MinImporterConfig,
    HikyuuKline5MinImporter,
)


def _load_codes_from_file(path: str) -> List[str]:
    codes: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            codes.append(line)
    return codes


def _iter_codes(args) -> Iterable[str]:
    if args.codes_file:
        return _load_codes_from_file(args.codes_file)
    if args.codes:
        return [c.strip() for c in args.codes.split(",") if c.strip()]
    raise SystemExit("Missing --codes or --codes-file")


def _build_ddb_manager(args) -> DolphinDBManager:
    return DolphinDBManager(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
    )


def _print_result(obj, fmt: str) -> None:
    if fmt == "json":
        print(json.dumps(obj, ensure_ascii=False, indent=2))
        return
    print(obj)


def cmd_init_kline5m(args) -> int:
    ddb = _build_ddb_manager(args)
    ddb.connect()
    try:
        script = build_kline_5min_init_script(
            db_path=args.db_path,
            table_name=args.table,
            start_month=args.start_month,
            end_month=args.end_month,
            hash_buckets=args.hash_buckets,
        )
        result = ddb.run_script(script)
        _print_result({"status": "ok", "result": result}, args.format)
        return 0
    finally:
        ddb.close()


def cmd_drop_db(args) -> int:
    if not args.yes:
        print(
            "Refusing to drop database without --yes (destructive).",
            file=sys.stderr,
        )
        return 2

    ddb = _build_ddb_manager(args)
    ddb.connect()
    try:
        script = build_drop_database_script(db_path=args.db_path)
        result = ddb.run_script(script)
        _print_result({"status": "ok", "result": result, "db_path": args.db_path}, args.format)
        return 0
    finally:
        ddb.close()

def cmd_import_tdx_5min(args) -> int:
    print(
        "Error: `import-tdx-5min` is deprecated. "
        "Use `import-hikyuu-5min` with HIKYUU_DATA_DIR (or --hikyuu-data-dir).",
        file=sys.stderr,
    )
    return 1


def _resolve_hikyuu_data_dir(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    cfg = ConfigManager()
    data_dir = cfg.get_hikyuu_data_dir()
    if data_dir:
        return data_dir
    raise RuntimeError(
        "Missing Hikyuu data dir. Provide --hikyuu-data-dir or set HIKYUU_DATA_DIR "
        "or configure ~/.alphahome/config.json (backtesting.hikyuu_data_dir)."
    )


def cmd_import_hikyuu_5min(args) -> int:
    ddb = _build_ddb_manager(args)
    ddb.connect()
    try:
        if args.init:
            script = build_kline_5min_init_script(
                db_path=args.db_path,
                table_name=args.table,
                start_month=args.start_month,
                end_month=args.end_month,
                hash_buckets=args.hash_buckets,
            )
            ddb.run_script(script)

        importer = HikyuuKline5MinImporter(
            ddb,
            Hikyuu5MinImporterConfig(
                hikyuu_data_dir=_resolve_hikyuu_data_dir(args.hikyuu_data_dir),
                db_path=args.db_path,
                table_name=args.table,
                chunk_rows=args.chunk_rows,
                price_scale=args.price_scale,
                amount_scale=args.amount_scale,
            ),
        )
        result = importer.import_many(
            _iter_codes(args),
            start=args.start,
            end=args.end,
            incremental=args.incremental,
            dry_run=args.dry_run,
        )
        _print_result(result, args.format)
        return 0
    finally:
        ddb.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="alphahome-ddb", description="AlphaHome DolphinDB tools")
    parser.add_argument("--log-level", default="INFO", help="Log level (default: INFO)")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")

    # DolphinDB connection overrides (config.json is used if omitted)
    parser.add_argument("--host", default=None, help="DolphinDB host override")
    parser.add_argument("--port", type=int, default=None, help="DolphinDB port override")
    parser.add_argument("--username", default=None, help="DolphinDB username override")
    parser.add_argument("--password", default=None, help="DolphinDB password override")

    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser("init-kline5m", help="Create DFS db/table for 5-min K-line")
    p_init.add_argument("--db-path", default="dfs://kline_5min", help='DFS path (default: dfs://kline_5min)')
    p_init.add_argument("--table", default="kline_5min", help="Table name (default: kline_5min)")
    p_init.add_argument("--start-month", type=int, default=200501, help="Partition start month yyyymm")
    p_init.add_argument("--end-month", type=int, default=203012, help="Partition end month yyyymm")
    p_init.add_argument("--hash-buckets", type=int, default=10, help="HASH buckets for ts_code")
    p_init.set_defaults(func=cmd_init_kline5m)

    p_drop = sub.add_parser("drop-db", help="Drop a DFS database (DANGEROUS)")
    p_drop.add_argument("--db-path", default="dfs://kline_5min", help='DFS path (default: dfs://kline_5min)')
    p_drop.add_argument("--yes", action="store_true", help="Confirm dropping the database")
    p_drop.set_defaults(func=cmd_drop_db)

    p_import = sub.add_parser("import-tdx-5min", help="Import local TDX lc5 5-min data into DolphinDB")
    p_import.add_argument("--tdx-dir", required=True, help="TDX install dir or vipdoc dir")
    p_import.set_defaults(func=cmd_import_tdx_5min)

    p_h5 = sub.add_parser(
        "import-hikyuu-5min",
        help="Import Hikyuu 5-min HDF5 (sh_5min.h5/sz_5min.h5/bj_5min.h5) into DolphinDB",
    )
    p_h5.add_argument(
        "--hikyuu-data-dir",
        default=None,
        help="Hikyuu data dir (e.g. E:/stock). Falls back to HIKYUU_DATA_DIR or config backtesting.hikyuu_data_dir",
    )
    p_h5.add_argument("--db-path", default="dfs://kline_5min", help='DFS path (default: dfs://kline_5min)')
    p_h5.add_argument("--table", default="kline_5min", help="Table name (default: kline_5min)")
    p_h5.add_argument("--codes", default=None, help="Comma-separated ts_code list, e.g. 000001.SZ,600000.SH")
    p_h5.add_argument("--codes-file", default=None, help="One ts_code per line")
    p_h5.add_argument("--start", default=None, help="Start date/time (inclusive), e.g. 2024-01-01")
    p_h5.add_argument("--end", default=None, help="End date/time (inclusive)")
    p_h5.add_argument(
        "--incremental",
        action="store_true",
        help="Incremental mode: only import rows newer than max(trade_time) in DolphinDB for each ts_code",
    )
    p_h5.add_argument("--chunk-rows", type=int, default=200_000, help="Upload chunk rows (default: 200000)")
    p_h5.add_argument("--price-scale", type=float, default=1000.0, help="Price scale (default: 1000)")
    p_h5.add_argument("--amount-scale", type=float, default=10.0, help="Amount scale (default: 10)")
    p_h5.add_argument("--dry-run", action="store_true", help="Read/transform only, no writes")
    p_h5.add_argument("--init", action="store_true", help="Init table before importing")
    p_h5.add_argument("--start-month", type=int, default=200501, help="(with --init) partition start month yyyymm")
    p_h5.add_argument("--end-month", type=int, default=203012, help="(with --init) partition end month yyyymm")
    p_h5.add_argument("--hash-buckets", type=int, default=10, help="(with --init) HASH buckets for ts_code")
    p_h5.set_defaults(func=cmd_import_hikyuu_5min)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(log_level=str(args.log_level).upper())

    if not getattr(args, "command", None):
        parser.print_help()
        print("\n" + "="*60)
        print("💡 迁移提示：建议使用统一CLI")
        print("   alphahome-ddb 命令将继续可用，但推荐使用:")
        print("   ah ddb ...")
        print("   例如: ah ddb init-kline5m --db-path dfs://kline_5min")
        print("="*60)
        return 1

    # 显示迁移提示（仅在非help场景）
    print("\n💡 提示：推荐使用统一CLI 'ah ddb ...' 替代 'alphahome-ddb ...'")
    print("   例如: ah ddb init-kline5m --db-path dfs://kline_5min")
    print()
    logging.getLogger(__name__).debug("Args: %s", args)

    try:
        return int(args.func(args))
    except Exception as e:
        logging.getLogger(__name__).error("Command failed: %s", e, exc_info=True)
        print(f"Error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


def main_sync() -> int:
    # 显示全局迁移提示
    print("提示：推荐使用统一CLI 'ah ddb ...' 替代 'alphahome-ddb ...'")
    print("      例如: ah ddb init-kline5m --db-path dfs://kline_5min")
    print()
    return main()


if __name__ == "__main__":
    raise SystemExit(main())

