#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from .core import exitcodes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ah", description="Retired AlphaHome CLI; use alphahome.factors / alphahome.features / PIT entrypoints")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--format", default="text", choices=["text", "json"])
    parser.add_argument("--version", action="version", version="ah 1.0")

    subparsers = parser.add_subparsers(dest="command")

    prod_parser = subparsers.add_parser("prod", help="Production helpers")
    prod_subparsers = prod_parser.add_subparsers(dest="prod_command")
    prod_subparsers.add_parser("list", help="List production tasks")

    mv_parser = subparsers.add_parser("mv", help="Materialized view helpers")
    mv_subparsers = mv_parser.add_subparsers(dest="mv_command")
    mv_status = mv_subparsers.add_parser("status", help="Show materialized view status")
    mv_status.add_argument("view_name")
    mv_refresh = mv_subparsers.add_parser("refresh", help="Refresh materialized view")
    mv_refresh.add_argument("view_name")
    mv_refresh.add_argument("--db-url")

    subparsers.add_parser("gui", help="Launch GUI")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args_list = list(sys.argv[1:] if argv is None else argv)
    if not args_list:
        parser.print_help()
        return exitcodes.INVALID_ARGS

    try:
        parser.parse_args(args_list)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else exitcodes.INVALID_ARGS
        return code

    print("旧统一 CLI 已下线。使用 python run.py、python -m alphahome.factors 或 python -m alphahome.features。", file=sys.stderr)
    return exitcodes.UNAVAILABLE


__all__ = ["build_parser", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
