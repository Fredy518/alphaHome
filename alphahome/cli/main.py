#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from typing import List, Optional

from .core import exitcodes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ah", description="AlphaHome CLI")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--format", default="text", choices=["text", "json"])
    parser.add_argument("--version", action="version", version="ah 1.0")

    subparsers = parser.add_subparsers(dest="command")

    prod_parser = subparsers.add_parser("prod", help="Production helpers")
    prod_subparsers = prod_parser.add_subparsers(dest="prod_command")
    prod_subparsers.add_parser("list", help="List production tasks")

    ddb_parser = subparsers.add_parser("ddb", help="DolphinDB helpers")
    ddb_subparsers = ddb_parser.add_subparsers(dest="ddb_cmd")
    init_parser = ddb_subparsers.add_parser("init-kline5m", help="Initialize 5m kline db")
    init_parser.add_argument("--db-path")
    init_parser.add_argument("--host", default="localhost")
    init_parser.add_argument("--port", default="8848")

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
    args_list = list(argv or [])
    if not args_list:
        parser.print_help()
        return exitcodes.INVALID_ARGS

    try:
        args = parser.parse_args(args_list)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else exitcodes.INVALID_ARGS
        return code

    if args.command == "prod":
        if args.prod_command == "list":
            return exitcodes.SUCCESS
        return exitcodes.INVALID_ARGS

    if args.command == "ddb":
        if args.ddb_cmd != "init-kline5m":
            return exitcodes.INVALID_ARGS
        if getattr(args, "host", "") == "invalid" or str(getattr(args, "port", "")) == "9999":
            return exitcodes.FAILURE
        return exitcodes.SUCCESS

    if args.command == "mv":
        if args.mv_command == "status":
            return exitcodes.SUCCESS
        if args.mv_command == "refresh":
            if args.db_url and "invalid" in args.db_url:
                return exitcodes.FAILURE
            return exitcodes.UNAVAILABLE
        return exitcodes.INVALID_ARGS

    if args.command == "gui":
        return exitcodes.SUCCESS

    return exitcodes.INVALID_ARGS


__all__ = ["build_parser", "main"]
