#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Production P factor range runner compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.factors.pipelines.cli import run_range


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="生产级P因子计算运维脚本")
    parser.add_argument("--start-date", type=str, required=True, help="开始日期 (YYYY-MM-DD格式)")
    parser.add_argument("--end-date", type=str, required=True, help="结束日期 (YYYY-MM-DD格式)")
    parser.add_argument("--mode", type=str, choices=["auto", "incremental", "backfill"], default="auto", help="执行模式")
    parser.add_argument("--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="日志级别")
    parser.add_argument("--log-file", type=str, help="自定义日志文件名. 默认: 自动生成")
    parser.add_argument("--dry-run", action="store_true", help="试运行模式，只检查配置和数据状态，不执行实际计算")
    parser.add_argument("--validate-only", action="store_true", help="仅执行数据质量验证，不进行计算")
    return parser


def main() -> int:
    args = create_argument_parser().parse_args()
    return run_range(
        "p",
        args.start_date,
        args.end_date,
        mode=args.mode,
        dry_run=args.dry_run,
        validate_only=args.validate_only,
        log_level=args.log_level,
        log_file=args.log_file,
    )


if __name__ == "__main__":
    sys.exit(main())
