#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Batch missing P/G factor compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from alphahome.factors.pipelines.cli import run_missing_factors


def main() -> int:
    parser = argparse.ArgumentParser(description="批量补齐缺失的P/G因子数据")
    parser.add_argument("--start-date", required=True, help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="日志级别")
    parser.add_argument("--dry-run", action="store_true", help="仅分析不实际计算")
    args = parser.parse_args()
    return run_missing_factors(args.start_date, args.end_date, args.log_level, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
