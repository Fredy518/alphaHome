#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""G factor specified-date compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.factors.pipelines.cli import run_specific_dates


def main() -> int:
    parser = argparse.ArgumentParser(description="G因子指定日期计算器")
    parser.add_argument("--dates", nargs="+", required=True, help="需要计算的日期列表，格式: YYYY-MM-DD YYYY-MM-DD ...")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="日志级别")
    args = parser.parse_args()
    return run_specific_dates("g", args.dates, args.log_level)


if __name__ == "__main__":
    sys.exit(main())
