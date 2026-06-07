#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Recent missing P/G factor compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from alphahome.factors.pipelines.cli import run_recent_missing_factors


def main() -> int:
    parser = argparse.ArgumentParser(description="批量补齐最近缺失的P/G因子数据")
    parser.add_argument("--months", type=int, default=3, help="补齐最近几个月的数据 (默认: 3)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="日志级别")
    args = parser.parse_args()
    return run_recent_missing_factors(args.months, args.log_level)


if __name__ == "__main__":
    sys.exit(main())
