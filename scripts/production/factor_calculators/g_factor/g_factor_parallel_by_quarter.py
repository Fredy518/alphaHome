#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""G factor quarterly worker compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.factors.pipelines.cli import run_quarter_worker


def main() -> int:
    parser = argparse.ArgumentParser(description="G因子季度并行计算")
    parser.add_argument("--worker_id", type=int, required=True, help="工作进程ID (0-based)")
    parser.add_argument("--total_workers", type=int, required=True, help="总工作进程数")
    parser.add_argument("--start_quarter", type=str, help="开始季度，格式: YYYYQN (例如: 2025Q3)")
    parser.add_argument("--end_quarter", type=str, help="结束季度，格式: YYYYQN (例如: 2025Q4)")
    parser.add_argument("--quarter", action="append", help="季度，格式: YYYYQN (可多次指定，与范围参数互斥)")
    args = parser.parse_args()
    return run_quarter_worker("g", args.worker_id, args.total_workers, args.quarter, args.start_quarter, args.end_quarter)


if __name__ == "__main__":
    sys.exit(main())
