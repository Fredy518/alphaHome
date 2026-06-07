#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P factor yearly worker compatibility entrypoint."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.factors.pipelines.cli import run_year_worker


def main() -> int:
    parser = argparse.ArgumentParser(description="P因子年度并行计算脚本")
    parser.add_argument("--start_year", type=int, required=True, help="开始年份")
    parser.add_argument("--end_year", type=int, required=True, help="结束年份")
    parser.add_argument("--worker_id", type=int, required=True, help="当前worker ID (从0开始)")
    parser.add_argument("--total_workers", type=int, required=True, help="总worker数量")
    args = parser.parse_args()
    return run_year_worker("p", args.start_year, args.end_year, args.worker_id, args.total_workers)


if __name__ == "__main__":
    sys.exit(main())
