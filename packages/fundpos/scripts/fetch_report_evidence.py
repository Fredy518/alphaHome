from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from fundpos.report_evidence import collect_report_evidence, register_report_evidence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PILOT = ROOT / "resources" / "universe" / "pilot_110.parquet"
DEFAULT_SUPPLEMENTS = (ROOT / ".." / ".." / "logs" / "fundpos-engine" / "supplements").resolve()


def main():
    parser = argparse.ArgumentParser(description="下载试测基金定期报告并生成公告日期证据")
    parser.add_argument("--pilot", type=Path, default=DEFAULT_PILOT)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--supplements", type=Path, default=DEFAULT_SUPPLEMENTS)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument(
        "--selection-group",
        help="Only fetch one frozen pilot selection_group; existing evidence is preserved",
    )
    args = parser.parse_args()
    pilot = pd.read_parquet(args.pilot)
    if args.selection_group:
        pilot = pilot.loc[pilot.selection_group.eq(args.selection_group)].copy()
        if pilot.empty:
            raise ValueError(f"No pilot rows for selection_group={args.selection_group}")
    evidence, summary = collect_report_evidence(
        pilot,
        args.start,
        args.end,
        args.supplements,
        workers=args.workers,
    )
    register_report_evidence(
        args.supplements, args.supplements / "report_evidence.parquet", summary
    )
    print(json.dumps(summary | {"evidence_rows": len(evidence)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
