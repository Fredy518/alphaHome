from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from fundpos.contract_evidence import collect_contract_evidence, register_contract_evidence

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PILOT = ROOT / "resources" / "universe" / "pilot_110.parquet"
DEFAULT_SUPPLEMENTS = (ROOT / ".." / ".." / "logs" / "fundpos-engine" / "supplements").resolve()


def main():
    parser = argparse.ArgumentParser(description="下载并结构化试测基金合同约束")
    parser.add_argument("--pilot", type=Path, default=DEFAULT_PILOT)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--supplements", type=Path, default=DEFAULT_SUPPLEMENTS)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    evidence, constraints, summary = collect_contract_evidence(
        pd.read_parquet(args.pilot),
        args.cutoff,
        args.supplements,
        workers=args.workers,
    )
    register_contract_evidence(args.supplements, summary)
    print(
        json.dumps(
            summary | {"evidence_rows": len(evidence), "constraint_rows": len(constraints)},
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
