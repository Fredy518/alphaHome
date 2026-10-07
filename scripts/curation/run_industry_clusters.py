#!/usr/bin/env python
"""Build/update representative industry clusters, then optionally publish saved results."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
import sys

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from alphahome.common.config_manager import get_database_url
from alphahome.curation.industry_clusters.engine import ClusterConfig
from alphahome.curation.industry_clusters.data import load_inputs
from alphahome.curation.industry_clusters.service import run_history
from alphahome.curation.industry_clusters.store import publish_series, read_previous, read_universe


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "update", "publish"))
    parser.add_argument("--universe-id")
    parser.add_argument("--universe-file", type=Path)
    parser.add_argument("--start")
    parser.add_argument("--as-of")
    parser.add_argument("--config", type=Path, default=ROOT / "artifacts/industry_clusters/industry_minimax_v2.json")
    parser.add_argument("--input-cache-dir", type=Path, help="Reuse an existing frozen input cache for a matched comparison.")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--source-description", default="Frozen industry index library; ETF eligibility is applied by consumers.")
    args = parser.parse_args()
    conn = psycopg2.connect(get_database_url())
    try:
        if args.command == "publish":
            with gzip.open(args.output_dir / "series.json.gz", "rt", encoding="utf-8") as source:
                payload = json.load(source)
            result = publish_series(conn, payload)
            (args.output_dir / "publication.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return
        if not args.universe_id or not args.as_of:
            parser.error("--universe-id and --as-of are required")
        if pd.Timestamp(args.as_of).normalize() > pd.Timestamp.now(tz="Asia/Shanghai").normalize().tz_localize(None):
            parser.error("--as-of cannot be a future date")
        config = ClusterConfig.from_dict(json.loads(args.config.read_text(encoding="utf-8"))) if args.config else ClusterConfig()
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        if args.command == "build":
            if args.universe_file is None or args.start is None:
                parser.error("build requires --universe-file and --start")
            universe = pd.read_csv(args.universe_file, dtype={"index_code": str})
            if "index_name" not in universe:
                universe["index_name"] = universe.index_code
            universe = universe[["index_code", "index_name"]].sort_values("index_code").reset_index(drop=True)
            previous = None
            start = args.start
        else:
            universe = pd.DataFrame(read_universe(conn, args.universe_id)).sort_values("index_code").reset_index(drop=True)
            previous = read_previous(conn, args.universe_id, args.as_of, config.version)
            if previous is None:
                parser.error("update requires a previously published snapshot")
            start = str((pd.Timestamp(previous["state"]["asof"]) + pd.offsets.MonthEnd(1)).date())
        if universe.index_code.duplicated().any() or universe.index_code.isna().any():
            raise ValueError("universe index codes must be unique and non-null")
        inputs = load_inputs(conn, universe, start, args.as_of, args.input_cache_dir or args.output_dir / "inputs", config)
        result = run_history(inputs, universe, start, args.as_of, config, args.output_dir,
                             args.universe_id, args.source_description, previous)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
