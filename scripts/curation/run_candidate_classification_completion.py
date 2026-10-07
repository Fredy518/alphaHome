#!/usr/bin/env python
# ruff: noqa: E402
"""Plan/execute evidence-backed completion of missing ETF/LOF classifications."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from alphahome.common.config_manager import get_database_url
from alphahome.curation.candidate_llm_config import (
    DEFAULT_REVIEW_MODEL,
    resolve_candidate_llm_config,
)
from alphahome.curation.deepseek_candidate_client import CandidateLLMClient
from alphahome.curation.etf_candidate_ai_automation import execute_candidate_automation
from alphahome.curation.etf_candidate_classification_completion import (
    COMPLETION_GENERATION_SETTINGS,
    COMPLETION_PROMPT_VERSION,
    COMPLETION_SYSTEM_PROMPT,
    build_candidate_classification_completion_plan,
)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--expected-plan-hash")
    p.add_argument("--expected-target-count", type=int)
    p.add_argument("--run-date", type=date.fromisoformat, default=date.today())
    p.add_argument("--web-evidence", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--model", help="override GLMS_MODEL; defaults to deepseek-v4-pro")
    p.add_argument("--batch-size", type=int, default=8)
    p.add_argument("--workers", type=int, default=4)
    args = p.parse_args()
    config = resolve_candidate_llm_config(
        model=args.model, default_model=DEFAULT_REVIEW_MODEL
    )
    args.model = config.model
    if args.execute and not args.expected_plan_hash:
        p.error("--execute requires --expected-plan-hash")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(get_database_url())
    try:
        evidence = json.loads(args.web_evidence.read_text(encoding="utf-8"))
        plan = build_candidate_classification_completion_plan(
            conn,
            model_requested=args.model,
            llm_base_url=config.base_url,
            run_date=args.run_date,
            web_evidence=evidence,
            expected_target_count=args.expected_target_count,
        )
        conn.rollback()
        (args.output_dir / "plan.json").write_text(
            json.dumps(plan.plan_payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        if args.execute and plan.target_items:
            client = CandidateLLMClient(
                model=args.model,
                base_url=config.base_url,
                system_prompt=COMPLETION_SYSTEM_PROMPT,
                prompt_version=COMPLETION_PROMPT_VERSION,
                max_retries=4,
                timeout_seconds=240,
                **{
                    "thinking_enabled": COMPLETION_GENERATION_SETTINGS[
                        "thinking_enabled"
                    ],
                    "reasoning_effort": COMPLETION_GENERATION_SETTINGS[
                        "reasoning_effort"
                    ],
                    "max_output_tokens": COMPLETION_GENERATION_SETTINGS[
                        "max_output_tokens"
                    ],
                },
            )
            result = execute_candidate_automation(
                conn,
                plan,
                client=client,
                expected_plan_hash=args.expected_plan_hash,
                batch_size=args.batch_size,
                max_workers=args.workers,
                checkpoint_dir=args.output_dir / "batches" / COMPLETION_PROMPT_VERSION,
                progress_callback=lambda event: print(
                    json.dumps({"progress": event}), flush=True
                ),
            )
        else:
            result = plan.summary()
            if args.execute and not plan.target_items:
                result["status"] = "no_classification_gaps"
        name = "result.json" if args.execute else "plan_summary.json"
        (args.output_dir / name).write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
