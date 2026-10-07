#!/usr/bin/env python
# ruff: noqa: E402
"""Plan/execute an evidence-enriched model review of pending ETF classifications."""

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
from alphahome.curation.etf_candidate_ai_automation import (
    CandidateAutomationError,
    execute_candidate_automation,
    get_existing_month_result,
)
from alphahome.curation.etf_candidate_second_review import (
    REVIEW_PROMPT_VERSION,
    REVIEW_SYSTEM_PROMPT,
    REVIEW_GENERATION_SETTINGS,
    build_candidate_second_review_plan,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--expected-plan-hash")
    parser.add_argument("--expected-target-count", type=int)
    parser.add_argument("--run-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--model", help="override GLMS_MODEL; defaults to deepseek-v4-pro")
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    config = resolve_candidate_llm_config(
        model=args.model, default_model=DEFAULT_REVIEW_MODEL
    )
    args.model = config.model
    if args.execute and not args.expected_plan_hash:
        parser.error("--execute requires --expected-plan-hash")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    connection = psycopg2.connect(get_database_url())
    try:
        existing = get_existing_month_result(
            connection, args.run_date.replace(day=1).isoformat(), "review"
        )
        connection.rollback()
        if existing:
            print(json.dumps(existing, ensure_ascii=False, indent=2, default=str))
            return 0
        plan = build_candidate_second_review_plan(
            connection,
            model_requested=args.model,
            llm_base_url=config.base_url,
            run_date=args.run_date,
            expected_target_count=args.expected_target_count,
        )
        connection.rollback()
        if args.execute:
            if args.expected_plan_hash != plan.plan_hash:
                raise CandidateAutomationError(
                    "second review plan changed; preview again"
                )
            client = (
                CandidateLLMClient(
                    model=args.model,
                    base_url=config.base_url,
                    system_prompt=REVIEW_SYSTEM_PROMPT,
                    prompt_version=REVIEW_PROMPT_VERSION,
                    max_retries=6,
                    timeout_seconds=180,
                    thinking_enabled=REVIEW_GENERATION_SETTINGS["thinking_enabled"],
                    reasoning_effort=REVIEW_GENERATION_SETTINGS["reasoning_effort"],
                    max_output_tokens=REVIEW_GENERATION_SETTINGS["max_output_tokens"],
                )
                if plan.target_items
                else None
            )
            result = execute_candidate_automation(
                connection,
                plan,
                client=client,
                expected_plan_hash=args.expected_plan_hash,
                batch_size=args.batch_size,
                max_workers=args.workers,
                checkpoint_dir=args.output_dir / "batches" / REVIEW_PROMPT_VERSION,
                progress_callback=lambda event: print(
                    json.dumps({"progress": event}), flush=True
                ),
            )
            output_name = "result.json"
        else:
            (args.output_dir / "plan.json").write_text(
                json.dumps(
                    plan.plan_payload, ensure_ascii=False, indent=2, default=str
                ),
                encoding="utf-8",
            )
            result = plan.summary()
            output_name = "plan_summary.json"
        (args.output_dir / output_name).write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
