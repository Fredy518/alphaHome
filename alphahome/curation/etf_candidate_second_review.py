"""Evidence-enriched second model review of AI_REVIEW_REQUIRED candidates."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import date
from typing import Any

from psycopg2.extras import RealDictCursor

from alphahome.curation.deepseek_candidate_client import canonical_json, sha256_json
from alphahome.curation.etf_candidate_ai_automation import (
    ADD_REQUIRED_FIELDS,
    CURATED_PATCH_FIELDS,
    EXPOSURE_CLASS_FIELDS,
    CandidateAutomationError,
    CandidateAutomationPlan,
    DecisionEnvelope,
    _decision_confirmation_status,
    _subset,
    build_candidate_automation_plan,
)
from alphahome.curation.etf_candidate_master import (
    CONFIRMATION_STATUS_AI,
    CONFIRMATION_STATUS_AI_REVIEW,
)

REVIEW_PROMPT_VERSION = "etf_candidate_second_review_v2"
REVIEW_MIN_CONFIDENCE = 0.85
REVIEW_GENERATION_SETTINGS = {
    "thinking_enabled": True,
    "reasoning_effort": "high",
    "max_output_tokens": 24576,
}
REVIEW_FROZEN_FIELDS = {
    "exposure_id",
    "candidate_status",
    "product_role",
    "parent_fund_code",
    "exposure_relationship",
    "source_supplement",
}
REVIEW_PATCH_FIELDS = tuple(
    f for f in CURATED_PATCH_FIELDS if f not in REVIEW_FROZEN_FIELDS
)
REVIEW_SYSTEM_PROMPT = """你是 AlphaHome ETF 研究候选池的第二轮分类核对模型。
逐只独立核对：基金身份、跟踪指数、资产大类、配置模块、地域、行业/风格归属和已有分类。
根据输入中的基金名录、指数名录、指数类别、同指数产品、同暴露的指数变体、第一轮疑点交叉核验。
只能使用输入资料，不得声称查阅了未提供的持仓、编制方案或官方网站，不得编造指数成分、权重或重合度。
本轮产品入池资格、候选等级、产品角色、父工具关系及 exposure_id 均冻结，不得修改这些字段。
暴露编号是治理身份：原有暴露允许不同指数实施变体；ETF_INDEX_* 是本地按指数代码分配的稳定身份。
不同指数共用原有母暴露不自动构成错误；不得以指数不同为由拆分或新建编号。
风险研究与分类核验分开：规模小、历史短、未来需检查交易性、尚未定量研究交叉指数重合度，
本身不等于基金身份或分类不明。可在 risk_boundary 中保留限制，并维持已有观察等级。
同一指数先核对共同的源资料和规范分类，字段缺失/null 与非空字段本身不构成语义冲突。
对真实冲突、名称歧义、无法证实的行业/风格或预算归属，必须保留 requires_human_review=true 并说明最小缺失证据。
不能仅因用户希望核对就确认全部；仅当来源足以支持当前分类或明确更正后分类时给出不低于 0.85 的置信度。
只有 KEEP 和 UPDATE 两种动作，include_in_candidate_pool 恒为 true。KEEP 的 classification_patch 必须为空。
UPDATE 只含确需修正且 allowed_patch_fields 允许的字段；枚举必须逐字使用 allowed_values，不得自行扩充。
没有充分证据时不要用相近枚举凑分类。可选字段可保留原值或 null，非空必需字段不得清空。
evidence 至少两条，必须引用每只产品的 required_evidence_anchors 中的全部实际值：基金代码、指数代码和指数全名。
第一条写明基金源表的本只基金代码及对应指数代码，第二条写明指数源表的完整指数名称和支持的分类。
不得只输出“基金名称一致”“指数全名支持类别”等没有任何实际值的空泛模板。
decision_summary 必须逐只回答第一轮的具体疑点为何解决或仍未解决，不能通用地声称“原疑点已解决”。
只输出合法 JSON 对象，唯一外层键 decisions 是数组，每个输入 fund_code 必须恰好一次。
每条决定严格包含下列十个键：fund_code(输入基金代码字符串)、action(KEEP或UPDATE)、
include_in_candidate_pool(true)、confidence(0到1数值)、classification_patch(字段修正对象，KEEP时为空)、
decision_summary(具体核对结论字符串)、evidence(含实际源值的字符串数组)、
uncertainty(仍缺证据的字符串数组)、requires_human_review(布尔)。不得增加其他键，不要 Markdown。
decision_summary、每条 evidence 和每条 uncertainty 不超过 100 个汉字。"""
REVIEW_PROMPT_SHA256 = hashlib.sha256(REVIEW_SYSTEM_PROMPT.encode("utf-8")).hexdigest()


def _read_review_evidence(connection: Any) -> dict[str, dict[str, Any]]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT c.fund_code, jsonb_build_object(
                'previous_ai_run_id', c.ai_run_id,
                'previous_decision_hash', c.ai_decision_hash,
                'previous_decision', d.decision_payload,
                'previous_model_decision', d.evidence_payload->'model_decision',
                'previous_local_adjustments', d.evidence_payload->'identity_adjustments',
                'rawdata.fund_etf_basic', to_jsonb(e),
                'rawdata.fund_basic', jsonb_build_object(
                    'ts_code', b.ts_code, 'name', b.name, 'fund_type', b.fund_type,
                    'invest_type', b.invest_type, 'benchmark', b.benchmark,
                    'status', b.status, 'list_date', b.list_date,
                    'market', b.market, 'update_time', b.update_time),
                'rawdata.fund_etf_index', to_jsonb(x),
                'rawdata.index_basic', to_jsonb(i)
            ) AS evidence
            FROM fund_pool_on.etf_candidate_master_current c
            LEFT JOIN fund_pool_on.etf_candidate_ai_decision d
                ON d.ai_run_id=c.ai_run_id AND d.fund_code=c.fund_code
            LEFT JOIN rawdata.fund_etf_basic e ON e.ts_code=c.fund_code
            LEFT JOIN rawdata.fund_basic b ON b.ts_code=c.fund_code
            LEFT JOIN rawdata.fund_etf_index x ON x.ts_code=c.tracking_index_code
            LEFT JOIN rawdata.index_basic i ON i.ts_code=c.tracking_index_code
            WHERE c.confirmation_status='AI_REVIEW_REQUIRED'
            ORDER BY c.fund_code
        """
        )
        return {row["fund_code"]: row["evidence"] for row in cursor.fetchall()}


def build_candidate_second_review_plan(
    connection: Any,
    *,
    model_requested: str,
    llm_base_url: str | None = None,
    run_date: date,
    expected_target_count: int | None = None,
) -> CandidateAutomationPlan:
    # Reuse ordinary freshness, PIT, membership and source-drift protections.
    base = build_candidate_automation_plan(
        connection,
        model_requested=model_requested,
        llm_base_url=llm_base_url,
        run_date=run_date,
        reconfirm_all=True,
        max_new_products=3000,
    )
    evidence = _read_review_evidence(connection)
    targets = {
        r["fund_code"]
        for r in base.current_records
        if r["confirmation_status"] == CONFIRMATION_STATUS_AI_REVIEW
    }
    by_index: dict[str, list[dict]] = defaultdict(list)
    by_exposure: dict[str, list[dict]] = defaultdict(list)
    for row in base.current_records:
        by_index[row.get("tracking_index_code")].append(row)
        by_exposure[row["exposure_id"]].append(row)
    current_by_code = {row["fund_code"]: row for row in base.current_records}
    items = []
    for item in base.target_items:
        code = item["fund_code"]
        if code not in targets:
            continue
        current = current_by_code[code]
        peers = by_index[current.get("tracking_index_code")]
        peer_classes = {
            canonical_json(
                _subset(peer, (*EXPOSURE_CLASS_FIELDS, "exposure_name", "exposure_id"))
            )
            for peer in peers
        }
        variants = {
            canonical_json(
                _subset(
                    peer,
                    (
                        "tracking_index_code",
                        "tracking_index_name",
                        "exposure_name",
                        "exposure_relationship",
                    ),
                )
            )
            for peer in by_exposure[current["exposure_id"]]
        }
        items.append(
            {
                **item,
                "reason": "second_model_review",
                "review_evidence": evidence.get(code),
                "required_evidence_anchors": [
                    value
                    for value in (
                        code,
                        current.get("tracking_index_code"),
                        (
                            (evidence.get(code) or {}).get("rawdata.fund_etf_index")
                            or {}
                        ).get("index_name")
                        or (
                            (evidence.get(code) or {}).get("rawdata.index_basic") or {}
                        ).get("name")
                        or item["live_product_facts"].get("tracking_index_name"),
                    )
                    if value
                ],
                "same_index_products": [
                    _subset(
                        peer,
                        (
                            "fund_code",
                            "fund_name",
                            "product_role",
                            "candidate_status",
                            "confirmation_status",
                        ),
                    )
                    for peer in peers
                ],
                "same_index_classifications": [
                    json.loads(value) for value in sorted(peer_classes)
                ],
                "same_exposure_index_variants": [
                    json.loads(value) for value in sorted(variants)
                ],
            }
        )
    items.sort(
        key=lambda row: (
            str(row["live_product_facts"].get("tracking_index_code")),
            row["fund_code"],
        )
    )
    taxonomy = deepcopy(base.taxonomy)
    taxonomy.update(
        allowed_patch_fields=list(REVIEW_PATCH_FIELDS),
        non_null_patch_fields=sorted(ADD_REQUIRED_FIELDS & set(REVIEW_PATCH_FIELDS)),
        minimum_automatic_confidence=REVIEW_MIN_CONFIDENCE,
        frozen_fields=sorted(REVIEW_FROZEN_FIELDS),
        require_evidence_anchors=True,
    )
    # Identity is frozen; per-item peers replace the large, lossy first-row dictionary.
    taxonomy.pop("exposure_reference", None)
    payload = deepcopy(base.plan_payload)
    for code, row in payload["coverage"].items():
        if code in current_by_code:
            row["state"] = (
                "model_review_target" if code in targets else "current_candidate"
            )
    counts = Counter(row["state"] for row in payload["coverage"].values())
    guards = payload["guards"]
    guards.update(
        review_target_count_matches=(
            expected_target_count is None or len(targets) == expected_target_count
        ),
        review_target_evidence_complete=all(
            evidence.get(code, {}).get("previous_decision") for code in targets
        ),
        review_target_facts_valid={item["fund_code"] for item in items} == targets,
        new_product_count_within_limit=True,
    )
    payload.update(
        contract="etf_candidate_second_review_plan_v1",
        screening_scope="review",
        exposure_identity_policy="review_preserves_identity_and_product_grade_v1",
        prompt_version=REVIEW_PROMPT_VERSION,
        prompt_sha256=REVIEW_PROMPT_SHA256,
        generation_settings=REVIEW_GENERATION_SETTINGS,
        expected_target_count=expected_target_count,
        llm_target_count=len(items),
        new_product_count=0,
        discovered_new_product_count=0,
        target_reasons={"second_model_review": len(items)},
        target_items=items,
        taxonomy=taxonomy,
        coverage_counts={"total": sum(counts.values()), **counts},
        review_evidence_fingerprint=sha256_json(evidence),
        executable=base.executable
        and all(
            guards[key]
            for key in (
                "review_target_count_matches",
                "review_target_evidence_complete",
                "review_target_facts_valid",
            )
        ),
    )
    return CandidateAutomationPlan(
        plan_hash=sha256_json(payload),
        plan_payload=payload,
        source_batch=base.source_batch,
        current_records=base.current_records,
        facts_by_code=base.facts_by_code,
        target_items=items,
        taxonomy=taxonomy,
    )


def resolve_review_consistency(
    plan: CandidateAutomationPlan, envelopes: list[DecisionEnvelope]
) -> list[DecisionEnvelope]:
    """Apply only target-scoped classification corrections with consistent index peers."""
    before = {row["fund_code"]: row for row in plan.current_records}
    proposals = {code: deepcopy(row) for code, row in before.items()}
    reasons: dict[str, list[str]] = defaultdict(list)
    for envelope in envelopes:
        decision = envelope.decision
        code = decision["fund_code"]
        if (
            code not in before
            or before[code]["confirmation_status"] != CONFIRMATION_STATUS_AI_REVIEW
        ):
            raise CandidateAutomationError(
                "second review attempted to change a non-review candidate"
            )
        if decision["action"] not in ("KEEP", "UPDATE"):
            raise CandidateAutomationError(
                "second review cannot change candidate membership"
            )
        if set(decision["classification_patch"]) - set(REVIEW_PATCH_FIELDS):
            raise CandidateAutomationError(
                "second review attempted to change frozen identity/grade"
            )
        if decision["confidence"] < REVIEW_MIN_CONFIDENCE:
            reasons[code].append("二轮核对置信度低于0.85，继续待复核")
        if not before[code].get("tracking_index_code"):
            reasons[code].append("缺少跟踪指数身份，继续待复核")
        if len(decision["evidence"]) < 2:
            reasons[code].append("二轮交叉核对证据不足两条，继续待复核")
        if (
            _decision_confirmation_status(decision) == CONFIRMATION_STATUS_AI
            and not reasons[code]
        ):
            proposals[code].update(decision["classification_patch"])

    # A correction may not introduce a conflicting common classification into an
    # otherwise consistent index group. Unreviewed peers stay untouched.
    index_groups: dict[str, list[str]] = defaultdict(list)
    for code, row in before.items():
        if row.get("tracking_index_code"):
            index_groups[row["tracking_index_code"]].append(code)
    while True:
        rejected_codes = set()
        for codes in index_groups.values():
            for field in (*EXPOSURE_CLASS_FIELDS, "exposure_name"):
                old_values = {
                    before[code].get(field) for code in codes if before[code].get(field)
                }
                new_values = {
                    proposals[code].get(field)
                    for code in codes
                    if proposals[code].get(field)
                }
                if len(new_values) > 1 and new_values != old_values:
                    for code in codes:
                        if proposals[code].get(field) != before[code].get(field):
                            reasons[code].append(
                                f"{field}修正与同指数保留记录不一致，继续待复核"
                            )
                            rejected_codes.add(code)
        if not rejected_codes:
            break
        for code in rejected_codes:
            proposals[code] = deepcopy(before[code])

    resolved = []
    for envelope in envelopes:
        decision = deepcopy(envelope.decision)
        code = decision["fund_code"]
        if reasons[code]:
            decision["requires_human_review"] = True
            decision["uncertainty"] = list(
                dict.fromkeys(decision["uncertainty"] + reasons[code])
            )
        resolved.append(
            DecisionEnvelope(
                decision=decision,
                result=envelope.result,
                raw_decision=(
                    envelope.decision if decision != envelope.decision else None
                ),
                identity_adjustments=(
                    {"review_reasons": reasons[code]} if reasons[code] else None
                ),
            )
        )
    return resolved
