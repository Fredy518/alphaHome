"""通过公司 GLMS 网关调用的 ETF/LOF 候选确认客户端。

模型只返回结构化研究分类建议；本模块不提供资金或下单能力。
保留 DeepSeek 类名与模块路径，兼容已有调用和历史批次。
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests

from alphahome.curation.candidate_llm_config import (
    DEFAULT_BASE_URL,
    DEFAULT_MODEL,
    get_glms_api_key,
    resolve_candidate_llm_config,
)

PROMPT_VERSION = "exchange_fund_candidate_confirmation_v5"
ALLOWED_ACTIONS = {"KEEP", "UPDATE", "ADD", "EXCLUDE_NEW"}

SYSTEM_PROMPT = """你是 AlphaHome 的 ETF/LOF 候选池分类复核器。
你的任务仅限候选研究分类，不得生成买卖、仓位、资金分配或下单建议。
只能使用输入 JSON 中的产品事实、当前记录和分类参照；不能补造产品事实。
若证据不足，应提高 uncertainty、将 requires_human_review 设为 true，必要时对新产品给出 EXCLUDE_NEW。
现有产品只能返回 KEEP 或 UPDATE；新产品只能返回 ADD 或 EXCLUDE_NEW。
KEEP 的 classification_patch 必须是空对象。UPDATE 只返回确需变更的字段。
ADD 必须返回完整分类字段。include_in_candidate_pool 对 KEEP/UPDATE/ADD 为 true，
对 EXCLUDE_NEW 为 false。
地区、一级分类、二级分类必须完整；现有档案缺项时应有证据地补全，无法确认时保留待复核。
一级/二级组合必须符合 taxonomy.level2_by_level1，不能用不适用的相近类别填空。
分类字段必须使用 taxonomy.allowed_values 中的原值，不得自造枚举。
先匹配 exposure_reference 和 same_index_candidates；同一指数优先复用已有 exposure_id。
不同指数即使简称相近，也不得仅凭名字当成同一暴露。新暴露的 ID 使用清晰稳定的英文与数字下划线。
筛选覆盖存量及新上市 ETF 和 LOF。规模、成交额和成立时长是产品辅助判断，不是获准入池的充分条件。
LOF包含主动管理、指数、ETF联接、QDII和商品基金；有场内上市代码的联接LOF可以作为候选。
LOF的benchmark只是业绩比较基准，绝不能当成跟踪指数代码或据此认为产品与ETF等价。
主动LOF没有跟踪指数是正常结构，不应仅因此排除；依据基金名称、投资类型和基准确定有证据的分类。
主动管理的风格漂移、QDII净值时差、场内折溢价和申赎限制应写入risk_boundary/execution_check。
没有确认同指数身份的LOF使用独立产品暴露，不能自动作为ETF的同指数备份。
新增应说明其可识别暴露和候选用途；缺少分类证据或只是没有新增用途的重复工具可 EXCLUDE_NEW。
不能根据指数代码猜测未知指数内容；指数名称和基金名称有歧义时应标记人工复核。

必须只输出一个合法 JSON 对象，不要 Markdown。JSON 结构：
{
  "decisions": [
    {
      "fund_code": "510000.SH",
      "action": "KEEP",
      "include_in_candidate_pool": true,
      "confidence": 0.92,
      "classification_patch": {},
      "decision_summary": "当前分类与产品事实一致",
      "evidence": ["基金名称与跟踪指数代码和现有暴露一致"],
      "uncertainty": [],
      "requires_human_review": false
    }
  ]
}
confidence 必须是 0 到 1 的数字。每个输入 fund_code 必须且只能出现一次。
decision_summary、每条 evidence 和每条 uncertainty 都应简洁，不超过 80 个汉字。
"""
PROMPT_SHA256 = hashlib.sha256(SYSTEM_PROMPT.encode("utf-8")).hexdigest()


class DeepSeekCandidateError(RuntimeError):
    """模型请求、JSON 解析或本地结构验证失败（兼容旧异常名）。"""


@dataclass(frozen=True)
class DeepSeekBatchResult:
    """一次模型批次调用及其可审计元数据。"""

    decisions: list[dict[str, Any]]
    response_id: str | None
    actual_model: str
    system_fingerprint: str | None
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    input_hash: str
    output_hash: str


def canonical_json(value: Any) -> str:
    """生成稳定 UTF-8 JSON，用于运行计划和响应哈希。"""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _normalized_codes(items: Iterable[dict[str, Any]]) -> list[str]:
    codes: list[str] = []
    for item in items:
        code = str(item.get("fund_code") or "").strip().upper()
        if not code:
            raise DeepSeekCandidateError("input item has blank fund_code")
        codes.append(code)
    if len(codes) != len(set(codes)):
        raise DeepSeekCandidateError("input items contain duplicate fund_code")
    return codes


def validate_decisions(
    payload: Any,
    *,
    input_items: list[dict[str, Any]],
    taxonomy: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """本地验证模型 JSON；任何不完整输出都失败关闭。"""

    if not isinstance(payload, dict) or set(payload) != {"decisions"}:
        raise DeepSeekCandidateError(
            "model output must be an object containing only decisions"
        )
    decisions = payload["decisions"]
    if not isinstance(decisions, list):
        raise DeepSeekCandidateError("model output decisions must be a list")

    expected_codes = _normalized_codes(input_items)
    kind_by_code = {
        str(item["fund_code"]).strip().upper(): str(item.get("kind"))
        for item in input_items
    }
    item_by_code = {
        str(item["fund_code"]).strip().upper(): item for item in input_items
    }
    normalized: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    required_keys = {
        "fund_code",
        "action",
        "include_in_candidate_pool",
        "confidence",
        "classification_patch",
        "decision_summary",
        "evidence",
        "uncertainty",
        "requires_human_review",
    }

    for index, decision in enumerate(decisions, start=1):
        if not isinstance(decision, dict):
            raise DeepSeekCandidateError(f"decision {index} must be an object")
        if set(decision) != required_keys:
            missing = sorted(required_keys - set(decision))
            extra = sorted(set(decision) - required_keys)
            raise DeepSeekCandidateError(
                f"decision {index} keys mismatch; missing={missing}, extra={extra}"
            )
        code = str(decision["fund_code"]).strip().upper()
        if code not in kind_by_code or code in seen_codes:
            raise DeepSeekCandidateError(
                f"unexpected or duplicate model fund_code: {code}"
            )
        seen_codes.add(code)

        action = str(decision["action"]).strip().upper()
        if action not in ALLOWED_ACTIONS:
            raise DeepSeekCandidateError(f"invalid action for {code}: {action}")
        kind = kind_by_code[code]
        if kind == "existing" and action not in {"KEEP", "UPDATE"}:
            raise DeepSeekCandidateError(
                f"existing candidate {code} cannot use action {action}"
            )
        if kind == "new" and action not in {"ADD", "EXCLUDE_NEW"}:
            raise DeepSeekCandidateError(
                f"new product {code} cannot use action {action}"
            )

        include = decision["include_in_candidate_pool"]
        if not isinstance(include, bool):
            raise DeepSeekCandidateError(
                f"include_in_candidate_pool must be boolean for {code}"
            )
        expected_include = action != "EXCLUDE_NEW"
        if include is not expected_include:
            raise DeepSeekCandidateError(
                f"action/include mismatch for {code}: {action}/{include}"
            )

        confidence = decision["confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
            raise DeepSeekCandidateError(f"confidence must be numeric for {code}")
        if not 0.0 <= float(confidence) <= 1.0:
            raise DeepSeekCandidateError(f"confidence out of range for {code}")
        patch = decision["classification_patch"]
        if not isinstance(patch, dict):
            raise DeepSeekCandidateError(
                f"classification_patch must be an object for {code}"
            )
        if action == "KEEP" and patch:
            raise DeepSeekCandidateError(
                f"KEEP classification_patch must be empty for {code}"
            )
        if action == "UPDATE" and not patch:
            raise DeepSeekCandidateError(
                f"UPDATE classification_patch must not be empty for {code}"
            )
        if taxonomy:
            allowed_patch_fields = set(taxonomy.get("allowed_patch_fields") or [])
            extra_patch_fields = set(patch) - allowed_patch_fields
            if extra_patch_fields:
                raise DeepSeekCandidateError(
                    f"classification_patch has forbidden fields for {code}: "
                    f"{sorted(extra_patch_fields)}"
                )
            if action == "ADD":
                required_add_fields = set(taxonomy.get("required_add_fields") or [])
                missing_add_fields = {
                    field
                    for field in required_add_fields
                    if not str(patch.get(field) or "").strip()
                }
                if missing_add_fields:
                    raise DeepSeekCandidateError(
                        f"ADD classification_patch missing fields for {code}: "
                        f"{sorted(missing_add_fields)}"
                    )
            for field, value in patch.items():
                if field in taxonomy.get("non_null_patch_fields", ()) and not value:
                    raise DeepSeekCandidateError(
                        f"patch {field} must be non-empty for {code}"
                    )
                if value is not None and not isinstance(value, str):
                    raise DeepSeekCandidateError(
                        f"patch {field} must be string/null for {code}"
                    )
                allowed_values = (taxonomy.get("allowed_values") or {}).get(field)
                if (
                    value is not None
                    and allowed_values is not None
                    and value not in allowed_values
                ):
                    raise DeepSeekCandidateError(
                        f"invalid taxonomy value for {code}: {field}={value}"
                    )
            proposed = {**(item_by_code[code].get("current_record") or {}), **patch}
            level1, level2 = proposed.get("level1_group"), proposed.get("level2_group")
            pairs = taxonomy.get("level2_by_level1") or {}
            if (
                patch
                and ("level1_group" in patch or "level2_group" in patch)
                and level1
                and level2
                and pairs
            ):
                if level2 not in pairs.get(level1, []):
                    raise DeepSeekCandidateError(
                        f"invalid classification hierarchy for {code}: {level1}/{level2}"
                    )
            if (
                taxonomy.get("require_complete_classification_for_confirmation")
                and action in ("KEEP", "UPDATE", "ADD")
                and not decision["requires_human_review"]
                and confidence >= taxonomy.get("minimum_automatic_confidence", 0.75)
            ):
                missing = [
                    f
                    for f in ("region_market", "level1_group", "level2_group")
                    if not proposed.get(f)
                ]
                if missing:
                    raise DeepSeekCandidateError(
                        f"confirmed classification incomplete for {code}: {missing}"
                    )
            if patch.get("exposure_id") and not re.fullmatch(
                r"[A-Za-z0-9_]+", patch["exposure_id"]
            ):
                raise DeepSeekCandidateError(f"invalid exposure_id for {code}")
        for list_field in ("evidence", "uncertainty"):
            value = decision[list_field]
            if not isinstance(value, list) or not all(
                isinstance(entry, str) and entry.strip() for entry in value
            ):
                raise DeepSeekCandidateError(
                    f"{list_field} must be a list of non-empty strings for {code}"
                )
        if taxonomy and taxonomy.get("require_evidence_anchors"):
            evidence_text = "\n".join(decision["evidence"])
            for anchor in item_by_code[code].get("required_evidence_anchors", []):
                if anchor and anchor not in evidence_text:
                    raise DeepSeekCandidateError(
                        f"evidence for {code} must quote this actual input value: {anchor}; generic template evidence is insufficient"
                    )
        if not isinstance(decision["requires_human_review"], bool):
            raise DeepSeekCandidateError(
                f"requires_human_review must be boolean for {code}"
            )
        if (
            not isinstance(decision["decision_summary"], str)
            or not decision["decision_summary"].strip()
        ):
            raise DeepSeekCandidateError(
                f"decision_summary must be non-empty for {code}"
            )

        normalized.append(
            {
                **decision,
                "fund_code": code,
                "action": action,
                "confidence": float(confidence),
            }
        )

    if seen_codes != set(expected_codes):
        missing_codes = sorted(set(expected_codes) - seen_codes)
        raise DeepSeekCandidateError(
            f"model output omitted fund_code values: {missing_codes}"
        )
    order = {code: index for index, code in enumerate(expected_codes)}
    return sorted(normalized, key=lambda item: order[item["fund_code"]])


class DeepSeekCandidateClient:
    """使用 GLMS 的 Chat Completions JSON mode 完成候选分类确认。"""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        timeout_seconds: float = 90.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
        system_prompt: str = SYSTEM_PROMPT,
        prompt_version: str = PROMPT_VERSION,
        thinking_enabled: bool = False,
        reasoning_effort: str = "high",
        max_output_tokens: int = 8192,
    ) -> None:
        self.api_key = (api_key or "").strip() or get_glms_api_key()
        if not self.api_key:
            raise DeepSeekCandidateError(
                "GLMS_API_KEY is required for AI candidate confirmation"
            )
        config = resolve_candidate_llm_config(model=model, base_url=base_url)
        self.base_url = config.base_url
        self.model = config.model
        self.llm_config = config.audit()
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(1, max_retries)
        self.session = session
        self._thread_sessions = threading.local()
        self.system_prompt = system_prompt
        self.prompt_version = prompt_version
        self.prompt_sha256 = hashlib.sha256(system_prompt.encode("utf-8")).hexdigest()
        self.generation_settings = {
            "thinking_enabled": thinking_enabled,
            "reasoning_effort": reasoning_effort if thinking_enabled else None,
            "max_output_tokens": max_output_tokens,
        }

    def confirm_batch(
        self,
        *,
        items: list[dict[str, Any]],
        taxonomy: dict[str, Any],
    ) -> DeepSeekBatchResult:
        if not items:
            raise DeepSeekCandidateError("items must not be empty")
        _normalized_codes(items)
        user_payload = {
            "prompt_version": self.prompt_version,
            "taxonomy": taxonomy,
            "items": items,
        }
        request_body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {
                    "role": "user",
                    "content": "请根据以下 JSON 输入逐项确认，并只返回约定 JSON：\n"
                    + canonical_json(user_payload),
                },
            ],
            "response_format": {"type": "json_object"},
            "thinking": {
                "type": (
                    "enabled"
                    if self.generation_settings["thinking_enabled"]
                    else "disabled"
                )
            },
            "max_tokens": self.generation_settings["max_output_tokens"],
        }
        if self.generation_settings["thinking_enabled"]:
            request_body["reasoning_effort"] = self.generation_settings[
                "reasoning_effort"
            ]
        else:
            request_body["temperature"] = 0.0
        session = self.session
        if session is None:
            if not hasattr(self._thread_sessions, "session"):
                self._thread_sessions.session = requests.Session()
            session = self._thread_sessions.session
        response_json: dict[str, Any] | None = None
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            terminal_http_error = False
            try:
                input_hash = sha256_json(request_body)
                response = session.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=request_body,
                    timeout=self.timeout_seconds,
                    allow_redirects=False,
                )
                if response.status_code in (408, 429) or response.status_code >= 500:
                    raise DeepSeekCandidateError(
                        f"GLMS transient HTTP {response.status_code}"
                    )
                if response.status_code >= 300:
                    terminal_http_error = True
                    body_preview = response.text.replace(self.api_key, "[redacted]")[
                        :500
                    ].replace("\n", " ")
                    raise DeepSeekCandidateError(
                        f"GLMS HTTP {response.status_code}: {body_preview}"
                    )
                response_json = response.json()
                if not isinstance(response_json, dict):
                    raise DeepSeekCandidateError("GLMS returned an invalid response envelope")
                choices = response_json.get("choices") or []
                if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
                    raise DeepSeekCandidateError("GLMS returned invalid choices")
                message = choices[0].get("message")
                if not isinstance(message, dict):
                    raise DeepSeekCandidateError("GLMS returned an invalid message")
                content = message.get("content")
                if not isinstance(content, str) or not content.strip():
                    raise DeepSeekCandidateError("GLMS returned empty content")
                decoded = json.loads(content)
                try:
                    decisions = validate_decisions(
                        decoded,
                        input_items=items,
                        taxonomy=taxonomy,
                    )
                except DeepSeekCandidateError as validation_error:
                    # 把明确的合同错误反馈给下一次请求，避免 temperature=0 原样重试。
                    # 原始事实与字典不变；最终请求哈希包含实际修复提示。
                    enum_match = re.search(
                        r"invalid taxonomy value for [^:]+: ([A-Za-z_]+)=",
                        str(validation_error),
                    )
                    enum_hint = ""
                    if enum_match:
                        field = enum_match.group(1)
                        enum_hint = (
                            "该字段 "
                            + field
                            + " 的合法值仅为："
                            + canonical_json(taxonomy["allowed_values"][field])
                            + "。"
                        )
                    request_body["messages"].append(
                        {
                            "role": "user",
                            "content": "上次结果未通过本地结构校验："
                            + str(validation_error)
                            + "。"
                            + enum_hint
                            + "。请重新返回全部基金的完整 JSON。枚举只能取 allowed_values 中的原值；"
                            "无法确认的可选字段可为 null 并标记 requires_human_review=true，"
                            "新产品缺少必需分类证据可 EXCLUDE_NEW。不要新增字典值。",
                        }
                    )
                    raise
                usage = response_json.get("usage") or {}
                if not isinstance(usage, dict):
                    raise DeepSeekCandidateError("GLMS returned invalid usage metadata")
                return DeepSeekBatchResult(
                    decisions=decisions,
                    response_id=response_json.get("id"),
                    actual_model=str(response_json.get("model") or self.model),
                    system_fingerprint=response_json.get("system_fingerprint"),
                    prompt_tokens=usage.get("prompt_tokens"),
                    completion_tokens=usage.get("completion_tokens"),
                    total_tokens=usage.get("total_tokens"),
                    input_hash=input_hash,
                    output_hash=sha256_json(decoded),
                )
            except (
                DeepSeekCandidateError,
                json.JSONDecodeError,
                requests.RequestException,
                ValueError,
            ) as exc:
                last_error = exc
                if terminal_http_error or attempt == self.max_retries:
                    break
                time.sleep(min(2 ** (attempt - 1), 4))

        message = str(last_error) if last_error else "unknown GLMS error"
        message = message.replace(self.api_key, "[redacted]")
        raise DeepSeekCandidateError(
            f"GLMS candidate confirmation failed after "
            f"{attempt} attempts: {message}"
        ) from None


# 新入口使用服务无关名称；旧调用和历史测试仍可使用 DeepSeek 名称。
CandidateLLMClient = DeepSeekCandidateClient
