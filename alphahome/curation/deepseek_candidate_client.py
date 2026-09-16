"""DeepSeek ETF 候选确认客户端。

模型只返回结构化研究分类建议；本模块不提供资金或下单能力。
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests


DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL = "deepseek-flash"
PROMPT_VERSION = "etf_candidate_confirmation_v2"
ALLOWED_ACTIONS = {"KEEP", "UPDATE", "ADD", "EXCLUDE_NEW"}

SYSTEM_PROMPT = """你是 AlphaHome 的 ETF 候选池分类复核器。
你的任务仅限候选研究分类，不得生成买卖、仓位、资金分配或下单建议。
只能使用输入 JSON 中的产品事实、当前记录和分类参照；不能补造产品事实。
若证据不足，应提高 uncertainty、将 requires_human_review 设为 true，必要时对新产品给出 EXCLUDE_NEW。
现有产品只能返回 KEEP 或 UPDATE；新产品只能返回 ADD 或 EXCLUDE_NEW。
KEEP 的 classification_patch 必须是空对象。UPDATE 只返回确需变更的字段。
ADD 必须返回完整分类字段。include_in_candidate_pool 对 KEEP/UPDATE/ADD 为 true，
对 EXCLUDE_NEW 为 false。

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
    """DeepSeek 请求、JSON 解析或本地结构验证失败。"""


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
        for list_field in ("evidence", "uncertainty"):
            value = decision[list_field]
            if not isinstance(value, list) or not all(
                isinstance(entry, str) and entry.strip() for entry in value
            ):
                raise DeepSeekCandidateError(
                    f"{list_field} must be a list of non-empty strings for {code}"
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
    """使用 DeepSeek Chat Completions JSON mode 完成候选分类确认。"""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        timeout_seconds: float = 90.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise DeepSeekCandidateError(
                "DEEPSEEK_API_KEY is required for AI candidate confirmation"
            )
        self.base_url = (
            base_url or os.environ.get("DEEPSEEK_BASE_URL") or DEFAULT_BASE_URL
        ).rstrip("/")
        self.model = model or os.environ.get("DEEPSEEK_MODEL") or DEFAULT_MODEL
        self.timeout_seconds = timeout_seconds
        self.max_retries = max(1, max_retries)
        self.session = session or requests.Session()

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
            "prompt_version": PROMPT_VERSION,
            "taxonomy": taxonomy,
            "items": items,
        }
        request_body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": "请根据以下 JSON 输入逐项确认，并只返回约定 JSON：\n"
                    + canonical_json(user_payload),
                },
            ],
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
            "temperature": 0.0,
            "max_tokens": 8192,
        }
        input_hash = sha256_json(request_body)
        response_json: dict[str, Any] | None = None
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=request_body,
                    timeout=self.timeout_seconds,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    raise DeepSeekCandidateError(
                        f"DeepSeek transient HTTP {response.status_code}"
                    )
                if response.status_code >= 400:
                    body_preview = response.text[:500].replace("\n", " ")
                    raise DeepSeekCandidateError(
                        f"DeepSeek HTTP {response.status_code}: {body_preview}"
                    )
                response_json = response.json()
                choices = response_json.get("choices") or []
                content = (
                    choices[0].get("message", {}).get("content") if choices else None
                )
                if not isinstance(content, str) or not content.strip():
                    raise DeepSeekCandidateError("DeepSeek returned empty content")
                decoded = json.loads(content)
                decisions = validate_decisions(
                    decoded,
                    input_items=items,
                    taxonomy=taxonomy,
                )
                usage = response_json.get("usage") or {}
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
                if attempt == self.max_retries:
                    break
                time.sleep(min(2 ** (attempt - 1), 4))

        message = str(last_error) if last_error else "unknown DeepSeek error"
        raise DeepSeekCandidateError(
            f"DeepSeek candidate confirmation failed after "
            f"{self.max_retries} attempts: {message}"
        )
