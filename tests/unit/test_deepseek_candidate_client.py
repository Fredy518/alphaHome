import json

import pytest

from alphahome.curation.deepseek_candidate_client import (
    DeepSeekCandidateClient,
    DeepSeekCandidateError,
    validate_decisions,
)


def _decision(**overrides):
    value = {
        "fund_code": "510300.SH",
        "action": "KEEP",
        "include_in_candidate_pool": True,
        "confidence": 0.91,
        "classification_patch": {},
        "decision_summary": "现有分类与输入事实一致",
        "evidence": ["基金代码和跟踪指数代码与现有记录一致"],
        "uncertainty": [],
        "requires_human_review": False,
    }
    value.update(overrides)
    return value


class FakeResponse:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payload)


def test_validate_decisions_rejects_existing_candidate_add():
    item = {"fund_code": "510300.SH", "kind": "existing"}
    with pytest.raises(DeepSeekCandidateError, match="cannot use action ADD"):
        validate_decisions(
            {"decisions": [_decision(action="ADD")]},
            input_items=[item],
        )


def test_client_uses_json_mode_and_returns_audit_hashes():
    response_payload = {
        "id": "response-1",
        "model": "deepseek-flash",
        "system_fingerprint": "fp-1",
        "choices": [
            {
                "message": {
                    "content": json.dumps(
                        {"decisions": [_decision()]}, ensure_ascii=False
                    )
                }
            }
        ],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 30,
            "total_tokens": 130,
        },
    }
    session = FakeSession(response_payload)
    client = DeepSeekCandidateClient(
        api_key="unit-test-key",
        session=session,
        max_retries=1,
    )
    result = client.confirm_batch(
        items=[{"fund_code": "510300.SH", "kind": "existing"}],
        taxonomy={"allowed_patch_fields": []},
    )

    assert result.decisions[0]["action"] == "KEEP"
    assert result.total_tokens == 130
    assert len(result.input_hash) == 64
    assert len(result.output_hash) == 64
    _, request = session.calls[0]
    assert request["json"]["response_format"] == {"type": "json_object"}
    assert request["json"]["thinking"] == {"type": "disabled"}
    assert request["headers"]["Authorization"] == "Bearer unit-test-key"


def test_client_rejects_missing_decision_without_partial_result():
    session = FakeSession(
        {
            "id": "response-2",
            "model": "deepseek-flash",
            "choices": [{"message": {"content": '{"decisions": []}'}}],
        }
    )
    client = DeepSeekCandidateClient(
        api_key="unit-test-key",
        session=session,
        max_retries=1,
    )
    with pytest.raises(DeepSeekCandidateError, match="omitted fund_code"):
        client.confirm_batch(
            items=[{"fund_code": "510300.SH", "kind": "existing"}],
            taxonomy={},
        )
