import json

import pytest

from alphahome.curation import candidate_llm_config
from alphahome.curation.deepseek_candidate_client import (
    DeepSeekCandidateClient,
    DeepSeekCandidateError,
    validate_decisions,
)


@pytest.fixture(autouse=True)
def isolated_glms_environment(monkeypatch):
    for name in ("GLMS_API_KEY", "GLMS_MODEL", "GLMS_BASE_URL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        candidate_llm_config, "_read_windows_user_variable", lambda _: None
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


@pytest.mark.parametrize("payload", [None, [], {"choices": "bad"}, {"choices": [None]},
                                    {"choices": [{"message": None}]},
                                    {"choices": [{"message": "bad"}]}])
def test_client_reports_malformed_envelope_as_sanitized_domain_error(payload):
    client = DeepSeekCandidateClient(api_key="unit-test-key", session=FakeSession(payload), max_retries=1)
    with pytest.raises(DeepSeekCandidateError, match="GLMS candidate confirmation failed") as error:
        client.confirm_batch(items=[{"fund_code": "510300.SH", "kind": "existing"}], taxonomy={"allowed_patch_fields": []})
    assert "unit-test-key" not in str(error.value)


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
    url, request = session.calls[0]
    assert url == "https://models.glms.com.cn/ucloud/v1/chat/completions"
    assert request["json"]["model"] == "deepseek-v4-flash"
    assert client.llm_config == {
        "provider": "glms",
        "base_url": "https://models.glms.com.cn/ucloud/v1",
        "model": "deepseek-v4-flash",
    }
    assert request["json"]["response_format"] == {"type": "json_object"}
    assert request["json"]["thinking"] == {"type": "disabled"}
    assert request["headers"]["Authorization"] == "Bearer unit-test-key"
    assert request["allow_redirects"] is False


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


def test_model_cannot_invent_taxonomy_enum_or_nonstring_patch():
    taxonomy = {
        "allowed_patch_fields": ["candidate_status"],
        "allowed_values": {"candidate_status": ["观察"]},
    }
    for invalid_value in ("直接买入", 123):
        with pytest.raises(DeepSeekCandidateError):
            validate_decisions(
                {
                    "decisions": [
                        _decision(
                            action="UPDATE",
                            classification_patch={"candidate_status": invalid_value},
                        )
                    ]
                },
                input_items=[{"fund_code": "510300.SH", "kind": "existing"}],
                taxonomy=taxonomy,
            )


def test_retry_returns_validation_error_to_model_and_hashes_actual_request(monkeypatch):
    from copy import deepcopy
    from alphahome.curation.deepseek_candidate_client import sha256_json

    monkeypatch.setattr(
        "alphahome.curation.deepseek_candidate_client.time.sleep", lambda _: None
    )
    calls = []

    class RepairSession:
        def post(self, url, **kwargs):
            calls.append(deepcopy(kwargs["json"]))
            patch = {"candidate_status": "错误枚举" if len(calls) == 1 else "观察"}
            return FakeResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "decisions": [
                                            _decision(
                                                action="UPDATE",
                                                classification_patch=patch,
                                            )
                                        ]
                                    },
                                    ensure_ascii=False,
                                )
                            }
                        }
                    ]
                }
            )

    result = DeepSeekCandidateClient(
        api_key="test", session=RepairSession(), max_retries=2
    ).confirm_batch(
        items=[{"fund_code": "510300.SH", "kind": "existing"}],
        taxonomy={
            "allowed_patch_fields": ["candidate_status"],
            "allowed_values": {"candidate_status": ["观察"]},
        },
    )
    assert len(calls) == 2
    assert "错误枚举" in calls[1]["messages"][-1]["content"]
    assert result.input_hash == sha256_json(calls[1])


def test_second_review_rejects_generic_evidence_without_actual_source_values():
    item = {
        "fund_code": "510300.SH",
        "kind": "existing",
        "required_evidence_anchors": ["510300.SH", "000300.SH", "沪深300指数"],
    }
    taxonomy = {"require_evidence_anchors": True}
    with pytest.raises(DeepSeekCandidateError, match="510300.SH"):
        validate_decisions(
            {"decisions": [_decision()]}, input_items=[item], taxonomy=taxonomy
        )
    result = validate_decisions(
        {
            "decisions": [
                _decision(
                    evidence=[
                        "基金名录510300.SH跟踪000300.SH",
                        "指数名录全名为沪深300指数",
                    ]
                )
            ]
        },
        input_items=[item],
        taxonomy=taxonomy,
    )
    assert result[0]["requires_human_review"] is False


def test_thinking_settings_are_sent_and_temperature_is_omitted():
    session = FakeSession(
        {
            "model": "deepseek-v4-pro",
            "choices": [
                {"message": {"content": json.dumps({"decisions": [_decision()]})}}
            ],
        }
    )
    DeepSeekCandidateClient(
        api_key="unit-test-key",
        session=session,
        thinking_enabled=True,
        reasoning_effort="high",
        max_output_tokens=24576,
    ).confirm_batch(items=[{"fund_code": "510300.SH", "kind": "existing"}], taxonomy={})
    request = session.calls[0][1]["json"]
    assert request["thinking"] == {"type": "enabled"}
    assert request["reasoning_effort"] == "high"
    assert request["max_tokens"] == 24576
    assert "temperature" not in request


def test_missing_company_key_does_not_fall_back_to_deepseek(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "old-provider-key")
    with pytest.raises(DeepSeekCandidateError, match="GLMS_API_KEY"):
        DeepSeekCandidateClient()


@pytest.mark.parametrize("status", [301, 400, 401, 403])
def test_terminal_http_errors_redact_token_and_do_not_retry(status):
    key = "company-private-token"
    calls = []

    class ErrorSession:
        def post(self, *_args, **_kwargs):
            calls.append(1)
            response = FakeResponse({})
            response.status_code = status
            response.text = "request echoed Authorization: Bearer " + key
            return response

    with pytest.raises(DeepSeekCandidateError) as error:
        DeepSeekCandidateClient(api_key=key, session=ErrorSession()).confirm_batch(
            items=[{"fund_code": "510300.SH", "kind": "existing"}], taxonomy={}
        )
    assert key not in str(error.value)
    assert "[redacted]" in str(error.value)
    assert f"GLMS HTTP {status}" in str(error.value)
    assert len(calls) == 1
