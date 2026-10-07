from datetime import date
from types import SimpleNamespace

from alphahome.curation import candidate_llm_config
from alphahome.curation import etf_candidate_monthly_maintenance as service


class FakeConnection:
    def __init__(self):
        self.rollback_count = 0
        self.closed = False

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


def test_plan_before_monthly_window_does_not_connect(monkeypatch):
    monkeypatch.setattr(
        service.psycopg2,
        "connect",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    result = service.build_candidate_monthly_maintenance_plan(
        "postgresql://unused",
        run_date=date(2026, 10, 4),
    )

    assert result["status"] == "deferred_before_monthly_window"
    assert result["executable_now"] is False


def test_plan_stops_before_rebuild_when_month_already_succeeded(monkeypatch):
    connection = FakeConnection()
    monkeypatch.setattr(service.psycopg2, "connect", lambda *_args: connection)
    monkeypatch.setattr(
        service,
        "get_existing_month_result",
        lambda *_args: {
            "status": "skipped_already_succeeded",
            "ai_run_id": "etf_ai_202610_done",
            "model_requested": "deepseek-flash",
            "llm_config": None,
        },
    )
    monkeypatch.setattr(
        service,
        "build_candidate_automation_plan",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("must not rebuild")
        ),
    )

    result = service.build_candidate_monthly_maintenance_plan(
        "postgresql://unit-test",
        run_date=date(2026, 10, 15),
    )

    assert result["status"] == "skipped_already_succeeded"
    assert result["ai_run_id"] == "etf_ai_202610_done"
    assert result["model_requested"] == "deepseek-flash"
    assert result["llm_config"] is None
    assert connection.closed is True


def test_plan_blocks_only_when_llm_targets_need_missing_key(monkeypatch):
    connection = FakeConnection()
    plan = SimpleNamespace(
        target_items=[{"fund_code": "510300.SH"}],
        executable=True,
        summary=lambda: {
            "status": "planned",
            "plan_hash": "a" * 64,
            "llm_target_count": 1,
            "new_product_count": 0,
            "guards": {},
        },
    )
    monkeypatch.setattr(service, "get_glms_api_key", lambda: None)
    monkeypatch.setattr(service.psycopg2, "connect", lambda *_args: connection)
    monkeypatch.setattr(service, "get_existing_month_result", lambda *_args: None)
    monkeypatch.setattr(
        service,
        "build_candidate_automation_plan",
        lambda *_args, **_kwargs: plan,
    )

    result = service.build_candidate_monthly_maintenance_plan(
        "postgresql://unit-test",
        run_date=date(2026, 10, 15),
    )

    assert result["status"] == "blocked_missing_api_key"
    assert result["api_key_available"] is False
    assert result["executable_now"] is False


def test_execute_without_llm_targets_uses_frozen_internal_plan(monkeypatch):
    connection = FakeConnection()
    plan = SimpleNamespace(
        plan_hash="b" * 64,
        target_items=[],
        executable=True,
        plan_payload={
            "current_candidate_count": 143,
            "new_product_count": 0,
            "llm_target_count": 0,
        },
    )
    captured = {}
    monkeypatch.setattr(service.psycopg2, "connect", lambda *_args: connection)
    monkeypatch.setattr(service, "get_existing_month_result", lambda *_args: None)
    monkeypatch.setattr(
        service,
        "build_candidate_automation_plan",
        lambda *_args, **_kwargs: plan,
    )

    def execute(_connection, _plan, **kwargs):
        captured.update(kwargs)
        return {"status": "succeeded", "output_snapshot_id": "snapshot"}

    monkeypatch.setattr(service, "execute_candidate_automation", execute)

    result = service.execute_candidate_monthly_maintenance(
        "postgresql://unit-test",
        run_date=date(2026, 10, 15),
    )

    assert result["status"] == "succeeded"
    assert result["llm_target_count"] == 0
    assert captured["client"] is None
    assert captured["expected_plan_hash"] == "b" * 64
    assert result["capital_authority"] is False
    assert result["order_authority"] is False


def test_plan_accepts_windows_user_token_and_uses_glms_settings(monkeypatch):
    connection = FakeConnection()
    captured = {}
    plan = SimpleNamespace(
        target_items=[{"fund_code": "510300.SH"}],
        executable=True,
        summary=lambda: {"plan_hash": "c" * 64, "llm_target_count": 1},
    )
    monkeypatch.delenv("GLMS_API_KEY", raising=False)
    monkeypatch.delenv("GLMS_MODEL", raising=False)
    monkeypatch.delenv("GLMS_BASE_URL", raising=False)
    monkeypatch.setattr(
        candidate_llm_config,
        "_read_windows_user_variable",
        {
            "GLMS_API_KEY": "user-company-token",
            "GLMS_MODEL": "company-model",
            "GLMS_BASE_URL": "https://models.glms.com.cn/",
        }.get,
    )
    monkeypatch.setattr(service.psycopg2, "connect", lambda *_: connection)
    monkeypatch.setattr(service, "get_existing_month_result", lambda *_: None)

    def build(_connection, **kwargs):
        captured.update(kwargs)
        return plan

    monkeypatch.setattr(service, "build_candidate_automation_plan", build)
    result = service.build_candidate_monthly_maintenance_plan(
        "postgresql://unit-test", run_date=date(2026, 10, 15)
    )
    assert result["status"] == "ready"
    assert result["api_key_available"] is True
    assert result["llm_config"]["provider"] == "glms"
    assert captured["model_requested"] == "company-model"
    assert captured["llm_base_url"] == "https://models.glms.com.cn/ucloud/v1"
    assert "user-company-token" not in str(result)
