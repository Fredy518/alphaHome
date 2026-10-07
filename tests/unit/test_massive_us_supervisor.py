from datetime import datetime, timezone

import pytest

from scripts.production import supervise_massive_us_backfill as supervisor
from scripts.production.supervise_massive_us_backfill import retryable


START = datetime(2026, 9, 28, 15, 40, tzinfo=timezone.utc)


def state(error):
    return {
        "status": "failed",
        "plan_hash": "frozen",
        "updated_at": "2026-09-28T15:41:00+00:00",
        "error": error,
    }


@pytest.mark.parametrize(
    "error",
    [
        "Massive 网络请求失败，重试次数已耗尽",
        "Massive HTTP 429，重试次数已耗尽",
        "Massive HTTP 503，重试次数已耗尽",
    ],
)
def test_retries_only_transient_vendor_failures(error):
    assert retryable(state(error), "frozen", START)


@pytest.mark.parametrize(
    "error",
    [
        "Massive HTTP 401",
        "Massive HTTP 403",
        "Massive HTTP 404",
        "日线 OHLC 价格关系异常",
        "final_audit_failed",
        "source_changed",
        "起始日超出配置的历史权限窗口",
    ],
)
def test_auth_validation_and_plan_failures_stop(error):
    assert not retryable(state(error), "frozen", START)


def test_stale_status_does_not_trigger_retry_of_new_preflight_failure():
    old = state("Massive 网络请求失败")
    old["updated_at"] = "2026-09-28T13:17:00+00:00"
    assert not retryable(old, "frozen", START)
    assert not retryable(state("Massive HTTP 503"), "changed", START)
    assert not retryable({}, "frozen", START)


@pytest.mark.parametrize(
    "errors,expected_code,expected_attempts",
    [
        (["Massive 网络请求失败", None], 0, 2),
        (["Massive HTTP 403"], 1, 1),
        (["Massive HTTP 503"] * 3, 1, 3),
    ],
)
def test_supervisor_resumes_transients_but_stops_at_failure_budget(
    tmp_path, monkeypatch, errors, expected_code, expected_attempts
):
    import json

    monkeypatch.setattr(supervisor, "DELAYS", (0, 0))
    calls = []

    class Child:
        pid = 123

        def __init__(self, command, **kwargs):
            calls.append(command)
            self.error = errors[len(calls) - 1]
            snapshot = {
                "status": "failed" if self.error else "completed",
                "plan_hash": "frozen",
                "updated_at": supervisor.now(),
                "error": self.error,
                "completed": {"daily": 40, "basic": 2},
            }
            (tmp_path / "status.json").write_text(
                json.dumps(snapshot), encoding="utf-8"
            )

        def wait(self, timeout):
            return 1 if self.error else 0

    # Prevent the initial baseline of 40 dates from being interpreted as progress.
    (tmp_path / "status.json").write_text(
        json.dumps({"completed": {"daily": 40, "basic": 2}}), encoding="utf-8"
    )
    monkeypatch.setattr(supervisor.subprocess, "Popen", Child)
    assert supervisor.supervise(tmp_path, "frozen") == expected_code
    assert len(calls) == expected_attempts
    result = json.loads((tmp_path / "supervisor.json").read_text(encoding="utf-8"))
    assert result["status"] == ("completed" if expected_code == 0 else "failed")
    assert all(command[-1] == "frozen" for command in calls)
