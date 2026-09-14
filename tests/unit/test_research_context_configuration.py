import logging
import traceback
from unittest.mock import Mock
from urllib.parse import unquote, urlsplit

import pytest

from alphahome.common.config_manager import redact_sensitive_config, redact_url
from research.tools.context import ResearchContext


@pytest.fixture
def context(tmp_path, monkeypatch):
    for key in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(ResearchContext, "_load_alphahome_config", lambda self: None)
    return ResearchContext(tmp_path)


def test_missing_password_rejected_before_manager_creation(context, monkeypatch):
    factory = Mock()
    monkeypatch.setattr("alphahome.common.db_manager.create_sync_manager", factory)

    assert context._get_default_config()["db_manager"]["password"] is None
    with pytest.raises(ValueError, match="password is not configured"):
        _ = context.db_manager
    factory.assert_not_called()


def test_explicit_environment_password_reaches_factory_without_corrupting_spaces(
    context, monkeypatch
):
    secret = "test password/@+only"
    monkeypatch.setenv("DB_PASSWORD", secret)
    factory = Mock(return_value=object())
    monkeypatch.setattr("alphahome.common.db_manager.create_sync_manager", factory)

    assert context.db_manager is factory.return_value
    parsed = urlsplit(factory.call_args.args[0])
    assert unquote(parsed.password) == secret


def test_research_config_overrides_main_and_main_overrides_environment(
    context, monkeypatch
):
    monkeypatch.setenv("DB_PASSWORD", "test-environment-password")
    monkeypatch.setattr(
        context,
        "_load_alphahome_config",
        lambda: {"password": "test-main-password", "host": "main.invalid"},
    )
    assert context._get_merged_db_config()["password"] == "test-main-password"
    context.config_path.write_text("db_manager: {}", encoding="utf8")
    context.config = {"db_manager": {"password": "test-project-password", "host": ""}}

    merged = context._get_merged_db_config()

    assert merged["password"] == "test-project-password"
    assert merged["host"] == "main.invalid"


def test_driver_failure_does_not_expose_credentials_in_logs_or_traceback(
    context, monkeypatch, caplog
):
    secret = "test-driver-password"
    monkeypatch.setenv("DB_PASSWORD", secret)
    factory = Mock(side_effect=RuntimeError(f"password={secret}"))
    monkeypatch.setattr("alphahome.common.db_manager.create_sync_manager", factory)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(RuntimeError, match="Failed to create DBManager") as exc:
        _ = context.db_manager

    assert secret not in caplog.text
    assert secret not in "".join(traceback.format_exception(exc.value))
    assert context._db_manager is None


def test_invalid_yaml_does_not_log_credential_bearing_source(context, caplog):
    secret = "test-yaml-password"
    context.config_path.write_text(f"password: [{secret}", encoding="utf8")

    fallback = context._load_config()

    assert fallback["db_manager"]["password"] is None
    assert secret not in caplog.text


@pytest.mark.parametrize(
    "url, expected",
    [
        (
            "postgresql://test-user:test-pass@localhost:5432/test?password=query#secret",
            "postgresql://localhost:5432/test",
        ),
        ("https://test-token@example.invalid/data?token=test-query", "https://example.invalid/data"),
        ("postgresql://test-user:test-pass@[::1]:5432/test", "postgresql://[::1]:5432/test"),
        ("invalid-url", "***REDACTED***"),
    ],
)
def test_diagnostic_urls_exclude_all_userinfo_query_and_fragment(url, expected):
    assert redact_url(url) == expected
    assert redact_sensitive_config({"connection_string": url, "nested": [url]}) == {
        "connection_string": expected if "://" in url else url,
        "nested": [expected if "://" in url else url],
    }


def test_research_config_logging_masks_nested_secrets(context):
    assert context._mask_sensitive_config({"api": {"token": "test-token"}}) == {
        "api": {"token": "***REDACTED***"}
    }
