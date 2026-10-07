import sys
from types import SimpleNamespace

import pytest

from alphahome.curation import candidate_llm_config as config
from alphahome.curation.candidate_llm_config import _read_windows_user_variable


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    for name in (
        "GLMS_API_KEY",
        "GLMS_BASE_URL",
        "GLMS_MODEL",
        "DEEPSEEK_API_KEY",
        "DEEPSEEK_BASE_URL",
        "DEEPSEEK_MODEL",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config, "_read_windows_user_variable", lambda _: None)


def test_process_token_has_priority_and_is_trimmed(monkeypatch):
    monkeypatch.setenv("GLMS_API_KEY", " process-token ")
    monkeypatch.setattr(config, "_read_windows_user_variable", lambda _: "user-token")
    assert config.get_glms_api_key() == "process-token"


def test_token_is_read_lazily_from_user_environment(monkeypatch):
    values = {}
    monkeypatch.setattr(config, "_read_windows_user_variable", values.get)
    assert config.get_glms_api_key() is None
    values["GLMS_API_KEY"] = "user-token"
    assert config.get_glms_api_key() == "user-token"


def test_windows_reader_uses_current_user_environment(monkeypatch):
    calls = []

    class Registry:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    registry = Registry()
    hive = object()

    def open_key(actual_hive, path):
        calls.append((actual_hive, path))
        return registry

    monkeypatch.setitem(
        sys.modules,
        "winreg",
        SimpleNamespace(
            HKEY_CURRENT_USER=hive,
            OpenKey=open_key,
            QueryValueEx=lambda actual_registry, name: (" user-token ", 1),
        ),
    )
    assert _read_windows_user_variable("GLMS_API_KEY") == "user-token"
    assert calls == [(hive, "Environment")]


def test_legacy_deepseek_settings_do_not_supply_company_credentials(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "old-token")
    monkeypatch.setenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
    monkeypatch.setenv("DEEPSEEK_MODEL", "old-model")
    assert config.get_glms_api_key() is None
    resolved = config.resolve_candidate_llm_config()
    assert resolved.base_url == "https://models.glms.com.cn/ucloud/v1"
    assert resolved.model == "deepseek-v4-flash"
    assert resolved.provider == "glms"


def test_glms_overrides_and_cli_priority_are_shared(monkeypatch):
    monkeypatch.setattr(
        config,
        "_read_windows_user_variable",
        {
            "GLMS_API_KEY": "private-token",
            "GLMS_MODEL": "user-model",
            "GLMS_BASE_URL": "https://models.glms.com.cn/",
        }.get,
    )
    resolved = config.resolve_candidate_llm_config()
    assert resolved.model == "user-model"
    assert resolved.base_url == config.DEFAULT_BASE_URL
    assert "private-token" not in str(resolved.audit())
    assert config.resolve_candidate_llm_config(model="cli-model").model == "cli-model"
    monkeypatch.setenv("GLMS_MODEL", "process-model")
    assert config.resolve_candidate_llm_config().model == "process-model"


def test_review_default_uses_pro_and_environment_can_override(monkeypatch):
    assert (
        config.resolve_candidate_llm_config(
            default_model=config.DEFAULT_REVIEW_MODEL
        ).model
        == "deepseek-v4-pro"
    )
    monkeypatch.setenv("GLMS_MODEL", "override-model")
    assert (
        config.resolve_candidate_llm_config(
            default_model=config.DEFAULT_REVIEW_MODEL
        ).model
        == "override-model"
    )


@pytest.mark.parametrize(
    "base_url",
    [
        "models.glms.com.cn",
        "http://models.glms.com.cn/ucloud/v1",
        "https://token@models.glms.com.cn/ucloud/v1",
        "https://models.glms.com.cn/ucloud/v1?api_key=secret",
        "https://models.glms.com.cn/ucloud/v1#secret",
    ],
)
def test_base_url_cannot_contain_credentials_or_use_plaintext(base_url):
    with pytest.raises(ValueError, match="HTTPS API base URL"):
        config.resolve_candidate_llm_config(base_url=base_url)
