from __future__ import annotations

from pathlib import Path

import tinydata as td
from tinydata import config as cfg


def test_config_precedence_env_then_explicit(monkeypatch, tmp_path):
    td.reset_config()
    monkeypatch.delenv("TINYDATA_PASSWORD", raising=False)
    monkeypatch.delenv("TINYDATA_OPI_SESSION_KEY", raising=False)
    monkeypatch.delenv("TINYDATA_OPI_SESSION_PASSWORD", raising=False)
    monkeypatch.setattr(cfg, "_read_config_file", lambda path=cfg.DEFAULT_CONFIG_PATH: {"user": "file_user", "port": 1})
    monkeypatch.setenv("TINYDATA_USER", "env_user")
    monkeypatch.setenv("TINYDATA_PORT", "2")

    loaded = td.get_config()
    assert loaded.user == "env_user"
    assert loaded.port == 2

    explicit = td.configure(user="explicit_user", cache_dir=tmp_path)
    assert explicit.user == "explicit_user"
    assert explicit.cache_dir == Path(tmp_path)
    assert explicit.safe_dict()["password"] == ""

    td.reset_config()


def test_config_masks_password(monkeypatch):
    td.reset_config()
    monkeypatch.setenv("TINYDATA_PASSWORD", "secret")
    monkeypatch.setenv("TINYDATA_OPI_SESSION_KEY", "session-secret")
    monkeypatch.setenv("TINYDATA_OPI_SESSION_PASSWORD", "session-password")
    assert td.get_config().safe_dict()["password"] == "***"
    assert td.get_config().safe_dict()["session_key"] == "***"
    assert td.get_config().safe_dict()["session_password"] == "***"
    td.reset_config()


def test_opi_env_config(monkeypatch):
    td.reset_config()
    monkeypatch.setenv("TINYDATA_OPI_URL", "https://example.test")
    monkeypatch.setenv("TINYDATA_OPI_AUTH_MODE", "session-key")
    monkeypatch.setenv("TINYDATA_OPI_RUN_FUNC_NAME", "run_wrapper")

    loaded = td.get_config()
    assert loaded.opi_url == "https://example.test"
    assert loaded.opi_auth_mode == "session-key"
    assert loaded.run_func_name == "run_wrapper"
    td.reset_config()
