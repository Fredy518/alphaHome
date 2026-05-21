from __future__ import annotations

from pathlib import Path

import tinydata as td
from tinydata import config as cfg


def test_config_precedence_env_then_explicit(monkeypatch, tmp_path):
    td.reset_config()
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
    assert td.get_config().safe_dict()["password"] == "***"
    td.reset_config()
