import json
from unittest.mock import AsyncMock

import pytest

from alphahome.gui.services import configuration_service


@pytest.mark.asyncio
async def test_handle_save_settings_deep_merges_existing_config(tmp_path, monkeypatch):
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "database": {
                    "url": "postgresql://old:old@localhost:5432/alphadb",
                    "pool_config": {"max_size": 9},
                },
                "api": {
                    "tushare_token": "old-token",
                    "tinysoft": {"host": "tsl.example.com"},
                },
                "tasks": {"tushare_stock_daily": {"concurrent_limit": 3}},
            }
        ),
        encoding="utf-8",
    )
    reload_config = AsyncMock()
    monkeypatch.setattr(configuration_service, "CONFIG_FILE_PATH", str(config_path))
    monkeypatch.setattr(configuration_service, "CONFIG_DIR", str(tmp_path))
    monkeypatch.setattr(
        configuration_service.UnifiedTaskFactory,
        "reload_config",
        reload_config,
    )

    await configuration_service.handle_save_settings(
        {"database": {"url": ""}, "api": {"tushare_token": "new-token"}}
    )

    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert saved["database"]["url"] == "postgresql://old:old@localhost:5432/alphadb"
    assert saved["database"]["pool_config"] == {"max_size": 9}
    assert saved["api"]["tushare_token"] == "new-token"
    assert saved["api"]["tinysoft"] == {"host": "tsl.example.com"}
    assert saved["tasks"] == {"tushare_stock_daily": {"concurrent_limit": 3}}
    reload_config.assert_awaited_once()
