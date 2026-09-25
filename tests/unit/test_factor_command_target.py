"""The explicit maintenance target must win over a workstation's saved URL."""

from alphahome.factors import command


def test_factor_schema_apply_uses_explicit_database_target(monkeypatch):
    target = "postgresql://test_owner@127.0.0.1:55439/alphahome_test_fixture"
    calls = []

    class FakeDB:
        def __init__(self, url, mode):
            calls.append(("connect", url, mode))

        def close_sync(self):
            calls.append(("close",))

    class FakeStore:
        def __init__(self, db):
            assert isinstance(db, FakeDB)

        def ensure_schema(self):
            calls.append(("apply",))

        def schema_issues(self):
            return []

    monkeypatch.setenv("ALPHAHOME_DATABASE_URL", target)
    monkeypatch.setattr(
        command.ConfigManager,
        "get_database_url",
        lambda self: "postgresql://local_owner@localhost:5432/alphadb",
    )
    monkeypatch.setattr(command, "DBManager", FakeDB)
    monkeypatch.setattr(command, "FactorGovernanceStore", FakeStore)

    assert command.main(["schema", "--apply"]) == 0
    assert calls == [("connect", target, "sync"), ("apply",), ("close",)]
