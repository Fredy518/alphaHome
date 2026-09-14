import re
from pathlib import Path

import pytest

from research.pgs_factor.database import PGSFactorDBManager
from scripts.production.factor_calculators import run_factor_weekly


ROOT = Path(__file__).resolve().parents[2]
PGS_MUTATION = re.compile(
    r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|CREATE\s+TABLE|DROP\s+TABLE|"
    r"ALTER\s+TABLE)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?pgs_factors\.",
    re.IGNORECASE | re.DOTALL,
)


def test_legacy_pgs_writer_fails_closed():
    with pytest.raises(RuntimeError, match="FactorCoordinator"):
        PGSFactorDBManager(object())


def test_active_factor_code_has_no_pgs_mutation_sql():
    roots = [
        ROOT / "alphahome" / "factors",
        ROOT / "scripts" / "production" / "factor_calculators",
        ROOT / "research" / "pgs_factor" / "database",
    ]
    violations = []
    for root in roots:
        for path in root.rglob("*"):
            if path.suffix.lower() not in {".py", ".sql"}:
                continue
            if "archive_disabled" in path.parts:
                continue
            if PGS_MUTATION.search(path.read_text(encoding="utf-8")):
                violations.append(str(path.relative_to(ROOT)))
    assert violations == []


def test_destructive_legacy_sql_is_only_in_disabled_archive():
    database_dir = ROOT / "research" / "pgs_factor" / "database"
    active_sql = list(database_dir.glob("*.sql"))
    assert active_sql == []
    archived_sql = list((database_dir / "archive_disabled").glob("*.sql"))
    assert archived_sql
    for path in archived_sql:
        assert "ARCHIVED / DISABLED" in path.read_text(encoding="utf-8")[:300]


def test_weekly_runner_stops_before_audit_when_run_fails(monkeypatch):
    calls = []

    def fake_main(arguments):
        calls.append(arguments)
        return 2

    monkeypatch.setattr(run_factor_weekly, "factor_main", fake_main)
    assert run_factor_weekly.main() == 2
    assert calls == [["run", "--tasks", "p", "g", "--mode", "smart"]]


def test_weekly_runner_audits_after_success(monkeypatch):
    calls = []

    def fake_main(arguments):
        calls.append(arguments)
        return 0

    monkeypatch.setattr(run_factor_weekly, "factor_main", fake_main)
    assert run_factor_weekly.main() == 0
    assert calls == [
        ["run", "--tasks", "p", "g", "--mode", "smart"],
        ["audit", "--tasks", "p", "g"],
    ]
