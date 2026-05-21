from __future__ import annotations

from pathlib import Path


def test_runtime_source_does_not_import_alphahome():
    src = Path(__file__).resolve().parents[1] / "src" / "tinydata"
    offenders = []
    for path in src.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "import alphahome" in text or "from alphahome" in text:
            offenders.append(path.name)
    assert offenders == []
