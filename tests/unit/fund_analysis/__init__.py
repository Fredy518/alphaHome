"""Test shim that forwards ``fund_analysis`` imports to the real package."""

from __future__ import annotations

from pathlib import Path


_ROOT_DIR = Path(__file__).resolve().parents[3]
_PKG_DIR = _ROOT_DIR / "fund_analysis"
_INIT_FILE = _PKG_DIR / "__init__.py"
_TEST_DIR = Path(__file__).resolve().parent

__file__ = str(_INIT_FILE)
__path__ = [str(_TEST_DIR), str(_PKG_DIR)]

code = compile(_INIT_FILE.read_text(encoding="utf-8"), str(_INIT_FILE), "exec")
exec(code, globals())
