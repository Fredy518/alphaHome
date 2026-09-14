#!/usr/bin/env python
"""Governed weekly P/G calculation followed by an independent audit."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from alphahome.factors.command import main as factor_main  # noqa: E402


def main() -> int:
    run_exit = factor_main(["run", "--tasks", "p", "g", "--mode", "smart"])
    if run_exit != 0:
        return run_exit
    return factor_main(["audit", "--tasks", "p", "g"])


if __name__ == "__main__":
    raise SystemExit(main())
