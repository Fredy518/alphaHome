#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility entrypoint for PIT income manager."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.pit.pit_income_quarterly_manager import PITIncomeQuarterlyManager, main

__all__ = ["PITIncomeQuarterlyManager", "main"]

if __name__ == "__main__":
    sys.exit(main())
