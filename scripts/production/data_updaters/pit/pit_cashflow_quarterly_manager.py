#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility entrypoint for PIT cashflow quarterly manager."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.pit.pit_cashflow_quarterly_manager import PITCashflowQuarterlyManager, main

__all__ = ["PITCashflowQuarterlyManager", "main"]

if __name__ == "__main__":
    sys.exit(main())
