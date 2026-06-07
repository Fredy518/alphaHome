#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility entrypoint for PIT balance manager."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.pit.pit_balance_quarterly_manager import PITBalanceQuarterlyManager, main

__all__ = ["PITBalanceQuarterlyManager", "main"]

if __name__ == "__main__":
    sys.exit(main())
