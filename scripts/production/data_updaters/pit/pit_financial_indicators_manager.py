#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility entrypoint for PIT financial indicators manager."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.pit.pit_financial_indicators_manager import PITFinancialIndicatorsManager, main

__all__ = ["PITFinancialIndicatorsManager", "main"]

if __name__ == "__main__":
    sys.exit(main())
