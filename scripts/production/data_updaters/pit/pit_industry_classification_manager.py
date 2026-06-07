#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility entrypoint for PIT industry classification manager."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.pit.pit_industry_classification_manager import PITIndustryClassificationManager, main

__all__ = ["PITIndustryClassificationManager", "main"]

if __name__ == "__main__":
    sys.exit(main())
