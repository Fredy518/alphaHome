#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Compatibility entrypoint for PIT production updates."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from alphahome.pit.pit_data_update_production import PITDataUpdateCoordinator, main

__all__ = ["PITDataUpdateCoordinator", "main"]

if __name__ == "__main__":
    asyncio.run(main())
