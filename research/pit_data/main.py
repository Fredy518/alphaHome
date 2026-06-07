#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Research-side PIT coordinator compatibility entrypoint."""

import asyncio

from alphahome.pit.pit_data_update_production import PITDataUpdateCoordinator, main

PITDataCoordinator = PITDataUpdateCoordinator

__all__ = ["PITDataCoordinator", "PITDataUpdateCoordinator", "main"]

if __name__ == "__main__":
    asyncio.run(main())
