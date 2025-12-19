#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P因子年度并行计算启动器 - 薄包装版本

此脚本现在作为薄包装，调用 alphahome.production.factors 包内的核心逻辑。
原始的复杂逻辑已提取到 alphahome/production/factors/p_factor.py 中。

使用方法：
python scripts/production/factor_calculators/p_factor/start_parallel_p_factor_calculation.py --start_year 2020 --end_year 2024 --workers 10

或者通过统一CLI：
ah prod run p-factor -- --start_year 2020 --end_year 2024 --workers 10
"""

import argparse
import sys
import os

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.insert(0, project_root)

from alphahome.production.factors.p_factor import main


if __name__ == "__main__":
    # 直接调用包内模块的 main 函数
    sys.exit(main())
