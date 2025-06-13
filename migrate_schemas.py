#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Schema迁移便捷执行脚本

使用方法:
python migrate_schemas.py                # 交互式执行
python migrate_schemas.py --force        # 强制执行，跳过确认
python migrate_schemas.py --db-url "postgresql://user:pass@host:port/db"  # 指定数据库URL
"""

import sys
import os

# 添加项目路径到系统路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from alphahome.common.migrate_schemas import main

if __name__ == "__main__":
    main() 