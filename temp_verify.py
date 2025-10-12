#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, '.')

from alphahome.fetchers.tasks.index.tushare_index_dailybasic import TushareIndexDailyBasicTask

print('任务导入成功')
print('任务名称:', TushareIndexDailyBasicTask.name)
print('索引数量:', len(TushareIndexDailyBasicTask.indexes))
for i, idx in enumerate(TushareIndexDailyBasicTask.indexes):
    print(f'索引{i+1}: {idx["name"]} - {idx["columns"]}')
