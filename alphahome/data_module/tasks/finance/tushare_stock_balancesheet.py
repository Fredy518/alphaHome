# 1.核心属性
name = "tushare_stock_balancesheet"
description = "获取上市公司资产负债表数据"
table_name = "tushare_stock_balancesheet"
primary_keys = ["ts_code", "ann_date", "f_ann_date", "end_date", "report_type"]
date_column = "end_date" # 通常使用报告期结束日期
default_start_date = "19900101"

# --- 代码级默认配置 (会被 config.json 覆盖) --- #
default_concurrent_limit = 5
default_page_size = 7000

# 2.Tushare特有属性
api_name = "balancesheet_vip" # Tushare API 名称 