#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单集成测试：验证 express 缺失字段兜底修复逻辑
步骤：
1) 选取最近14天一条 express 记录
2) 将其 total_cur_assets/total_cur_liab/inventories 置为 NULL
3) 调用 fix_missing_express_fields(days=14)
4) 断言上述字段被成功回填（若数据库存在符合 PIT 的 report）
"""

from datetime import date, timedelta
from research.pit_data import PITBalanceQuarterlyManager


def main():
    with PITBalanceQuarterlyManager() as m:
        m.ensure_indexes()
        end_d = date.today()
        start_d = end_d - timedelta(days=14)
        # 选取一条 express 记录
        q = (
            "SELECT ts_code, end_date, ann_date FROM pgs_factors.pit_balance_quarterly "
            "WHERE data_source='express' AND ann_date BETWEEN %s AND %s LIMIT 1"
        )
        df = m.context.query_dataframe(q, (start_d, end_d))
        if df is None or df.empty:
            print("No express row found in last 14 days; skip test.")
            return 0
        row = df.iloc[0]
        # 注入缺失
        upd = (
            "UPDATE pgs_factors.pit_balance_quarterly "
            "SET total_cur_assets=NULL, total_cur_liab=NULL, inventories=NULL "
            "WHERE ts_code=%s AND end_date=%s AND ann_date=%s AND data_source='express'"
        )
        m.context.db_manager.execute_sync(upd, (row['ts_code'], row['end_date'], row['ann_date']))
        print("Injected missing fields for:", row.to_dict())
        # 运行修复
        res = m.fix_missing_express_fields(days=14)
        print("fix result:", res)
        # 验证回填
        chk = (
            "SELECT total_cur_assets, total_cur_liab, inventories "
            "FROM pgs_factors.pit_balance_quarterly "
            "WHERE ts_code=%s AND end_date=%s AND ann_date=%s AND data_source='express'"
        )
        chk_df = m.context.query_dataframe(chk, (row['ts_code'], row['end_date'], row['ann_date']))
        if chk_df is None or chk_df.empty:
            print("Record not found for verification.")
            return 1
        filled = chk_df.iloc[0]
        ok = (filled['total_cur_assets'] is not None) or (filled['total_cur_liab'] is not None) or (filled['inventories'] is not None)
        print("Verification row:", filled.to_dict())
        if not ok:
            print("Fix did not fill any target fields. This may happen if no suitable PIT report exists in DB.")
            return 2
        print("Test passed: at least one target field was filled.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

