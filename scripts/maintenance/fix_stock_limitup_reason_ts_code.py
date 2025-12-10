#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
修复 akshare.stock_limitup_reason 表中的 ts_code 字段

将6位数字代码转换为带后缀的标准格式（如 000001 -> 000001.SZ）
通过查询 tushare.stock_basic 表获取正确的代码映射。

使用方法：
python scripts/maintenance/fix_stock_limitup_reason_ts_code.py
python scripts/maintenance/fix_stock_limitup_reason_ts_code.py --dry_run  # 试运行，不实际更新
"""

import sys
import os
import argparse
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from research.tools.context import ResearchContext


class StockLimitupReasonTsCodeFixer:
    """股票涨停原因表 ts_code 修复器"""

    def __init__(self, context: ResearchContext):
        self.context = context
        self.code_mapping = {}

    def load_code_mapping(self) -> int:
        """
        从 tushare.stock_basic 表加载股票代码映射。

        Returns:
            加载的映射数量
        """
        print("📥 正在加载股票代码映射...")

        query = """
            SELECT ts_code
            FROM tushare.stock_basic
        """

        try:
            rows = self.context.db_manager.fetch_sync(query)

            if not rows:
                print("❌ 未能从 tushare.stock_basic 获取数据")
                return 0

            # 构建映射: 6位代码 -> 完整代码
            for row in rows:
                ts_code = row["ts_code"] if isinstance(row, dict) else row[0]
                if ts_code and "." in ts_code:
                    symbol = ts_code.split(".")[0]
                    self.code_mapping[symbol] = ts_code

            print(f"✅ 已加载 {len(self.code_mapping)} 条股票代码映射")
            return len(self.code_mapping)

        except Exception as e:
            print(f"❌ 加载股票代码映射失败: {e}")
            return 0

    def get_records_to_fix(self) -> list:
        """
        获取需要修复的记录（ts_code 不包含 '.' 的记录）。

        Returns:
            需要修复的记录列表
        """
        print("🔍 正在查询需要修复的记录...")

        query = """
            SELECT trade_date, ts_code
            FROM akshare.stock_limitup_reason
            WHERE ts_code NOT LIKE '%%.%%'
        """

        try:
            rows = self.context.db_manager.fetch_sync(query)

            if not rows:
                print("✅ 没有需要修复的记录")
                return []

            print(f"📊 找到 {len(rows)} 条需要修复的记录")
            return rows

        except Exception as e:
            print(f"❌ 查询需要修复的记录失败: {e}")
            return []

    def fix_records(self, records: list, dry_run: bool = False) -> dict:
        """
        修复记录中的 ts_code。

        Args:
            records: 需要修复的记录列表
            dry_run: 是否为试运行模式

        Returns:
            修复结果统计
        """
        if not records:
            return {
                "total": 0,
                "fixed": 0,
                "not_found": 0,
                "failed": 0,
            }

        total = len(records)
        fixed = 0
        not_found = 0
        failed = 0
        not_found_codes = set()

        print(f"🔧 开始修复 {total} 条记录...")

        update_query = """
            UPDATE akshare.stock_limitup_reason
            SET ts_code = %s
            WHERE trade_date = %s AND ts_code = %s
        """

        for i, row in enumerate(records, 1):
            if isinstance(row, dict):
                trade_date = row["trade_date"]
                old_ts_code = row["ts_code"]
            else:
                trade_date = row[0]
                old_ts_code = row[1]

            # 查找正确的代码
            new_ts_code = self.code_mapping.get(old_ts_code)

            if not new_ts_code:
                not_found += 1
                not_found_codes.add(old_ts_code)
                continue

            if dry_run:
                fixed += 1
                if i <= 10:  # 只显示前10条
                    print(f"  [试运行] {trade_date}: {old_ts_code} -> {new_ts_code}")
            else:
                try:
                    self.context.db_manager.execute_sync(
                        update_query, (new_ts_code, trade_date, old_ts_code)
                    )
                    fixed += 1
                except Exception as e:
                    failed += 1
                    if failed <= 5:  # 只显示前5个错误
                        print(f"  ❌ 更新失败 {trade_date}/{old_ts_code}: {e}")

            # 每1000条输出一次进度
            if i % 1000 == 0:
                print(f"  进度: {i}/{total} ({i*100//total}%)")

        # 输出未找到映射的代码
        if not_found_codes:
            print(f"\n⚠️ 以下 {len(not_found_codes)} 个代码在 stock_basic 中未找到映射:")
            for code in sorted(not_found_codes)[:20]:  # 只显示前20个
                print(f"  - {code}")
            if len(not_found_codes) > 20:
                print(f"  ... 还有 {len(not_found_codes) - 20} 个")

        return {
            "total": total,
            "fixed": fixed,
            "not_found": not_found,
            "failed": failed,
        }


def main():
    parser = argparse.ArgumentParser(
        description="修复 akshare.stock_limitup_reason 表中的 ts_code 字段"
    )
    parser.add_argument(
        "--dry_run", action="store_true", help="试运行模式，不实际更新数据库"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("🔧 股票涨停原因表 ts_code 修复工具")
    print("=" * 60)
    print(f"🕐 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.dry_run:
        print("⚠️  试运行模式: 不会实际更新数据库")

    print()

    # 初始化研究上下文
    try:
        context = ResearchContext()
        print("✅ 数据库连接初始化成功")
    except Exception as e:
        print(f"❌ 数据库连接初始化失败: {e}")
        sys.exit(1)

    # 创建修复器
    fixer = StockLimitupReasonTsCodeFixer(context)

    # 加载代码映射
    mapping_count = fixer.load_code_mapping()
    if mapping_count == 0:
        print("❌ 无法加载股票代码映射，退出")
        sys.exit(1)

    print()

    # 获取需要修复的记录
    records = fixer.get_records_to_fix()

    if not records:
        print("\n✅ 没有需要修复的记录，退出")
        sys.exit(0)

    print()

    # 执行修复
    result = fixer.fix_records(records, dry_run=args.dry_run)

    # 输出结果
    print()
    print("=" * 60)
    print("📊 修复结果统计")
    print("=" * 60)
    print(f"  总记录数:     {result['total']}")
    print(f"  成功修复:     {result['fixed']}")
    print(f"  未找到映射:   {result['not_found']}")
    print(f"  更新失败:     {result['failed']}")

    if result["total"] > 0:
        success_rate = result["fixed"] / result["total"] * 100
        print(f"  成功率:       {success_rate:.1f}%")

    print()
    print(f"🕐 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.dry_run:
        print("\n⚠️  这是试运行结果，实际运行请去掉 --dry_run 参数")


if __name__ == "__main__":
    main()
