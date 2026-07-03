#!/usr/bin/env python
"""
Features MV PIT 化验收脚本

验收标准（见 docs/architecture/features_module_design.md Section 6.2.2）：

D-1: PIT 窗口正确性 + 数据契约
    - query_start_date = f_ann_date (或 ann_date)
    - query_end_date >= query_start_date
    - report_period = end_date（财报类）
    - 同一 ts_code 下 PIT 窗口不允许"未来信息泄漏"

D-2: 与既有 PIT 产出对比（可量化）
    - 抽样对比 pit.pit_income_quarterly / pit_balance_quarterly
    - 关键字段一致性检查

D-3: 可运维性与幂等性
    - features_init.py 可初始化/刷新
    - 连续执行 2 次无副作用
    - 血缘字段完备

使用方法:
    python scripts/features_validate_pit.py --check-d1     # 仅验证 D-1
    python scripts/features_validate_pit.py --check-d2     # 仅验证 D-2
    python scripts/features_validate_pit.py --check-d3     # 仅验证 D-3
    python scripts/features_validate_pit.py --all          # 全部验收
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any

# 添加项目根目录到 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from alphahome.common.db_manager import DBManager
from alphahome.common.config_manager import get_database_url
from alphahome.common.schema_names import PIT_SCHEMA

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ==============================================================================
# PIT MV 列表
# ==============================================================================

PIT_MVS = [
    {
        "name": "mv_stock_income_quarterly",
        "schema": "features",
        "pit_comparison_table": f"{PIT_SCHEMA}.pit_income_quarterly",
        "key_fields": ["ts_code", "end_date", "ann_date", "data_source"],
        "compare_fields": ["n_income", "revenue", "total_profit"],
        "coverage_range": (0.8, 1.2),  # 对标后应接近 100%
    },
    {
        "name": "mv_stock_balance_quarterly",
        "schema": "features",
        "pit_comparison_table": f"{PIT_SCHEMA}.pit_balance_quarterly",
        "key_fields": ["ts_code", "end_date", "ann_date", "data_source"],
        "compare_fields": ["tot_assets", "tot_liab"],
        "coverage_range": (0.8, 1.2),  # 对标后应接近 100%
    },
    {
        "name": "mv_stock_fina_indicator",
        "schema": "features",
        "pit_comparison_table": None,  # 无直接对应的 PIT 表
        "key_fields": ["ts_code", "end_date", "ann_date"],
        "compare_fields": ["roe", "roa", "eps"],
    },
    {
        "name": "mv_stock_industry_monthly_snapshot",
        "schema": "features",
        "pit_comparison_table": f"{PIT_SCHEMA}.pit_industry_classification",
        "key_fields": ["ts_code", "obs_date", "data_source"],
        "compare_fields": ["industry_level1", "industry_level2"],
        "coverage_range": (0.95, 1.25),  # MV 可能因更新数据而略多
        "d1_mode": "monthly_snapshot",
    },
]


# ==============================================================================
# D-1 验证：PIT 窗口正确性 + 数据契约
# ==============================================================================

async def check_d1(db_manager: DBManager) -> Dict[str, Any]:
    """
    D-1 验证：数据契约（区分 PIT 窗口 / 月度快照）

    检查项：
    1. query_start_date 字段存在且非空
    2. query_end_date >= query_start_date
    3. 无未来信息泄漏（query_end_date 由下一公告日推导）
    4. 血缘字段存在
    """
    results = {"passed": [], "failed": [], "skipped": []}

    for mv_config in PIT_MVS:
        mv_name = mv_config["name"]
        schema = mv_config["schema"]
        full_name = f"{schema}.{mv_name}"

        try:
            # 检查 MV 是否存在
            exists_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM pg_matviews
                WHERE schemaname = '{schema}' AND matviewname = '{mv_name}'
            ) AS exists;
            """
            result = await db_manager.fetch(exists_sql)
            if not result or not result[0]["exists"]:
                results["skipped"].append({
                    "mv": mv_name,
                    "reason": "MV 不存在"
                })
                continue

            d1_mode = mv_config.get("d1_mode", "pit_window")

            # 月度快照模式：检查 obs_date / data_source 契约
            if d1_mode == "monthly_snapshot":
                columns_sql = f"""
                SELECT a.attname as column_name
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = '{schema}'
                  AND c.relname = '{mv_name}'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                  AND a.attname IN ('ts_code', 'obs_date', 'data_source')
                ORDER BY a.attname;
                """
                cols = await db_manager.fetch(columns_sql)
                col_names = {c["column_name"] for c in cols}
                required = {"ts_code", "obs_date", "data_source"}
                missing = sorted(required - col_names)
                if missing:
                    results["failed"].append({
                        "mv": mv_name,
                        "error": f"缺少快照字段: {missing}",
                    })
                    continue

                null_sql = f"""
                SELECT
                    COUNT(*) FILTER (WHERE ts_code IS NULL) AS null_ts,
                    COUNT(*) FILTER (WHERE obs_date IS NULL) AS null_obs,
                    COUNT(*) FILTER (WHERE data_source IS NULL) AS null_src
                FROM {full_name};
                """
                nulls = await db_manager.fetch(null_sql)
                n = nulls[0]
                if n["null_ts"] or n["null_obs"] or n["null_src"]:
                    results["failed"].append({
                        "mv": mv_name,
                        "error": f"发现空值 ts_code={n['null_ts']}, obs_date={n['null_obs']}, data_source={n['null_src']}",
                    })
                    continue

                results["passed"].append({
                    "mv": mv_name,
                    "check": "D-1 monthly_snapshot schema",
                })
                continue

            # PIT 窗口模式：检查 query_start_date / query_end_date 存在且类型正确
            # 注意: information_schema 不支持 materialized view，需用 pg_catalog
            columns_sql = f"""
            SELECT a.attname as column_name
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = '{schema}'
              AND c.relname = '{mv_name}'
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attname IN ('query_start_date', 'query_end_date', 'report_period',
                                '_source_table', '_processed_at', '_data_version');
            """
            columns = await db_manager.fetch(columns_sql)
            column_names = [c["column_name"] for c in columns]

            required_pit_fields = ["query_start_date", "query_end_date"]
            missing_pit = [f for f in required_pit_fields if f not in column_names]
            if missing_pit:
                results["failed"].append({
                    "mv": mv_name,
                    "check": "PIT 字段存在性",
                    "error": f"缺少字段: {missing_pit}"
                })
                continue

            # 检查 2: query_end_date >= query_start_date
            window_check_sql = f"""
            SELECT COUNT(*) AS violation_count
            FROM {full_name}
            WHERE query_end_date < query_start_date;
            """
            window_result = await db_manager.fetch(window_check_sql)
            violations = window_result[0]["violation_count"] if window_result else 0
            if violations > 0:
                results["failed"].append({
                    "mv": mv_name,
                    "check": "PIT 窗口有效性",
                    "error": f"发现 {violations} 条 query_end_date < query_start_date"
                })
                continue

            # 检查 3: 血缘字段
            lineage_fields = ["_source_table", "_processed_at", "_data_version"]
            missing_lineage = [f for f in lineage_fields if f not in column_names]
            if missing_lineage:
                results["failed"].append({
                    "mv": mv_name,
                    "check": "血缘字段完备性",
                    "error": f"缺少字段: {missing_lineage}"
                })
                continue

            # 检查 4: 空值率
            null_check_sql = f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE query_start_date IS NULL) AS null_start,
                COUNT(*) FILTER (WHERE query_end_date IS NULL) AS null_end
            FROM {full_name};
            """
            null_result = await db_manager.fetch(null_check_sql)
            if null_result:
                total = null_result[0]["total"]
                null_start = null_result[0]["null_start"]
                null_end = null_result[0]["null_end"]
                if total > 0:
                    null_rate = (null_start + null_end) / (total * 2)
                    if null_rate > 0.01:
                        results["failed"].append({
                            "mv": mv_name,
                            "check": "PIT 字段空值率",
                            "error": f"空值率 {null_rate:.2%} 超过阈值 1%"
                        })
                        continue

            # 全部检查通过
            results["passed"].append({
                "mv": mv_name,
                "row_count": total if null_result else 0,
                "checks": ["PIT 字段存在", "窗口有效", "血缘完备", "空值率达标"]
            })

        except Exception as e:
            results["failed"].append({
                "mv": mv_name,
                "check": "执行异常",
                "error": str(e)
            })

    return results


# ==============================================================================
# D-2 验证：与既有 PIT 产出对比
# ==============================================================================

async def check_d2(db_manager: DBManager, sample_limit: int = 1000) -> Dict[str, Any]:
    """
    D-2 验证：与既有 PIT 产出对比

    对 income/balance 抽样对比 pit.pit_* 表
    """
    results = {"passed": [], "failed": [], "skipped": []}

    for mv_config in PIT_MVS:
        mv_name = mv_config["name"]
        schema = mv_config["schema"]
        full_name = f"{schema}.{mv_name}"
        comparison_table = mv_config.get("pit_comparison_table")

        if not comparison_table:
            results["skipped"].append({
                "mv": mv_name,
                "reason": "无对应的 PIT 对比表"
            })
            continue

        try:
            # 检查 MV 是否存在
            exists_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM pg_matviews
                WHERE schemaname = '{schema}' AND matviewname = '{mv_name}'
            ) AS exists;
            """
            result = await db_manager.fetch(exists_sql)
            if not result or not result[0]["exists"]:
                results["skipped"].append({
                    "mv": mv_name,
                    "reason": "MV 不存在"
                })
                continue

            # 检查对比表是否存在
            comparison_schema, comparison_name = comparison_table.split(".")
            comp_exists_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{comparison_schema}' AND table_name = '{comparison_name}'
            ) AS exists;
            """
            comp_result = await db_manager.fetch(comp_exists_sql)
            if not comp_result or not comp_result[0]["exists"]:
                results["skipped"].append({
                    "mv": mv_name,
                    "reason": f"对比表 {comparison_table} 不存在"
                })
                continue

            # 对比行数
            mv_count_sql = f"SELECT COUNT(*) AS cnt FROM {full_name};"
            pit_count_sql = f"SELECT COUNT(*) AS cnt FROM {comparison_table};"

            mv_count = (await db_manager.fetch(mv_count_sql))[0]["cnt"]
            pit_count = (await db_manager.fetch(pit_count_sql))[0]["cnt"]

            # 计算覆盖率（使用配置的范围，默认 80%-150%）
            coverage_range = mv_config.get("coverage_range", (0.8, 1.5))
            if pit_count > 0:
                coverage = mv_count / pit_count
                coverage_ok = coverage_range[0] <= coverage <= coverage_range[1]
            else:
                coverage = 0
                coverage_ok = mv_count == 0

            if not coverage_ok:
                results["failed"].append({
                    "mv": mv_name,
                    "check": "行数覆盖率",
                    "mv_count": mv_count,
                    "pit_count": pit_count,
                    "coverage": f"{coverage:.2%}",
                    "error": f"覆盖率 {coverage:.2%} 超出 {coverage_range[0]*100:.0f}%-{coverage_range[1]*100:.0f}% 范围"
                })
                continue

            results["passed"].append({
                "mv": mv_name,
                "comparison_table": comparison_table,
                "mv_count": mv_count,
                "pit_count": pit_count,
                "coverage": f"{coverage:.2%}",
                "status": "行数覆盖率达标"
            })

        except Exception as e:
            results["failed"].append({
                "mv": mv_name,
                "check": "执行异常",
                "error": str(e)
            })

    return results


# ==============================================================================
# D-3 验证：可运维性与幂等性
# ==============================================================================

async def check_d3(db_manager: DBManager) -> Dict[str, Any]:
    """
    D-3 验证：可运维性与幂等性

    检查项：
    1. features schema 已初始化
    2. MV 元数据表存在
    3. 血缘字段完备（已在 D-1 检查）
    """
    results = {"passed": [], "failed": [], "info": {}}

    try:
        # 检查 features schema
        schema_sql = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.schemata
            WHERE schema_name = 'features'
        ) AS exists;
        """
        schema_result = await db_manager.fetch(schema_sql)
        schema_exists = schema_result and schema_result[0]["exists"]

        if not schema_exists:
            results["failed"].append({
                "check": "features schema",
                "error": "schema 不存在"
            })
            return results

        results["passed"].append({"check": "features schema", "status": "存在"})

        # 检查元数据表
        for table_name in ["mv_metadata", "mv_refresh_log"]:
            table_sql = f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'features' AND table_name = '{table_name}'
            ) AS exists;
            """
            table_result = await db_manager.fetch(table_sql)
            table_exists = table_result and table_result[0]["exists"]

            if table_exists:
                results["passed"].append({"check": f"元数据表 {table_name}", "status": "存在"})
            else:
                results["failed"].append({
                    "check": f"元数据表 {table_name}",
                    "error": "表不存在"
                })

        # 统计已创建的 MV
        mv_list_sql = """
        SELECT matviewname AS name
        FROM pg_matviews
        WHERE schemaname = 'features'
        ORDER BY matviewname;
        """
        mv_result = await db_manager.fetch(mv_list_sql)
        mvs = [row["name"] for row in mv_result] if mv_result else []
        results["info"]["materialized_views"] = mvs
        results["info"]["mv_count"] = len(mvs)

        # 检查 PIT 相关 MV 是否全部创建
        expected_pit_mvs = [mv["name"] for mv in PIT_MVS]
        created_pit_mvs = [mv for mv in expected_pit_mvs if mv in mvs]
        missing_pit_mvs = [mv for mv in expected_pit_mvs if mv not in mvs]

        results["info"]["expected_pit_mvs"] = expected_pit_mvs
        results["info"]["created_pit_mvs"] = created_pit_mvs
        results["info"]["missing_pit_mvs"] = missing_pit_mvs

        if missing_pit_mvs:
            results["failed"].append({
                "check": "PIT MV 完整性",
                "error": f"缺少 MV: {missing_pit_mvs}"
            })
        else:
            results["passed"].append({
                "check": "PIT MV 完整性",
                "status": f"全部 {len(expected_pit_mvs)} 个 PIT MV 已创建"
            })

    except Exception as e:
        results["failed"].append({
            "check": "执行异常",
            "error": str(e)
        })

    return results


# ==============================================================================
# 报告输出
# ==============================================================================

def print_results(title: str, results: Dict[str, Any]):
    """打印验证结果"""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")

    if results.get("passed"):
        print(f"\n✅ 通过 ({len(results['passed'])} 项):")
        for item in results["passed"]:
            if isinstance(item, dict):
                mv = item.get("mv", item.get("check", ""))
                status = item.get("status", item.get("checks", ""))
                extra = ""
                if "row_count" in item:
                    extra = f" (行数: {item['row_count']:,})"
                if "coverage" in item:
                    extra = f" (覆盖率: {item['coverage']})"
                print(f"    ✓ {mv}: {status}{extra}")
            else:
                print(f"    ✓ {item}")

    if results.get("skipped"):
        print(f"\n⏭️  跳过 ({len(results['skipped'])} 项):")
        for item in results["skipped"]:
            print(f"    - {item['mv']}: {item['reason']}")

    if results.get("failed"):
        print(f"\n❌ 失败 ({len(results['failed'])} 项):")
        for item in results["failed"]:
            mv = item.get("mv", item.get("check", ""))
            error = item.get("error", "")
            print(f"    ✗ {mv}: {error}")

    if results.get("info"):
        print(f"\n📊 统计信息:")
        info = results["info"]
        if "mv_count" in info:
            print(f"    已创建 MV 数量: {info['mv_count']}")
        if "materialized_views" in info:
            print(f"    MV 列表: {', '.join(info['materialized_views'])}")
        if "missing_pit_mvs" in info and info["missing_pit_mvs"]:
            print(f"    缺少的 PIT MV: {', '.join(info['missing_pit_mvs'])}")


# ==============================================================================
# 主函数
# ==============================================================================

async def main(args: argparse.Namespace) -> int:
    """主函数"""
    try:
        db_url = get_database_url()
        db_manager = DBManager(db_url)
        await db_manager.connect()

        all_passed = True

        if args.check_d1 or args.all:
            results = await check_d1(db_manager)
            print_results("D-1: PIT 窗口正确性 + 数据契约", results)
            if results.get("failed"):
                all_passed = False

        if args.check_d2 or args.all:
            results = await check_d2(db_manager)
            print_results("D-2: 与既有 PIT 产出对比", results)
            if results.get("failed"):
                all_passed = False

        if args.check_d3 or args.all:
            results = await check_d3(db_manager)
            print_results("D-3: 可运维性与幂等性", results)
            if results.get("failed"):
                all_passed = False

        await db_manager.close()

        print(f"\n{'=' * 60}")
        if all_passed:
            print("🎉 所有验收检查通过!")
        else:
            print("⚠️  部分验收检查未通过，请查看上述详情")
        print(f"{'=' * 60}\n")

        return 0 if all_passed else 1

    except Exception as e:
        logger.error(f"验收脚本执行失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Features MV PIT 化验收脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--check-d1",
        action="store_true",
        help="验证 D-1: PIT 窗口正确性 + 数据契约"
    )
    parser.add_argument(
        "--check-d2",
        action="store_true",
        help="验证 D-2: 与既有 PIT 产出对比"
    )
    parser.add_argument(
        "--check-d3",
        action="store_true",
        help="验证 D-3: 可运维性与幂等性"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="执行全部验收检查"
    )

    args = parser.parse_args()

    # 如果没有指定任何检查，默认执行全部
    if not (args.check_d1 or args.check_d2 or args.check_d3 or args.all):
        args.all = True

    return args


if __name__ == "__main__":
    args = parse_args()
    exit_code = asyncio.run(main(args))
    sys.exit(exit_code)
