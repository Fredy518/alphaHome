"""
MV SQL 片段级单测 - PIT/排除当日等关键逻辑验证

这些测试通过解析生成的 SQL 验证关键算法片段的正确性，
确保 PIT（Point-in-Time）、排除当日、分位数计算等逻辑符合预期。
"""

import pytest
import re


class TestPITPatterns:
    """PIT（Point-in-Time）相关模式测试"""

    def test_index_features_daily_pit_weight(self):
        """index_features_daily: 使用前一日权重避免未来函数"""
        from alphahome.features.recipes.mv import IndexFeaturesDailyMV

        mv = IndexFeaturesDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 验证权重使用 LAG 获取前一日
        assert "LAG" in sql or "w.trade_date < d.trade_date" in sql, \
            "权重应使用前一日数据（LAG 或 trade_date 条件）"

    def test_index_fundamental_daily_pit_weight(self):
        """index_fundamental_daily: 加权基本面使用 PIT 权重"""
        from alphahome.features.recipes.mv import IndexFundamentalDailyMV

        mv = IndexFundamentalDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 验证权重关联条件：weight_date <= trade_date（使用当日或之前最近的权重）
        # 实际实现使用 wd.weight_date <= t.trade_date
        has_pit_condition = (
            "weight_date <= " in sql.lower()
            or "weight_date <" in sql.lower()
            or "w.trade_date < d.trade_date" in sql
            or "w.trade_date <= d.trade_date" in sql
        )
        assert has_pit_condition, \
            "应使用 weight_date <= trade_date 确保 PIT 权重"

        # 验证使用最近一期权重（通过 MAX(weight_date) 实现）
        has_latest_weight_logic = (
            "MAX(" in sql.upper() and "weight_date" in sql.lower()
        ) or "ROW_NUMBER()" in sql.upper() or "DISTINCT ON" in sql.upper()
        
        assert has_latest_weight_logic, \
            "应有逻辑选取最近一期可用权重（MAX/ROW_NUMBER/DISTINCT ON）"

    def test_stock_financial_pit_uses_pit_ann_date(self):
        """财务三表 PIT 生效日：pit_ann_date=COALESCE(f_ann_date, ann_date)"""
        from alphahome.features.recipes.mv import (
            StockIncomeQuarterlyMV,
            StockBalanceQuarterlyMV,
            StockCashflowQuarterlyMV,
        )

        for mv_cls in [StockIncomeQuarterlyMV, StockBalanceQuarterlyMV, StockCashflowQuarterlyMV]:
            mv = mv_cls(schema="features")
            sql = mv.get_create_sql().lower()
            assert "coalesce(f_ann_date, ann_date)" in sql, f"{mv_cls.__name__} should use pit_ann_date"


class TestExcludeTodayPatterns:
    """排除当日数据的模式测试"""

    def test_market_sentiment_daily_excludes_today_for_percentile(self):
        """market_sentiment_daily: 历史分位数应排除当日"""
        from alphahome.features.recipes.mv import MarketSentimentDailyMV

        mv = MarketSentimentDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 验证历史分位数计算排除当日
        # 通常通过 ROWS BETWEEN ... AND 1 PRECEDING 或 trade_date < ... 实现
        sql_upper = sql.upper()

        # 检查 percentile 计算的窗口定义
        has_exclude_today_pattern = any([
            "1 PRECEDING" in sql_upper,  # 窗口到前一日
            "UNBOUNDED PRECEDING AND 1 PRECEDING" in sql_upper,
            re.search(r"trade_date\s*<\s*\w+\.trade_date", sql, re.IGNORECASE),  # 日期小于当日
        ])

        # 注意：如果使用 PERCENT_RANK() 基于历史全量，需要不同的验证逻辑
        # 此测试主要验证不会把当日数据纳入分母
        assert has_exclude_today_pattern or "PERCENT_RANK" in sql_upper, \
            "历史分位数应排除当日或使用 PERCENT_RANK 按历史计算"

    def test_market_sentiment_daily_new_high_low_excludes_today(self):
        """market_sentiment_daily: 新高新低计算应排除当日"""
        from alphahome.features.recipes.mv import MarketSentimentDailyMV

        mv = MarketSentimentDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 查找 new_high/new_low 相关逻辑
        # 预期模式：当日最高/最低 vs 过去 N 日最高/最低
        # 过去 N 日应不含当日

        # 检查窗口函数中的 ROWS BETWEEN 定义
        sql_upper = sql.upper()

        # 对于新高新低，常见模式是：
        # MAX(high) OVER (... ROWS BETWEEN N PRECEDING AND 1 PRECEDING)
        # 或者使用子查询 WHERE trade_date < current_date
        has_valid_lookback = any([
            "AND 1 PRECEDING" in sql_upper,  # 窗口到前一日
            re.search(r"ROWS\s+BETWEEN\s+\d+\s+PRECEDING\s+AND\s+1\s+PRECEDING", sql_upper),
        ])

        # 如果使用 LAG 或子查询方式也是合理的
        has_lag_pattern = "LAG(" in sql_upper

        assert has_valid_lookback or has_lag_pattern, \
            "新高新低计算应使用 '...AND 1 PRECEDING' 窗口或 LAG 函数排除当日"


class TestRollingWindowPatterns:
    """滚动窗口计算模式测试"""

    def test_macro_rate_daily_rolling_window(self):
        """macro_rate_daily: 滚动统计窗口正确性"""
        from alphahome.features.recipes.mv import MacroRateDailyMV

        mv = MacroRateDailyMV(schema="features")
        sql = mv.get_create_sql()

        sql_upper = sql.upper()

        # 预期有滚动窗口定义，可以是：
        # 1. ROWS BETWEEN N PRECEDING（显式窗口）
        # 2. LAG(col, N)（隐式历史回溯）
        # 3. 子查询 + 日期条件
        has_rolling_pattern = (
            re.search(r"ROWS\s+BETWEEN\s+(\d+|UNBOUNDED)\s+PRECEDING\s+AND", sql_upper)
            or "LAG(" in sql_upper
            or re.search(r"trade_date\s*>\s*trade_date\s*-\s*INTERVAL", sql, re.IGNORECASE)
        )

        assert has_rolling_pattern, \
            "应有 ROWS BETWEEN、LAG 或日期回溯的滚动窗口"

    def test_index_technical_daily_rolling_window(self):
        """index_technical_daily: 技术指标滚动窗口"""
        from alphahome.features.recipes.mv import IndexTechnicalDailyMV

        mv = IndexTechnicalDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 验证有滚动窗口用于动量/波动率计算
        sql_upper = sql.upper()

        # 检查 PARTITION BY + ORDER BY 模式（用于滚动计算）
        has_partitioned_window = "PARTITION BY" in sql_upper and "ORDER BY" in sql_upper

        assert has_partitioned_window, \
            "应有 PARTITION BY + ORDER BY 的窗口定义"


class TestDataQualityPatterns:
    """数据质量相关模式测试"""

    def test_market_stats_has_coverage_ratio(self):
        """market_stats_daily: 应输出有效覆盖率"""
        from alphahome.features.recipes.mv import MarketStatsDailyMV

        mv = MarketStatsDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 验证输出包含覆盖率字段
        assert "valid_coverage_ratio" in sql.lower() or "coverage" in sql.lower(), \
            "应输出有效覆盖率字段"

    def test_market_stats_has_stock_counts(self):
        """market_stats_daily: 应同时输出全市场股票数和有效样本数"""
        from alphahome.features.recipes.mv import MarketStatsDailyMV

        mv = MarketStatsDailyMV(schema="features")
        sql = mv.get_create_sql()

        sql_lower = sql.lower()

        # 验证有两类股票数
        has_total_count = "total_stock_count" in sql_lower or "all_stock" in sql_lower
        has_valid_count = "valid_stock_count" in sql_lower or "valid_count" in sql_lower

        assert has_total_count and has_valid_count, \
            "应同时输出全市场股票数和有效样本数"


class TestIndustryDistributionPatterns:
    """行业收益分布形态（偏度/峰度）"""

    def test_industry_features_has_skew_kurtosis(self):
        """industry_features_daily: 应输出行业收益偏度/峰度"""
        from alphahome.features.recipes.mv import IndustryFeaturesDailyMV

        mv = IndustryFeaturesDailyMV(schema="features")
        sql = mv.get_create_sql().lower()

        assert "l2_code" in sql, "应基于 index_swmember.l2_code 枚举申万二级行业"
        assert "industry_return_skew" in sql, "应输出 industry_return_skew"
        assert "industry_return_kurtosis_excess" in sql, "应输出 industry_return_kurtosis_excess"


class TestDynamicEnumerationPatterns:
    """动态枚举模式测试"""

    def test_etf_flow_dynamic_enumeration(self):
        """etf_flow_daily: 应从 fund_etf_basic 动态获取 ETF 列表"""
        from alphahome.features.recipes.mv import ETFFlowDailyMV

        mv = ETFFlowDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 验证使用 fund_etf_basic 表
        assert "fund_etf_basic" in sql.lower(), \
            "应从 fund_etf_basic 动态获取 ETF 列表"

        # 验证按 index_code 筛选
        assert "index_code" in sql.lower(), \
            "应按 index_code 筛选跟踪目标指数的 ETF"


class TestIndexCodePatterns:
    """指数代码正确性测试"""

    def test_style_features_500_value_growth_codes(self):
        """style_features_daily: 500价值/成长指数代码正确性"""
        from alphahome.features.recipes.mv import StyleFeaturesDailyMV

        mv = StyleFeaturesDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 500价值应为 H30351.CSI（非 000925.CSI）
        # 500成长应为 H30352.CSI（非 000926.CSI）
        assert "H30351" in sql, \
            "500价值指数代码应为 H30351.CSI"
        assert "H30352" in sql, \
            "500成长指数代码应为 H30352.CSI"

        # 确保不使用错误代码
        assert "000925.CSI" not in sql, \
            "不应使用 000925.CSI 作为500价值代码"
        assert "000926.CSI" not in sql, \
            "不应使用 000926.CSI 作为500成长代码"

    def test_index_mvs_consistent_index_list(self):
        """index_technical_daily 和 index_fundamental_daily 应使用一致的指数清单"""
        from alphahome.features.recipes.mv import IndexTechnicalDailyMV, IndexFundamentalDailyMV

        tech_mv = IndexTechnicalDailyMV(schema="features")
        fund_mv = IndexFundamentalDailyMV(schema="features")

        tech_sql = tech_mv.get_create_sql()
        fund_sql = fund_mv.get_create_sql()

        # 提取指数代码（简化检查：验证核心指数都存在）
        core_indexes = [
            "000300",  # HS300
            "000905",  # ZZ500
            "000852",  # ZZ1000
            "000016",  # SZ50
            "399006",  # CYB
            "000001",  # SZZZ
        ]

        for idx in core_indexes:
            assert idx in tech_sql, f"index_technical_daily 应包含 {idx}"
            assert idx in fund_sql, f"index_fundamental_daily 应包含 {idx}"


class TestMVContracts:
    """MV 输出契约：血缘字段/单语句创建等"""

    def _iter_mv_classes(self):
        import inspect

        import alphahome.features.recipes.mv as mv_pkg
        from alphahome.features.storage.base_view import BaseFeatureView

        seen: set[int] = set()
        for name in getattr(mv_pkg, "__all__", []):
            obj = getattr(mv_pkg, name, None)
            if not inspect.isclass(obj):
                continue
            if not issubclass(obj, BaseFeatureView):
                continue
            if id(obj) in seen:
                continue
            seen.add(id(obj))
            yield obj

    def test_all_mvs_have_lineage_fields(self):
        for mv_cls in self._iter_mv_classes():
            mv = mv_cls(schema="features")
            sql = mv.get_create_sql().lower()
            assert "_source_table" in sql, f"{mv_cls.__name__} missing _source_table"
            assert "_processed_at" in sql, f"{mv_cls.__name__} missing _processed_at"
            assert "_data_version" in sql, f"{mv_cls.__name__} missing _data_version"

    def test_create_sql_has_no_create_index(self):
        """asyncpg 执行 CREATE MATERIALIZED VIEW 时不应夹带 CREATE INDEX 多语句。"""
        for mv_cls in self._iter_mv_classes():
            mv = mv_cls(schema="features")
            sql = mv.get_create_sql().lower()
            assert "create index" not in sql, f"{mv_cls.__name__} create_sql contains CREATE INDEX"


class TestSourceTablePatterns:
    """源表引用正确性测试"""

    def test_market_sentiment_includes_margin_sources(self):
        """market_sentiment_daily: 合并 margin 后应包含 margin 源表"""
        from alphahome.features.recipes.mv import MarketSentimentDailyMV

        mv = MarketSentimentDailyMV(schema="features")
        sql = mv.get_create_sql()

        # 验证包含 margin 数据源
        sql_lower = sql.lower()
        has_margin_source = (
            "stock_margin" in sql_lower
            or "margin" in sql_lower
        )

        # 验证输出包含 margin 相关字段
        has_margin_fields = any([
            "margin_balance" in sql_lower,
            "margin_ratio" in sql_lower,
            "margin_net" in sql_lower,
        ])

        assert has_margin_source, \
            "market_sentiment_daily 应包含 margin 数据源"
        assert has_margin_fields, \
            "market_sentiment_daily 应输出 margin 相关字段"

    def test_market_sentiment_has_margin_billion_fields(self):
        """market_sentiment_daily: 应提供 *_billion 字段便于单位一致"""
        from alphahome.features.recipes.mv import MarketSentimentDailyMV

        mv = MarketSentimentDailyMV(schema="features")
        sql_lower = mv.get_create_sql().lower()

        assert "total_margin_balance_billion" in sql_lower
        assert "total_short_balance_billion" in sql_lower

    def test_limit_industry_daily_avoids_month_end_snapshot_leakage(self):
        """limit_industry_daily: 行业映射应使用 index_swmember in_date/out_date as-of，而非当月月末快照。"""
        from alphahome.features.recipes.mv import LimitIndustryDailyMV

        mv = LimitIndustryDailyMV(schema="features")
        sql = mv.get_create_sql().lower()

        assert "rawdata.index_swmember" in sql
        assert "in_date <=" in sql and "out_date" in sql, "应包含 in_date/out_date as-of 条件"
        assert "mv_stock_industry_monthly_snapshot" not in sql, "不应使用月末快照 join（可能月内未来函数）"
