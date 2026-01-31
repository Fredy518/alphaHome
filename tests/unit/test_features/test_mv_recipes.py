import pytest


@pytest.mark.parametrize(
    "mv_class, expected_sources",
    [
        (
            "StockIndustryMonthlySnapshotMV",
            ["rawdata.index_swmember", "rawdata.index_cimember"],
        ),
        (
            "StockFinaIndicatorMV",
            ["rawdata.fina_indicator"],
        ),
        (
            "StockIncomeQuarterlyMV",
            ["rawdata.fina_income", "rawdata.fina_express", "rawdata.fina_forecast"],
        ),
        (
            "StockBalanceQuarterlyMV",
            ["rawdata.fina_balancesheet", "rawdata.fina_express"],
        ),
        (
            "StockDailyEnrichedMV",
            ["rawdata.stock_daily", "rawdata.stock_dailybasic"],
        ),
        (
            "MarketStatsDailyMV",
            ["rawdata.stock_dailybasic"],
        ),
        # M7 新增：日频市场情绪（含 margin 指标）
        (
            "MarketSentimentDailyMV",
            ["tushare.stock_factor_pro", "tushare.stock_limitlist", "tushare.stock_st", "tushare.stock_margin"],
        ),
        # M7 新增：指数综合特征
        (
            "IndexFeaturesDailyMV",
            ["tushare.index_dailybasic", "tushare.index_factor_pro", "akshare.macro_bond_rate"],
        ),
        # M7 第二批：宏观利率特征
        (
            "MacroRateDailyMV",
            ["akshare.macro_bond_rate"],
        ),
        # M7 第二批：行业特征
        (
            "IndustryFeaturesDailyMV",
            ["tushare.index_swdaily", "tushare.index_swmember"],
        ),
        # 新增：行业龙虎榜聚合（日频，申万一级/二级）
        (
            "IndustryToplistSignalDailyMV",
            ["rawdata.stock_toplist", "rawdata.index_swmember", "rawdata.stock_daily"],
        ),
        # M7 第三批：市场技术特征
        (
            "MarketTechnicalDailyMV",
            ["tushare.stock_factor_pro"],
        ),
        # M7 第三批：指数技术特征
        (
            "IndexTechnicalDailyMV",
            ["tushare.index_factor_pro"],
        ),
        # M7 第三批：风格特征
        (
            "StyleFeaturesDailyMV",
            ["tushare.index_factor_pro"],
        ),
        # M7 第三批：两融成交占比
        (
            "MarginTurnoverDailyMV",
            ["tushare.stock_margin", "tushare.stock_daily"],
        ),
        # M7 第三批：ETF 资金流
        (
            "ETFFlowDailyMV",
            ["tushare.fund_share", "tushare.fund_nav"],
        ),
        # M7 第三批：指数加权基本面
        (
            "IndexFundamentalDailyMV",
            ["tushare.index_weight", "tushare.stock_dailybasic"],
        ),
        # M7 第四批：风险偏好代理
        (
            "RiskAppetiteDaily",
            [
                "tushare.stock_factor_pro",
                "tushare.stock_st",
                "tushare.stock_basic",
                "tushare.cbond_daily",
            ],
        ),
        # M7 第四批：期权情绪（日频）
        (
            "OptionSentimentDailyMV",
            ["tushare.option_basic", "tushare.option_daily"],
        ),
        # M7 Phase 2：股指期货特征
        (
            "FuturesFeaturesDaily",
            ["tushare.future_daily", "tushare.future_holding", "tushare.index_daily"],
        ),
        # M7 Phase 2：大小盘分化
        (
            "MarketSizeDaily",
            ["tushare.stock_factor_pro"],
        ),
        # M7 Phase 2：资金流向
        (
            "MoneyFlowDailyMV",
            ["tushare.stock_moneyflow"],
        ),
        # M7 Phase 3：回购周频
        (
            "RepurchaseWeeklyMV",
            ["tushare.stock_repurchase"],
        ),
        # M7 Phase 3：股东增减持周频
        (
            "HolderTradeWeeklyMV",
            ["tushare.stock_holdertrade"],
        ),
        # M7 Phase 3：涨跌停行业分布
        (
            "LimitIndustryDailyMV",
            ["tushare.stock_limitlist", "rawdata.index_swmember"],
        ),
        # 新增三星级特征：现金流量表
        (
            "StockCashflowQuarterlyMV",
            ["rawdata.fina_cashflow"],
        ),
        # 新增三星级特征：股东户数集中度
        (
            "StockShareholderConcentrationMV",
            ["rawdata.stock_holdernumber"],
        ),
        # 新增三星级特征：龙虎榜事件信号（日频，稀疏事件表，滚动按交易日）
        (
            "StockToplistEventDailyMV",
            ["rawdata.stock_toplist", "rawdata.stock_daily"],
        ),
        # 新增三星级特征：概念板块
        (
            "ConceptFeaturesDailyMV",
            ["rawdata.stock_kplconcept"],
        ),
        # 新增三星级特征：分析师覆盖
        (
            "StockAnalystCoverageMV",
            ["rawdata.stock_report_rc"],
        ),
        # 新增三星级特征：限售股解禁
        (
            "StockSharefloatScheduleMV",
            ["rawdata.stock_sharefloat"],
        ),
        # 新增三星级特征：宏观流动性
        (
            "MacroLiquidityMonthlyMV",
            ["rawdata.macro_sf_month", "rawdata.macro_cn_m"],
        ),
        # 新增三星级特征：公募重仓股
        (
            "FundHoldingsQuarterlyMV",
            ["rawdata.fund_portfolio"],
        ),
        # 新增三星级特征：AH溢价
        (
            "AHPremiumDailyMV",
            ["rawdata.stock_ahcomparison"],
        ),
        # 新增三星级特征：打板指数
        (
            "DCIndexFeaturesDailyMV",
            ["rawdata.stock_dcindex"],
        ),
        # 新增：RSRS 指标组件
        (
            "IndexRSRSDailyMV",
            ["tushare.index_daily"],
        ),
    ],
)
def test_mv_sql_generation_and_sources(mv_class: str, expected_sources: list[str]):
    import alphahome.features.recipes.mv as mv_pkg

    mv_cls = getattr(mv_pkg, mv_class)
    mv = mv_cls(schema="features")
    sql = mv.get_create_sql()

    assert "CREATE MATERIALIZED VIEW features." in sql
    assert "materialized_views." not in sql
    assert "pgs_factors." not in sql

    for src in expected_sources:
        assert src in sql


def test_schema_is_forced_to_features():
    from alphahome.features.recipes.mv import StockIndustryMonthlySnapshotMV

    with pytest.raises(ValueError):
        StockIndustryMonthlySnapshotMV(schema="materialized_views")

