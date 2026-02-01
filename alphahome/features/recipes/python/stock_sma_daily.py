"""
股票简单移动平均线特征（Python 计算示例）

说明：
- 这是一个 Python 计算特征的示例
- 从 rawdata.stock_daily 读取数据，计算 SMA5/SMA10/SMA20
- 结果存入 features.mv_stock_sma_daily 表

用途：
- 演示 Python 特征的工作流程
- 测试 GUI 的全量/增量刷新功能
"""

import pandas as pd

from alphahome.features.registry import feature_register
from alphahome.features.storage.python_feature import PythonFeatureTable


@feature_register
class StockSmaDailyFeature(PythonFeatureTable):
    """股票简单移动平均线特征（Python 计算）。"""

    name = "stock_sma_daily"
    description = "股票简单移动平均线 (SMA5/SMA10/SMA20) - Python 计算示例"
    category = "stock"  # 显式指定分类
    source_tables = ["rawdata.stock_daily"]
    refresh_strategy = "incremental"
    incremental_days = 30
    date_column = "trade_date"

    def get_create_sql(self) -> str:
        """返回创建表的 SQL。"""
        # 保持单语句，避免某些驱动不支持 multi-statement SQL
        return """
        CREATE TABLE IF NOT EXISTS features.mv_stock_sma_daily (
            ts_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            close NUMERIC(20, 4),
            sma5 NUMERIC(20, 4),
            sma10 NUMERIC(20, 4),
            sma20 NUMERIC(20, 4),
            sma5_ratio NUMERIC(10, 4),
            sma10_ratio NUMERIC(10, 4),
            sma20_ratio NUMERIC(10, 4),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (ts_code, trade_date)
        )
        """.strip()

    def get_post_create_sqls(self) -> list[str]:
        """创建索引。"""
        return [
            "CREATE INDEX IF NOT EXISTS idx_stock_sma_daily_trade_date "
            "ON features.mv_stock_sma_daily (trade_date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_stock_sma_daily_ts_code "
            "ON features.mv_stock_sma_daily (ts_code)",
            "COMMENT ON TABLE features.mv_stock_sma_daily "
            "IS '股票简单移动平均线特征（Python计算）'",
            "COMMENT ON COLUMN features.mv_stock_sma_daily.sma5 "
            "IS '5日简单移动平均线'",
            "COMMENT ON COLUMN features.mv_stock_sma_daily.sma10 "
            "IS '10日简单移动平均线'",
            "COMMENT ON COLUMN features.mv_stock_sma_daily.sma20 "
            "IS '20日简单移动平均线'",
            "COMMENT ON COLUMN features.mv_stock_sma_daily.sma5_ratio "
            "IS '收盘价/SMA5 - 1'",
            "COMMENT ON COLUMN features.mv_stock_sma_daily.sma10_ratio "
            "IS '收盘价/SMA10 - 1'",
            "COMMENT ON COLUMN features.mv_stock_sma_daily.sma20_ratio "
            "IS '收盘价/SMA20 - 1'",
        ]

    async def compute(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        计算指定日期范围的 SMA 特征。

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)

        Returns:
            pd.DataFrame: 计算结果
        """
        if self._db_manager is None:
            raise RuntimeError("db_manager 未设置")

        self.logger.info(f"计算 SMA 特征: {start_date} - {end_date}")

        # 为了计算 SMA20，需要向前多取 20 个交易日的数据
        # 这里简化处理，向前多取 30 天
        from datetime import datetime, timedelta

        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            # 向前多取 60 天以确保有足够数据计算 SMA
            extended_start = (start_dt - timedelta(days=60)).strftime("%Y%m%d")
        except ValueError:
            extended_start = start_date

        # 查询原始数据
        sql = f"""
        SELECT 
            ts_code,
            trade_date,
            close
        FROM rawdata.stock_daily
        WHERE trade_date >= '{extended_start}'
          AND trade_date <= '{end_date}'
          AND close IS NOT NULL
        ORDER BY ts_code, trade_date
        """

        result = await self._db_manager.fetch(sql)

        if not result:
            self.logger.warning("没有查询到数据")
            return pd.DataFrame()

        # 转换为 DataFrame
        df = pd.DataFrame([dict(r) for r in result])

        if df.empty:
            return pd.DataFrame()

        self.logger.info(f"查询到 {len(df)} 条原始数据")

        # 将 Decimal 转换为 float（PostgreSQL 返回的数值类型）
        df["close"] = df["close"].astype(float)

        # 按股票分组计算 SMA
        def calc_sma(group):
            group = group.sort_values("trade_date")
            group["sma5"] = group["close"].rolling(window=5, min_periods=5).mean()
            group["sma10"] = group["close"].rolling(window=10, min_periods=10).mean()
            group["sma20"] = group["close"].rolling(window=20, min_periods=20).mean()

            # 计算比率
            group["sma5_ratio"] = (group["close"] / group["sma5"] - 1).round(4)
            group["sma10_ratio"] = (group["close"] / group["sma10"] - 1).round(4)
            group["sma20_ratio"] = (group["close"] / group["sma20"] - 1).round(4)

            return group

        df = df.groupby("ts_code", group_keys=False).apply(calc_sma)

        # 只保留目标日期范围内的数据
        df = df[df["trade_date"].astype(str).str.replace("-", "") >= start_date]
        df = df[df["trade_date"].astype(str).str.replace("-", "") <= end_date]

        # 删除 SMA 计算失败的行（数据不足）
        df = df.dropna(subset=["sma5", "sma10", "sma20"])

        # 添加更新时间（使用 Python datetime，带时区）
        from datetime import timezone
        df["updated_at"] = datetime.now(timezone.utc)

        # 确保列顺序正确
        columns = [
            "ts_code", "trade_date", "close",
            "sma5", "sma10", "sma20",
            "sma5_ratio", "sma10_ratio", "sma20_ratio",
            "updated_at"
        ]
        df = df[columns]

        self.logger.info(f"计算完成，共 {len(df)} 条结果")

        return df
