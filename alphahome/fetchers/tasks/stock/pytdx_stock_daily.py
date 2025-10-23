from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd

from ...sources.pytdx import PytdxTask
from ....common.task_system.task_decorator import task_register


@task_register()
class PytdxStockDailyTask(PytdxTask):
    """pytdx股票日线数据任务

    借鉴hikyuu的实现，获取全市场股票的日线交易数据。
    从postgresql的tushare.stock_basic表获取股票列表，
    使用pytdx从通达信服务器获取数据，并更新到HDF5文件。
    表结构与hikyuu保持一致。
    """

    # 1.核心属性
    domain = "stock"  # 业务域标识
    name = "pytdx_stock_daily"
    description = "获取A股股票日线行情数据（pytdx数据源）"
    table_name = "stock_daily"
    primary_keys = ["ts_code", "trade_date"]
    date_column = "trade_date"  # 日期列名，用于确认最新数据日期
    default_start_date = "19901219"  # A股最早交易日
    smart_lookback_days = 3  # 智能增量模式下，回看3天

    # --- 代码级默认配置 (会被 config.json 覆盖) --- #
    default_concurrent_limit = 3  # pytdx并发限制较小
    default_batch_size = 800  # pytdx每次最多获取800条记录

    # 2.数据类型转换
    transformations = {
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        "amount": float,
    }

    # 3.列名映射 (无映射，保持与tushare一致)

    # 4.表结构定义 (与hikyuu表结构保持一致)
    schema_def = {
        "ts_code": {"type": "VARCHAR(15)", "constraints": "NOT NULL"},
        "trade_date": {"type": "DATE", "constraints": "NOT NULL"},
        "open": {"type": "NUMERIC(15,4)"},
        "high": {"type": "NUMERIC(15,4)"},
        "low": {"type": "NUMERIC(15,4)"},
        "close": {"type": "NUMERIC(15,4)"},
        "volume": {"type": "NUMERIC(20,2)"},
        "amount": {"type": "NUMERIC(20,3)"},
    }

    # 5.数据验证规则
    validations = [
        (lambda df: df["close"] > 0, "收盘价必须为正数"),
        (lambda df: df["open"] > 0, "开盘价必须为正数"),
        (lambda df: df["high"] > 0, "最高价必须为正数"),
        (lambda df: df["low"] > 0, "最低价必须为正数"),
        (lambda df: df["volume"] >= 0, "成交量不能为负数"),
        (lambda df: df["amount"] >= 0, "成交额不能为负数"),
        (lambda df: df["high"] >= df["low"], "最高价不能低于最低价"),
        (lambda df: df["high"] >= df["open"], "最高价不能低于开盘价"),
        (lambda df: df["high"] >= df["close"], "最高价不能低于收盘价"),
        (lambda df: df["low"] <= df["open"], "最低价不能高于开盘价"),
        (lambda df: df["low"] <= df["close"], "最低价不能高于收盘价"),
    ]

    # 6.验证模式配置
    validation_mode = "report"  # 报告验证结果但保留所有数据

    # 7.批次配置
    batch_records_all_codes = 800    # 全市场查询时，每个批次的记录数量 (pytdx限制)


    async def get_batch_list(self, **kwargs: Any) -> List[Dict]:
        """
        生成基于交易日的批处理参数列表。

        借鉴hikyuu的实现，仅支持全市场模式。
        从postgresql的tushare.stock_basic表获取股票列表。
        """
        # 参数提取和验证
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        market = kwargs.get("market")  # SH, SZ, BJ 或 None(全市场)

        # 支持基类的全量更新机制：如果没有提供日期范围，使用默认范围
        if not start_date:
            start_date = self.default_start_date
            self.logger.info(f"任务 {self.name}: 未提供 start_date，使用默认起始日期: {start_date}")
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
            self.logger.info(f"任务 {self.name}: 未提供 end_date，使用当前日期: {end_date}")

        if not start_date or not end_date:
            self.logger.error(f"任务 {self.name}: 缺少必要的日期参数")
            return []

        # 如果开始日期晚于结束日期，说明数据已是最新，无需更新
        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            self.logger.info(
                f"起始日期 ({start_date}) 晚于结束日期 ({end_date})，无需执行任务。"
            )
            return []

        self.logger.info(
            f"任务 {self.name}: 生成批处理列表，范围: {start_date} 到 {end_date}, 市场: {market or '全市场'}"
        )

        try:
            # 获取股票列表 (全市场模式)
            stock_list = await self.api.get_stock_list_from_db(self.db, market)
            if not stock_list:
                self.logger.warning("未获取到有效的股票列表")
                return []

            # 检查是否有调试限制
            debug_limit = kwargs.get("debug_limit")
            if debug_limit:
                stock_list = stock_list[:debug_limit]
                self.logger.info(f"调试模式：只处理前 {debug_limit} 只股票")

            # 为每个股票生成批次
            batch_list = []
            for stock_record in stock_list:
                stockid, marketid, code, valid, stktype = stock_record

                # 转换为alphahome的市场代码
                market_map = {0: 'SZ', 1: 'SH', 2: 'BJ'}
                market_code = market_map.get(marketid, 'SH')

                # 生成批次参数
                batch_params = {
                    "stock_record": stock_record,  # 传递完整的股票记录信息
                    "code": code,
                    "market": market_code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "batch_size": self.batch_records_all_codes
                }
                batch_list.append(batch_params)

            self.logger.info(f"任务 {self.name}: 成功生成 {len(batch_list)} 个批次")
            return batch_list

        except Exception as e:
            self.logger.error(
                f"任务 {self.name}: 生成批次时出错: {e}", exc_info=True
            )
            return []

    async def _fetch_raw_data(self, params: Dict) -> List[Dict]:
        """
        从pytdx获取日线原始数据

        借鉴hikyuu的实现方式，从stock_record中提取股票信息。

        Args:
            params: 参数字典，包含stock_record等

        Returns:
            原始日线数据列表
        """
        stock_record = params.get("stock_record")
        if not stock_record:
            self.logger.error("缺少股票记录参数")
            return []

        stockid, marketid, code, valid, stktype = stock_record

        # 转换为市场代码
        market_map = {0: 'SZ', 1: 'SH', 2: 'BJ'}
        market = market_map.get(marketid, 'SH')

        if not code or valid != 1:
            self.logger.debug(f"跳过无效股票: {market}{code} (valid={valid})")
            return []

        try:
            # 计算需要获取的记录数量
            batch_size = params.get("batch_size", self.batch_records_all_codes)

            self.logger.info(f"开始获取 {market}{code} 的日线数据，批次大小: {batch_size}")

            # 获取日线数据
            raw_data = await self.api.get_stock_daily_bars(
                market=market,
                code=code,
                start=0,  # 从最新数据开始
                count=batch_size
            )

            if raw_data:
                self.logger.info(f"成功从pytdx获取 {market}{code} 的 {len(raw_data)} 条日线数据")
                return raw_data
            else:
                self.logger.warning(f"从pytdx获取 {market}{code} 日线数据失败，返回空数据")
                return []

        except Exception as e:
            self.logger.error(f"从pytdx获取 {market}{code} 日线数据时出错: {e}")
            import traceback
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            return []

    def _transform_data(self, raw_data: List[Dict], params: Dict) -> pd.DataFrame:
        """
        转换原始数据为标准格式

        Args:
            raw_data: pytdx返回的原始数据
            params: 参数信息，包含stock_record

        Returns:
            标准格式的DataFrame
        """
        stock_record = params.get("stock_record")
        if not stock_record:
            self.logger.error("缺少股票记录参数")
            return pd.DataFrame()

        stockid, marketid, code, valid, stktype = stock_record

        # 转换为市场代码
        market_map = {0: 'SZ', 1: 'SH', 2: 'BJ'}
        market = market_map.get(marketid, 'SH')

        return self.data_transformer.transform_daily_bars(raw_data, market, code)
