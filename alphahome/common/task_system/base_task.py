import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd


class BaseTask(ABC):
    """统一数据任务基类
    
    支持数据采集(fetch)和数据处理(processor)两种任务类型的统一框架。
    为所有任务提供基础功能，包括数据获取、处理、验证和保存。
    """

    # 任务类型标识 ('fetch', 'processor', 'derivative', etc.)
    task_type: str = "base"

    # 必须由子类定义的属性
    name = None
    table_name = None

    # 可选属性（有合理默认值）
    primary_keys = []
    date_column = None
    description = ""
    auto_add_update_time = True  # 是否自动添加更新时间
    data_source: Optional[str] = None  # 数据源标识（如'tushare', 'wind', 'jqdata'等）
    
    # 新增：支持processor任务的属性
    source_tables = []        # 源数据表列表（processor任务使用）
    dependencies = []         # 依赖的其他任务（processor任务使用）

    def __init__(self, db_connection):
        """初始化任务"""
        if self.name is None or self.table_name is None:
            raise ValueError("必须定义name和table_name属性")

        self.db = db_connection
        self.logger = logging.getLogger(f"task.{self.name}")

    async def execute(
        self, stop_event: Optional[asyncio.Event] = None, **kwargs
    ):
        """执行任务的完整生命周期"""
        self.logger.info(f"开始执行任务: {self.name} (类型: {self.task_type})")

        try:
            if stop_event and stop_event.is_set():
                self.logger.warning(f"任务 {self.name} 在开始前被取消。")
                raise asyncio.CancelledError("任务在开始前被取消")

            await self.pre_execute(stop_event=stop_event)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 pre_execute 后被取消")

            # 获取数据
            self.logger.info(f"获取数据，参数: {kwargs}")
            data = await self.fetch_data(stop_event=stop_event, **kwargs)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 fetch_data 后被取消")

            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.info("没有获取到数据")
                return {"status": "no_data", "rows": 0}

            # 处理数据
            self.logger.info(f"处理数据，共 {len(data) if isinstance(data, pd.DataFrame) else '多源'} 行")
            data = self.process_data(data, stop_event=stop_event, **kwargs)

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 process_data 后被取消")

            # 再次检查处理后的数据是否为空
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                self.logger.warning("数据处理后为空")
                return {"status": "no_data", "rows": 0}

            # 验证数据
            self.logger.info("验证数据")
            validation_passed = self.validate_data(data)
            if not validation_passed:
                self.logger.warning("数据验证未通过，但将继续保存")

            # 保存数据
            self.logger.info(f"保存数据到表 {self.table_name}")
            result = await self.save_data(data, stop_event=stop_event)

            if not validation_passed and result.get("status") == "success":
                result["status"] = "partial_success"
                result["validation"] = False

            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在 save_data 后被取消")

            # 后处理
            await self.post_execute(result, stop_event=stop_event)

            self.logger.info(f"任务执行完成: {result}")
            return result
            
        except asyncio.CancelledError:
            self.logger.warning(f"任务 {self.name} 被取消。")
            return self.handle_error(asyncio.CancelledError("任务被用户取消"))
        except Exception as e:
            self.logger.error(
                f"任务执行失败: 类型={type(e).__name__}, 错误={str(e)}", exc_info=True
            )
            return self.handle_error(e)

    # 新增：多表数据获取支持
    async def fetch_multiple_sources(self, source_configs, **kwargs):
        """支持从多个表获取数据，为processor任务提供"""
        if len(source_configs) == 1:
            # 单源，使用现有逻辑
            return await self.fetch_data(**kwargs)
        else:
            # 多源，调用子类实现
            return await self._fetch_from_multiple_tables(source_configs, **kwargs)

    async def _fetch_from_multiple_tables(self, source_configs, **kwargs):
        """从多个表获取数据，子类可重写此方法实现具体的多表查询逻辑"""
        raise NotImplementedError("多源查询需要子类实现")

    async def _get_latest_date(self):
        """获取数据库中该表的最新数据日期"""
        if not self.table_name or not self.date_column:
            return None

        try:
            query = f"SELECT MAX({self.date_column}) FROM {self.table_name}"
            result = await self.db.fetch_one(query)

            if result and result[0]:
                latest_date = result[0]
                if isinstance(latest_date, str):
                    if "-" in latest_date:
                        latest_date = datetime.strptime(
                            latest_date, "%Y-%m-%d"
                        ).strftime("%Y%m%d")
                else:
                    latest_date = latest_date.strftime("%Y%m%d")

                return latest_date
        except Exception as e:
            self.logger.warning(f"获取最新日期失败: {str(e)}")

        return None

    async def pre_execute(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行前的准备工作"""
        pass

    async def post_execute(self, result, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """任务执行后的清理工作"""
        pass

    def handle_error(self, error):
        """处理任务执行过程中的错误"""
        if isinstance(error, asyncio.CancelledError):
            return {"status": "cancelled", "error": str(error), "task": self.name}
        return {"status": "error", "error": str(error), "task": self.name}

    @abstractmethod
    async def fetch_data(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """获取原始数据（子类必须实现）"""
        raise NotImplementedError

    def process_data(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """处理原始数据"""
        # 支持数据转换
        if hasattr(self, "transformations") and self.transformations:
            # 检查data是否为DataFrame（processor可能返回字典）
            if isinstance(data, pd.DataFrame):
                for column, transform in self.transformations.items():
                    if column in data.columns:
                        try:
                            data[column] = data[column].fillna(np.nan)
                            has_nan = data[column].isna().any()

                            if has_nan:
                                self.logger.debug(f"列 {column} 包含缺失值(NaN)，将使用安全转换")

                                def safe_transform(x):
                                    if pd.isna(x):
                                        return np.nan
                                    try:
                                        return transform(x)
                                    except Exception as e:
                                        self.logger.warning(f"转换值 {x} 时发生错误: {str(e)}")
                                        return np.nan

                                data[column] = data[column].apply(safe_transform)
                            else:
                                try:
                                    data[column] = data[column].apply(transform)
                                except Exception as e:
                                    self.logger.error(f"应用转换函数到列 {column} 时发生错误: {str(e)}")
                        except Exception as e:
                            self.logger.error(f"处理列 {column} 时发生错误: {str(e)}")

        return data

    def validate_data(self, data):
        """验证数据有效性"""
        if hasattr(self, "validations") and self.validations:
            try:
                for validation_func in self.validations:
                    if callable(validation_func):
                        if not validation_func(data):
                            self.logger.warning(f"数据验证失败: {validation_func}")
                            return False
                    else:
                        self.logger.warning(f"无效的验证函数: {validation_func}")
            except Exception as e:
                self.logger.error(f"数据验证过程中出错: {str(e)}")
                return False

        return True

    async def save_data(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """将处理后的数据保存到数据库"""
        await self._ensure_table_exists(stop_event=stop_event)
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 _ensure_table_exists 后被取消")

        # 检查数据中的NaN值
        if isinstance(data, pd.DataFrame):
            nan_count = data.isna().sum().sum()
            if nan_count > 0:
                self.logger.warning(f"数据中包含 {nan_count} 个NaN值，这些值在保存到数据库时将被转换为NULL")
                cols_with_nan = data.columns[data.isna().any()].tolist()
                if cols_with_nan:
                    self.logger.debug(f"包含NaN值的列: {', '.join(cols_with_nan)}")

        # 将数据保存到数据库
        affected_rows = await self._save_to_database(data, stop_event=stop_event)
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 _save_to_database 后被取消")

        return {"status": "success", "table": self.table_name, "rows": affected_rows}

    async def _ensure_table_exists(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """确保表存在，如果不存在则创建"""
        query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{self.table_name}'
        );
        """
        result = await self.db.fetch_one(query)
        table_exists = result[0] if result else False

        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在检查表存在后被取消")

        if not table_exists and hasattr(self, "schema"):
            self.logger.info(f"表 {self.table_name} 不存在，正在创建...")
            await self._create_table(stop_event=stop_event)

    async def _create_table(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """创建数据表"""
        if not hasattr(self, "schema") or not self.schema:
            raise ValueError(f"无法创建表 {self.table_name}，未定义schema")

        # 构建创建表的SQL语句
        columns = []
        for col_name, col_def in self.schema.items():
            if isinstance(col_def, dict):
                col_type = col_def.get("type", "TEXT")
                constraints_val = col_def.get("constraints")
                constraints_str = str(constraints_val).strip() if constraints_val is not None else ""
                columns.append(f"{col_name} {col_type} {constraints_str}".strip())
            else:
                columns.append(f"{col_name} {col_def}")

        # 如果配置自动添加更新时间，且schema中没有定义update_time列
        if self.auto_add_update_time and "update_time" not in self.schema:
            columns.append("update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        # 添加主键约束
        if self.primary_keys:
            primary_key_clause = f"PRIMARY KEY ({', '.join(self.primary_keys)})"
            columns.append(primary_key_clause)

        columns_str = ",\n            ".join(columns)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {columns_str}
        );
        """

        await self.db.execute(create_table_sql)
        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 CREATE TABLE 后被取消")
        self.logger.info(f"表 {self.table_name} 创建成功")

        await self._create_indexes(stop_event=stop_event)

    async def _create_indexes(self, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """创建必要的索引"""
        # 如果定义了日期列，为其创建索引
        if self.date_column and self.date_column not in self.primary_keys:
            index_name = f"idx_{self.table_name}_{self.date_column}"
            create_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {index_name} 
            ON {self.table_name} ({self.date_column});
            """
            await self.db.execute(create_index_sql)
            if stop_event and stop_event.is_set():
                raise asyncio.CancelledError("任务在创建日期索引后被取消")
            self.logger.info(f"为表 {self.table_name} 的 {self.date_column} 列创建索引")

        # 如果定义了自定义索引，也创建它们
        if hasattr(self, "indexes") and self.indexes:
            for i, index_def in enumerate(self.indexes):
                if stop_event and stop_event.is_set():
                    raise asyncio.CancelledError(f"任务在创建第 {i+1} 个自定义索引前被取消")
                
                if isinstance(index_def, dict):
                    index_name = index_def.get("name", f"idx_{self.table_name}_{index_def['columns'].replace(',', '_')}")
                    columns = index_def["columns"]
                    unique = "UNIQUE " if index_def.get("unique", False) else ""
                else:
                    index_name = f"idx_{self.table_name}_{index_def}"
                    columns = index_def
                    unique = ""

                create_index_sql = f"""
                CREATE {unique}INDEX IF NOT EXISTS {index_name} 
                ON {self.table_name} ({columns});
                """
                await self.db.execute(create_index_sql)
                self.logger.info(f"为表 {self.table_name} 的 {columns} 列创建索引")

    async def _save_to_database(self, data, stop_event: Optional[asyncio.Event] = None, **kwargs):
        """将DataFrame保存到数据库"""
        if isinstance(data, pd.DataFrame) and data.empty:
            return 0

        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在调用 upsert 前被取消")

        affected_rows = await self.db.upsert(
            table_name=self.table_name,
            data=data,
            conflict_columns=self.primary_keys,
            update_columns=None,
            timestamp_column="update_time",
            stop_event=stop_event,
        )

        if stop_event and stop_event.is_set():
            raise asyncio.CancelledError("任务在 upsert 调用后被取消")

        return affected_rows

    async def smart_incremental_update(
        self,
        stop_event: Optional[asyncio.Event] = None,
        lookback_days=None,
        end_date=None,
        use_trade_days=False,
        safety_days=1,
        **kwargs,
    ):
        """智能增量更新方法"""
        self.logger.info(f"开始执行 {self.name} 的智能增量更新")

        if stop_event and stop_event.is_set():
            self.logger.warning(f"智能增量更新 {self.name} 在开始前被取消。")
            raise asyncio.CancelledError("智能增量更新在开始前被取消")

        # 确定结束日期
        if end_date is None:
            current_time = datetime.now()
            if current_time.hour >= 18:
                end_date = current_time.strftime("%Y%m%d")
                self.logger.info(f"未指定结束日期，当前时间晚于18:00，使用当前日期: {end_date}")
            else:
                yesterday = current_time - timedelta(days=1)
                end_date = yesterday.strftime("%Y%m%d")
                self.logger.info(f"未指定结束日期，当前时间早于18:00，使用昨天日期: {end_date}")

        # 获取数据库中最新的数据日期
        latest_date = await self._get_latest_date()
        start_date = None

        if use_trade_days:
            # 使用交易日模式 - 需要从原fetchers模块导入
            try:
                from ...fetchers.tools.calendar import get_last_trade_day, get_next_trade_day

                if lookback_days is not None and lookback_days > 0:
                    try:
                        start_date = await get_last_trade_day(end_date, n=lookback_days)
                        self.logger.info(f"按回溯 {lookback_days} 个交易日计算，起始日期为: {start_date}")
                    except Exception as e:
                        self.logger.error(f"计算交易日回溯失败: {str(e)}")
                        return {"status": "error", "error": f"交易日计算失败: {str(e)}"}
                elif latest_date:
                    try:
                        start_date = await get_next_trade_day(latest_date)
                        self.logger.info(f"数据库最新日期: {latest_date}，下一个交易日: {start_date}")
                    except Exception as e:
                        self.logger.error(f"计算下一个交易日失败: {str(e)}")
                        return {"status": "error", "error": f"交易日计算失败: {str(e)}"}
                else:
                    start_date = self.default_start_date if hasattr(self, 'default_start_date') else "20200101"
                    self.logger.info(f"无历史数据，使用默认起始日期: {start_date}")
            except ImportError:
                self.logger.error("无法导入交易日历工具，回退到自然日模式")
                use_trade_days = False

        if not use_trade_days:
            # 使用自然日模式
            if lookback_days is not None and lookback_days > 0:
                end_date_obj = datetime.strptime(end_date, "%Y%m%d")
                start_date_obj = end_date_obj - timedelta(days=lookback_days + safety_days)
                start_date = start_date_obj.strftime("%Y%m%d")
                self.logger.info(f"按回溯 {lookback_days} 天计算（含安全天数 {safety_days}），起始日期为: {start_date}")
            elif latest_date:
                latest_date_obj = datetime.strptime(latest_date, "%Y%m%d")
                start_date_obj = latest_date_obj + timedelta(days=1) - timedelta(days=safety_days)
                start_date = start_date_obj.strftime("%Y%m%d")
                self.logger.info(f"数据库最新日期: {latest_date}，增量起始日期（含安全天数）: {start_date}")
            else:
                start_date = self.default_start_date if hasattr(self, 'default_start_date') else "20200101"
                self.logger.info(f"无历史数据，使用默认起始日期: {start_date}")

        # 检查日期有效性
        if start_date > end_date:
            self.logger.info(f"起始日期 {start_date} 晚于结束日期 {end_date}，无需更新")
            return {"status": "no_update", "message": "无需更新，数据已是最新"}

        # 执行增量更新
        self.logger.info(f"执行增量更新: {start_date} 到 {end_date}")
        kwargs.update({"start_date": start_date, "end_date": end_date})
        return await self.execute(stop_event=stop_event, **kwargs)


# 为了向后兼容，创建Task的别名
Task = BaseTask 