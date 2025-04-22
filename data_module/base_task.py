import asyncio
import logging
import pandas as pd
import numpy as np  # 添加numpy导入
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Any, Callable, Optional, Union

class Task(ABC):
    """数据任务基类"""
    
    # 必须由子类定义的属性
    name = None
    table_name = None
    
    # 可选属性（有合理默认值）
    primary_keys = []
    date_column = None
    description = ""
    auto_add_update_time = True  # 是否自动添加更新时间
    
    def __init__(self, db_connection):
        """初始化任务"""
        if self.name is None or self.table_name is None:
            raise ValueError("必须定义name和table_name属性")
        
        self.db = db_connection
        self.logger = logging.getLogger(f"task.{self.name}")
    
    async def execute(self, **kwargs):
        """执行任务的完整生命周期"""
        self.logger.info(f"开始执行任务: {self.name}")
        
        try:
            await self.pre_execute()
            
            # 获取数据
            self.logger.info(f"获取数据，参数: {kwargs}")
            data = await self.fetch_data(**kwargs)
            
            if data is None or data.empty:
                self.logger.info("没有获取到数据")
                return {"status": "no_data", "rows": 0}
            
            # 处理数据
            self.logger.info(f"处理数据，共 {len(data)} 行")
            data = self.process_data(data)
            
            # 再次检查处理后的数据是否为空
            if data is None or data.empty:
                self.logger.warning("数据处理后为空")
                return {"status": "no_data", "rows": 0}
            
            # 验证数据
            self.logger.info("验证数据")
            validation_passed = self.validate_data(data)
            # 记录验证结果，但不中断执行流程
            if not validation_passed:
                self.logger.warning("数据验证未通过，但将继续保存")
            
            # 保存数据
            self.logger.info(f"保存数据到表 {self.table_name}")
            result = await self.save_data(data)
            
            # 如果验证未通过但成功保存，则标记为部分成功
            if not validation_passed and result.get("status") == "success":
                result["status"] = "partial_success"
                result["validation"] = False
            
            # 后处理
            await self.post_execute(result)
            
            self.logger.info(f"任务执行完成: {result}")
            return result
        except Exception as e:
            self.logger.error(f"任务执行失败: {str(e)}", exc_info=True)
            return self.handle_error(e)
    
    async def _get_latest_date(self):
        """获取数据库中该表的最新数据日期"""
        if not self.table_name or not self.date_column:
            return None
        
        try:
            # 查询最新日期
            query = f"SELECT MAX({self.date_column}) FROM {self.table_name}"
            result = await self.db.fetch_one(query)
            
            if result and result[0]:
                # 格式化日期为YYYYMMDD格式
                latest_date = result[0]
                if isinstance(latest_date, str):
                    # 如果已经是字符串，尝试标准化格式
                    if '-' in latest_date:
                        # 假设格式为YYYY-MM-DD
                        latest_date = datetime.strptime(latest_date, '%Y-%m-%d').strftime('%Y%m%d')
                else:
                    # 如果是日期对象，转换为字符串
                    latest_date = latest_date.strftime('%Y%m%d')
                
                return latest_date
        except Exception as e:
            self.logger.warning(f"获取最新日期失败: {str(e)}")
        
        return None
    
    async def pre_execute(self):
        """任务执行前的准备工作"""
        # 可以在子类中重写该方法来添加自定义的准备工作
        pass
    
    async def post_execute(self, result):
        """任务执行后的清理工作"""
        # 可以在子类中重写该方法来添加自定义的清理工作
        pass
    
    def handle_error(self, error):
        """处理任务执行过程中的错误"""
        return {
            "status": "error",
            "error": str(error),
            "task": self.name
        }
    
    @abstractmethod
    async def fetch_data(self, **kwargs):
        """获取原始数据（子类必须实现）"""
        raise NotImplementedError
    
    def process_data(self, data):
        """处理原始数据"""
        if hasattr(self, 'transformations') and self.transformations:
            for column, transform in self.transformations.items():
                if column in data.columns:
                    try:
                        # 先将所有None值转换为np.nan，确保一致的缺失值处理
                        data[column] = data[column].fillna(np.nan)
                        
                        # 检查列是否包含nan值
                        has_nan = data[column].isna().any()
                        
                        if has_nan:
                            self.logger.debug(f"列 {column} 包含缺失值(NaN)，将使用安全转换")
                            # 定义一个安全的转换函数，处理np.nan值
                            def safe_transform(x):
                                if pd.isna(x):  # 检查是否为np.nan
                                    return np.nan  # 保持np.nan不变
                                try:
                                    return transform(x)  # 应用原始转换函数
                                except Exception as e:
                                    self.logger.warning(f"转换值 {x} 时发生错误: {str(e)}")
                                    return np.nan  # 转换失败时返回np.nan
                            
                            # 使用安全转换函数
                            data[column] = data[column].apply(safe_transform)
                        else:
                            # 没有np.nan值，直接应用转换函数，但仍然捕获可能的异常
                            try:
                                data[column] = data[column].apply(transform)
                            except Exception as e:
                                self.logger.error(f"应用转换函数到列 {column} 时发生错误: {str(e)}")
                                # 不中断处理，继续处理其他列
                    except Exception as e:
                        self.logger.error(f"处理列 {column} 时发生错误: {str(e)}")
                        # 不中断处理，继续处理其他列
        
        return data
    
    def validate_data(self, data):
        """验证数据有效性
        
        如果验证失败，会返回False，但不会抛出异常，以允许数据处理继续进行。
        开发者可以根据需要在子类中重写此方法，实现更严格的验证策略。
        
        Args:
            data (pd.DataFrame): 要验证的数据
            
        Returns:
            bool: 验证结果，True表示验证通过，False表示验证失败
        """
        if not hasattr(self, 'validations') or not self.validations:
            return True
        
        validation_passed = True
        
        for validator in self.validations:
            try:
                # 检查验证器中是否处理了np.nan值
                validator_name = validator.__name__ if hasattr(validator, '__name__') else '未命名验证器'
                self.logger.debug(f"执行验证器: {validator_name}")
                
                validation_result = validator(data)
                if isinstance(validation_result, bool) and not validation_result:
                    self.logger.warning(f"数据验证失败: {validator_name}")
                    validation_passed = False
                    # 不立即退出，继续运行其他验证器以获取更多信息
            except Exception as e:
                self.logger.error(f"执行验证器时发生错误: {str(e)}")
                validation_passed = False
        
        if not validation_passed:
            self.logger.warning("数据验证未完全通过，但将继续处理")
            
            # 记录包含NaN值的列，帮助诊断
            nan_columns = data.columns[data.isna().any()].tolist()
            if nan_columns:
                self.logger.info(f"以下列包含NaN值，可能影响验证结果: {', '.join(nan_columns)}")
        
        return validation_passed
    
    async def save_data(self, data):
        """将处理后的数据保存到数据库"""
        await self._ensure_table_exists()
        
        # 检查数据中的NaN值
        nan_count = data.isna().sum().sum()
        if nan_count > 0:
            self.logger.warning(f"数据中包含 {nan_count} 个NaN值，这些值在保存到数据库时将被转换为NULL")
            
            # 查看哪些列包含NaN值
            cols_with_nan = data.columns[data.isna().any()].tolist()
            if cols_with_nan:
                self.logger.debug(f"包含NaN值的列: {', '.join(cols_with_nan)}")
        
        # 将数据保存到数据库
        affected_rows = await self._save_to_database(data)
        
        return {
            "status": "success",
            "table": self.table_name,
            "rows": affected_rows
        }
    
    async def _ensure_table_exists(self):
        """确保表存在，如果不存在则创建"""
        # 检查表是否存在
        query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{self.table_name}'
        );
        """
        result = await self.db.fetch_one(query)
        table_exists = result[0] if result else False
        
        if not table_exists and hasattr(self, 'schema'):
            # 如果表不存在且定义了schema，则创建表
            self.logger.info(f"表 {self.table_name} 不存在，正在创建...")
            await self._create_table()
    
    async def _create_table(self):
        """创建数据表"""
        if not hasattr(self, 'schema') or not self.schema:
            raise ValueError(f"无法创建表 {self.table_name}，未定义schema")
        
        # 构建创建表的SQL语句
        columns = []
        for col_name, col_def in self.schema.items():
            if isinstance(col_def, dict):
                col_type = col_def.get('type', 'TEXT')
                constraints = col_def.get('constraints', '')
                columns.append(f"{col_name} {col_type} {constraints}")
            else:
                columns.append(f"{col_name} {col_def}")
        
        # 如果配置自动添加更新时间，且schema中没有定义update_time列
        if self.auto_add_update_time and 'update_time' not in self.schema:
            columns.append("update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        # 添加主键约束
        if self.primary_keys:
            primary_key_clause = f"PRIMARY KEY ({', '.join(self.primary_keys)})"
            columns.append(primary_key_clause)
        
        create_table_sql = f"""
        CREATE TABLE {self.table_name} (
            {',\n            '.join(columns)}
        );
        """
        
        # 执行创建表的SQL
        await self.db.execute(create_table_sql)
        self.logger.info(f"表 {self.table_name} 创建成功")
        
        # 创建索引
        await self._create_indexes()
    
    async def _create_indexes(self):
        """创建必要的索引"""
        # 如果定义了日期列，为其创建索引
        if self.date_column and self.date_column not in self.primary_keys:
            index_name = f"idx_{self.table_name}_{self.date_column}"
            create_index_sql = f"""
            CREATE INDEX IF NOT EXISTS {index_name} 
            ON {self.table_name} ({self.date_column});
            """
            await self.db.execute(create_index_sql)
            self.logger.info(f"为表 {self.table_name} 的 {self.date_column} 列创建索引")
        
        # 如果定义了自定义索引，也创建它们
        if hasattr(self, 'indexes') and self.indexes:
            for index_def in self.indexes:
                if isinstance(index_def, dict):
                    index_name = index_def.get('name', f"idx_{self.table_name}_{index_def['columns'].replace(',', '_')}")
                    columns = index_def['columns']
                    unique = "UNIQUE " if index_def.get('unique', False) else ""
                else:
                    # 如果只是字符串，则假设它是列名
                    index_name = f"idx_{self.table_name}_{index_def}"
                    columns = index_def
                    unique = ""
                
                create_index_sql = f"""
                CREATE {unique}INDEX IF NOT EXISTS {index_name} 
                ON {self.table_name} ({columns});
                """
                await self.db.execute(create_index_sql)
                self.logger.info(f"为表 {self.table_name} 的 {columns} 列创建索引")
    
    async def _save_to_database(self, data):
        """将DataFrame保存到数据库"""
        if data.empty:
            return 0
        
        # 这里不再需要手动转换和处理数据，直接使用db_manager的upsert方法
        timestamp_column = 'update_time' if self.auto_add_update_time else None
        
        affected_rows = await self.db.upsert(
            table_name=self.table_name,
            data=data,
            conflict_columns=self.primary_keys,
            update_columns=None,  # 自动更新所有非冲突列
            timestamp_column=timestamp_column
        )
        
        return affected_rows

    async def smart_incremental_update(self, lookback_days=None, end_date=None, use_trade_days=False, safety_days=1, **kwargs):
        """智能增量更新方法
        
        整合了基于自然日和交易日的增量更新功能。可以根据需要选择使用自然日或交易日进行更新。
        
        Args:
            lookback_days (int, optional): 回溯的天数。
                                         如果 use_trade_days=True，表示回溯的交易日数。
                                         如果 use_trade_days=False，表示回溯的自然日数。
            end_date (str, optional): 更新的结束日期 (YYYYMMDD)，默认为当前日期。
            use_trade_days (bool): 是否使用交易日计算。
                                 True: 使用交易日计算（通过交易日历）
                                 False: 使用自然日计算（通过timedelta）
            safety_days (int): 安全天数，为了确保数据连续性，会额外回溯的天数。
                             仅在 use_trade_days=False 时使用。
            **kwargs: 传递给 execute 方法的其他参数。
            
        Returns:
            Dict[str, Any]: 执行结果，包含新增数据行数和状态信息
        """
        self.logger.info(f"开始执行 {self.name} 的智能增量更新")
        
        # 确定结束日期
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
            self.logger.info(f"未指定结束日期，使用当前日期: {end_date}")
            
        # 获取数据库中最新的数据日期
        latest_date = await self._get_latest_date()
        start_date = None
        
        if use_trade_days:
            # 使用交易日模式
            from .tools.calendar import get_last_trade_day, get_next_trade_day
            
            if lookback_days is not None and lookback_days > 0:
                # 按指定交易日回溯天数计算起始日期
                try:
                    start_date = await get_last_trade_day(end_date, n=lookback_days)
                    self.logger.info(f"按回溯 {lookback_days} 个交易日计算，起始日期为: {start_date}")
                except Exception as e:
                    self.logger.error(f"计算回溯 {lookback_days} 个交易日失败: {e}")
                    return self.handle_error(e)
            elif latest_date:
                try:
                    next_day = await get_next_trade_day(latest_date, n=1)
                    if next_day and next_day <= end_date:
                        start_date = next_day
                        self.logger.info(f"数据库最新日期为 {latest_date}，从下一个交易日 {start_date} 开始更新")
                    elif not next_day:
                        self.logger.info(f"无法确定 {latest_date} 的下一个交易日。数据可能已最新。")
                        return {"status": "up_to_date", "rows": 0}
                    else:  # next_day > end_date
                        self.logger.info(f"数据库最新日期 {latest_date} 之后无新交易日需要更新")
                        return {"status": "up_to_date", "rows": 0}
                except Exception as e:
                    self.logger.error(f"获取 {latest_date} 的下一个交易日时发生错误: {e}")
                    return self.handle_error(e)
            else:
                # 数据库无记录，获取最近30个交易日
                try:
                    start_date = await get_last_trade_day(end_date, n=30)
                    self.logger.info(f"数据库无记录，默认更新最近30个交易日，起始日期: {start_date}")
                except Exception as e:
                    self.logger.error(f"计算最近30个交易日失败: {e}")
                    return self.handle_error(e)
        else:
            # 使用自然日模式
            if latest_date:
                # 将最新日期转换为datetime对象
                latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                
                if lookback_days is not None:
                    # 使用指定的回溯天数
                    start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=lookback_days)
                else:
                    # 从最新日期开始，考虑安全天数
                    start_dt = latest_dt - timedelta(days=safety_days)
                
                start_date = start_dt.strftime('%Y%m%d')
                
                # 检查是否需要更新
                if datetime.strptime(end_date, '%Y%m%d') <= latest_dt and safety_days == 0:
                    self.logger.info(f"数据库中已有最新数据（{latest_date}），无需更新")
                    return {"status": "up_to_date", "rows": 0}
            else:
                # 数据库中没有数据，则从30天前开始获取
                start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=30)
                start_date = start_dt.strftime('%Y%m%d')
                self.logger.info(f"数据库中没有数据，将从 {start_date} 开始获取")
        
        # 执行更新
        if start_date and end_date:
            self.logger.info(f"最终执行更新范围: {start_date} 到 {end_date}")
            kwargs['start_date'] = start_date
            kwargs['end_date'] = end_date
            result = await self.execute(**kwargs)
            
            if result.get("status") == "no_data":
                self.logger.info("没有新数据需要更新")
            else:
                self.logger.info(f"增量更新完成: 新增/更新 {result.get('rows', 0)} 行数据")
            
            return result
        else:
            self.logger.warning("未能确定有效的更新日期范围，任务未执行")
            return {"status": "no_range", "rows": 0}
