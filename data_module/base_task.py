import asyncio
import logging
import pandas as pd
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
            
            # 验证数据
            self.logger.info("验证数据")
            self.validate_data(data)
            
            # 保存数据
            self.logger.info(f"保存数据到表 {self.table_name}")
            result = await self.save_data(data)
            
            # 后处理
            await self.post_execute(result)
            
            self.logger.info(f"任务执行完成: {result}")
            return result
        except Exception as e:
            self.logger.error(f"任务执行失败: {str(e)}", exc_info=True)
            return self.handle_error(e)
    
    async def full_update(self, **kwargs):
        """
        执行全量数据更新
        
        这是一个快捷方法，用于获取并存储所有可用数据，不指定日期范围
        
        Args:
            **kwargs: 传递给fetch_data的额外参数
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"开始执行{self.name}的全量更新...")
        result = await self.execute(**kwargs)
        self.logger.info(f"全量更新完成: {result}")
        return result
    
    async def incremental_update(self, days_lookback=None, safety_days=1, **kwargs):
        """
        执行增量数据更新
        
        智能增量更新方法，它会查询数据库中最新的数据日期，
        然后只获取该日期之后的新数据，避免不必要的数据重复获取。
        
        Args:
            days_lookback (int, optional): 向后兼容参数，如果指定则相当于设置safety_days
            safety_days (int): 安全天数，为了确保数据连续性，会额外回溯的天数，默认为1天
            **kwargs: 传递给fetch_data的额外参数
            
        Returns:
            Dict[str, Any]: 执行结果，包含新增数据行数和状态信息
        """
        # 向后兼容：如果指定了days_lookback，则使用它作为safety_days
        if days_lookback is not None:
            safety_days = days_lookback
            self.logger.info(f"使用days_lookback={days_lookback}作为safety_days参数（向后兼容）")
            
        # 获取数据库中最新的数据日期
        latest_date = await self._get_latest_date()
        
        # 计算当前日期
        current_date = datetime.now().strftime('%Y%m%d')
        
        if latest_date:
            # 将最新日期转换为datetime对象
            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
            
            # 计算开始日期（最新日期减去安全天数）
            start_dt = latest_dt - timedelta(days=safety_days)
            start_date = start_dt.strftime('%Y%m%d')
            
            # 检查是否需要更新（如果最新日期是今天，则可能不需要更新）
            current_dt = datetime.strptime(current_date, '%Y%m%d')
            days_diff = (current_dt - latest_dt).days
            
            if days_diff <= 0 and safety_days == 0:
                self.logger.info(f"数据库中已有最新数据（{latest_date}），无需更新")
                return {"status": "up_to_date", "rows": 0}
        else:
            # 如果数据库中没有数据，则从30天前开始获取（可根据需要调整）
            start_dt = datetime.now() - timedelta(days=30)
            start_date = start_dt.strftime('%Y%m%d')
            self.logger.info(f"数据库中没有数据，将从 {start_date} 开始获取")
        
        # 执行数据获取和保存
        self.logger.info(f"开始执行{self.name}的增量更新，日期范围: {start_date} 至 {current_date}...")
        result = await self.execute(start_date=start_date, end_date=current_date, **kwargs)
        
        # 记录结果
        if result.get("status") == "no_data":
            self.logger.info(f"没有新数据需要更新")
        else:
            self.logger.info(f"增量更新完成: 新增/更新 {result.get('rows', 0)} 行数据")
            
        return result
    
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
                    data[column] = data[column].apply(transform)
        
        return data
    
    def validate_data(self, data):
        """验证数据有效性"""
        if not hasattr(self, 'validations') or not self.validations:
            return True
        
        for validator in self.validations:
            validation_result = validator(data)
            if isinstance(validation_result, bool) and not validation_result:
                raise ValueError("数据验证失败")
        
        return True
    
    async def save_data(self, data):
        """将处理后的数据保存到数据库"""
        await self._ensure_table_exists()
        
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
        
        # 将DataFrame转换为记录列表
        records = data.to_dict('records')
        if not records:
            return 0
        
        # 构建插入语句
        columns = list(records[0].keys())
        # 使用PostgreSQL的$1, $2格式的参数占位符
        placeholders = ', '.join([f'${i+1}' for i in range(len(columns))])
        column_str = ', '.join(columns)
        
        # 构建 UPSERT 语句（INSERT ... ON CONFLICT DO UPDATE ...)
        if self.primary_keys:
            # 如果有主键，则使用UPSERT
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in self.primary_keys])
            if update_clause:
                upsert_sql = f"""
                INSERT INTO {self.table_name} ({column_str})
                VALUES ({placeholders})
                ON CONFLICT ({', '.join(self.primary_keys)}) 
                DO UPDATE SET {update_clause};
                """
            else:
                # 如果没有需要更新的列，则什么也不做
                upsert_sql = f"""
                INSERT INTO {self.table_name} ({column_str})
                VALUES ({placeholders})
                ON CONFLICT ({', '.join(self.primary_keys)}) 
                DO NOTHING;
                """
        else:
            # 如果没有主键，则使用普通的INSERT
            upsert_sql = f"""
            INSERT INTO {self.table_name} ({column_str})
            VALUES ({placeholders});
            """
        
        # 准备数据
        values = [[record.get(col) for col in columns] for record in records]
        
        # 执行插入
        affected_rows = await self.db.executemany(upsert_sql, values)
        
        return affected_rows
