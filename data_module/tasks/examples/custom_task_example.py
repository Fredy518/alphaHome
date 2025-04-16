import pandas as pd
from typing import Dict, List
from datetime import datetime

from ...base_task import Task
from ...task_decorator import task_register

@task_register("custom_example_task")  # 显式指定任务名称
class CustomExampleTask(Task):
    """自定义示例任务
    
    这是一个演示如何使用装饰器注册任务的示例类。
    此示例展示了如何创建一个自定义任务并使用装饰器将其注册到任务工厂。
    """
    
    # 核心属性
    name = "custom_example"
    description = "自定义示例任务，用于演示装饰器注册"
    table_name = "custom_example_data"
    primary_keys = ["id", "create_date"]
    date_column = "create_date"
    
    # 表结构定义
    schema = {
        "id": {"type": "SERIAL", "constraints": "PRIMARY KEY"},
        "create_date": {"type": "DATE", "constraints": "NOT NULL"},
        "data_value": {"type": "NUMERIC(10,2)"},
        "data_text": {"type": "VARCHAR(100)"}
    }
    
    async def fetch_data(self, **kwargs):
        """获取示例数据
        
        这里模拟一个数据获取过程，实际应用中应替换为真实的数据源。
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            DataFrame: 获取的数据
        """
        # 创建示例数据
        self.logger.info("生成示例数据...")
        
        # 获取当前日期作为示例数据日期
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 创建示例数据DataFrame
        data = pd.DataFrame({
            'id': range(1, 11),  # 10行示例数据
            'create_date': current_date,
            'data_value': [i * 1.5 for i in range(1, 11)],
            'data_text': [f'示例文本-{i}' for i in range(1, 11)]
        })
        
        self.logger.info(f"生成了 {len(data)} 行示例数据")
        return data
        
    def process_data(self, data):
        """处理数据
        
        对获取的数据进行处理，例如数据转换、清洗等。
        
        Args:
            data: 原始数据
            
        Returns:
            处理后的数据
        """
        self.logger.info("处理示例数据...")
        
        # 对数值字段进行处理（这里只是一个示例）
        if 'data_value' in data.columns:
            data['data_value'] = data['data_value'] * 2
            
        # 对文本字段进行处理
        if 'data_text' in data.columns:
            data['data_text'] = data['data_text'].apply(lambda x: x.upper())
            
        return data
        
    def validate_data(self, data):
        """验证数据
        
        验证处理后的数据是否符合要求。
        
        Args:
            data: 处理后的数据
            
        Returns:
            bool: 验证结果
        """
        self.logger.info("验证示例数据...")
        
        # 验证数值字段
        if 'data_value' in data.columns:
            if not all(data['data_value'] > 0):
                self.logger.warning("数据验证失败：data_value字段存在非正值")
                return False
                
        # 验证日期字段
        if self.date_column in data.columns:
            try:
                pd.to_datetime(data[self.date_column])
            except Exception as e:
                self.logger.warning(f"数据验证失败：日期字段格式错误 - {e}")
                return False
                
        return True

# 无需手动注册，装饰器已经完成注册操作
# TaskFactory.register_task('custom_example_task', CustomExampleTask)

# 使用不带参数的装饰器（将使用类的name属性作为任务名称）
@task_register
class AnotherExampleTask(Task):
    """另一个示例任务，使用不带参数的装饰器"""
    
    name = "another_example"
    description = "另一个示例任务，展示不带参数的装饰器用法"
    table_name = "another_example_data"
    primary_keys = ["id"]
    
    # 表结构定义
    schema = {
        "id": {"type": "SERIAL", "constraints": "PRIMARY KEY"},
        "name": {"type": "VARCHAR(50)", "constraints": "NOT NULL"},
        "value": {"type": "INTEGER"}
    }
    
    async def fetch_data(self, **kwargs):
        """获取示例数据"""
        # 简化的示例实现
        data = pd.DataFrame({
            'id': range(1, 6),
            'name': [f'name-{i}' for i in range(1, 6)],
            'value': [i * 10 for i in range(1, 6)]
        })
        return data 