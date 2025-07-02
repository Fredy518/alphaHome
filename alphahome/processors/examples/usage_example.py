#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新架构使用示例

演示如何使用重构后的processors模块的分层架构。
"""

import asyncio
import pandas as pd
from datetime import datetime

# 导入新架构的组件
from alphahome.processors import (
    ProcessorEngine,
    ProcessingPipeline,
    Operation,
    OperationPipeline,
    ProcessorTaskBase,
    task_register
)


# 示例1: 创建自定义操作
class DataCleaningOperation(Operation):
    """数据清洗操作示例"""
    
    def __init__(self, config=None):
        super().__init__(name="DataCleaningOperation", config=config)
    
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """清洗数据"""
        if data.empty:
            return data
        
        result = data.copy()
        
        # 移除重复行
        original_len = len(result)
        result = result.drop_duplicates()
        
        # 填充缺失值
        result = result.fillna(method='forward')
        
        self.logger.info(f"数据清洗完成，原始行数: {original_len}，清洗后: {len(result)}")
        return result


class FeatureEngineeringOperation(Operation):
    """特征工程操作示例"""
    
    def __init__(self, config=None):
        super().__init__(name="FeatureEngineeringOperation", config=config)
    
    async def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """特征工程"""
        if data.empty:
            return data
        
        result = data.copy()
        
        # 假设数据有price列，计算一些技术指标
        if 'price' in result.columns:
            # 计算移动平均
            result['ma_5'] = result['price'].rolling(window=5).mean()
            result['ma_20'] = result['price'].rolling(window=20).mean()
            
            # 计算价格变化率
            result['price_change'] = result['price'].pct_change()
        
        self.logger.info(f"特征工程完成，新增特征列")
        return result


# 示例2: 创建自定义流水线
class ExampleDataPipeline(ProcessingPipeline):
    """示例数据处理流水线"""
    
    def build_pipeline(self):
        """构建流水线"""
        # 阶段1: 数据清洗
        self.add_stage(
            name="数据清洗",
            operations=DataCleaningOperation(config=self.config)
        )
        
        # 阶段2: 特征工程
        self.add_stage(
            name="特征工程",
            operations=FeatureEngineeringOperation(config=self.config)
        )


# 示例3: 创建自定义任务
@task_register()
class ExampleProcessorTask(ProcessorTaskBase):
    """示例处理任务"""
    
    name = "example_processor"
    table_name = "example_result"
    description = "示例数据处理任务"
    
    source_tables = ["example_source"]
    
    def create_pipeline(self) -> ProcessingPipeline:
        """创建处理流水线"""
        return ExampleDataPipeline(
            name="ExampleDataPipeline",
            config=self.get_pipeline_config()
        )
    
    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        """获取示例数据"""
        # 创建示例数据
        data = pd.DataFrame({
            'id': range(1, 101),
            'price': [10 + i * 0.1 + (i % 10) * 0.05 for i in range(100)],
            'volume': [1000 + i * 10 for i in range(100)],
            'date': pd.date_range('2024-01-01', periods=100)
        })
        
        # 添加一些缺失值和重复行用于测试清洗功能
        data.loc[10:15, 'price'] = None
        data = pd.concat([data, data.iloc[:5]], ignore_index=True)
        
        self.logger.info(f"获取示例数据，行数: {len(data)}")
        return data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存结果（示例实现）"""
        self.logger.info(f"保存处理结果，行数: {len(data)}")
        # 在实际应用中，这里会保存到数据库
        print("处理结果预览:")
        print(data.head())


async def example_usage():
    """使用示例"""
    print("=== 新架构使用示例 ===\n")
    
    # 示例1: 直接使用操作
    print("1. 直接使用操作")
    sample_data = pd.DataFrame({
        'price': [10, 11, None, 12, 13],
        'volume': [1000, 1100, 1200, 1300, 1400]
    })
    
    cleaning_op = DataCleaningOperation()
    cleaned_data = await cleaning_op.execute(sample_data)
    print(f"清洗后数据行数: {len(cleaned_data['data'])}")
    print()
    
    # 示例2: 使用操作流水线
    print("2. 使用操作流水线")
    op_pipeline = OperationPipeline("ExampleOperationPipeline")
    op_pipeline.add_operation(DataCleaningOperation())
    op_pipeline.add_operation(FeatureEngineeringOperation())
    
    result = await op_pipeline.execute(sample_data)
    print(f"操作流水线处理结果: {result['status']}")
    print()
    
    # 示例3: 使用处理流水线
    print("3. 使用处理流水线")
    pipeline = ExampleDataPipeline(name="ExamplePipeline")
    result = await pipeline.execute(sample_data)
    print(f"处理流水线结果行数: {len(result['data'])}")
    print()
    
    # 示例4: 使用处理引擎执行任务
    print("4. 使用处理引擎执行任务")
    engine = ProcessorEngine(max_workers=2)
    
    try:
        result = await engine.execute_task("example_processor")
        print(f"任务执行结果: {result['status']}")
        print(f"处理行数: {result.get('rows', 0)}")
        
        # 获取引擎统计信息
        stats = engine.get_stats()
        print(f"引擎统计: 总任务数={stats['total_tasks']}, 成功率={stats['success_rate']:.2%}")
        
    finally:
        engine.shutdown()
    
    print()
    
    # 示例5: 批量执行任务
    print("5. 批量执行任务")
    engine = ProcessorEngine(max_workers=2)
    
    try:
        # 批量执行配置
        batch_config = {
            "example_processor": {"param1": "value1"},
            # 可以添加更多任务
        }
        
        results = await engine.execute_batch(batch_config, parallel=True)
        
        for task_name, result in results.items():
            print(f"任务 {task_name}: {result['status']}")
        
    finally:
        engine.shutdown()


if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())
