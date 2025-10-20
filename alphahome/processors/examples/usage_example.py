#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
新架构使用示例

演示如何使用重构后的 `Engine -> Task -> Operation` 三层架构。
"""

import asyncio
import pandas as pd
from datetime import datetime

# 导入新架构的组件
from alphahome.processors import (
    ProcessorEngine,
    Operation,
    OperationPipeline,
    ProcessorTaskBase,
    task_register
)


# 示例1: 创建自定义操作 (Operation)
# Operation是可复用的、原子级的数据处理单元。
class DataCleaningOperation(Operation):
    """数据清洗操作示例"""
    
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """清洗数据"""
        if data.empty:
            return data
        
        result = data.copy()
        
        # 移除重复行
        original_len = len(result)
        result = result.drop_duplicates()
        
        # 填充缺失值
        result = result.ffill()
        
        self.logger.info(f"数据清洗完成，原始行数: {original_len}，清洗后: {len(result)}")
        return result


class FeatureEngineeringOperation(Operation):
    """特征工程操作示例"""
    
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
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


# 示例2: 创建自定义任务 (Task)
# Task是业务逻辑的封装单元，负责数据IO和编排Operations。
@task_register()
class ExampleProcessorTask(ProcessorTaskBase):
    """示例处理任务"""
    
    name = "example_processor"
    table_name = "example_result"
    description = "一个演示新架构的示例数据处理任务"
    
    source_tables = ["example_source"]
    
    async def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        在任务内部编排一个或多个操作来处理数据。
        """
        self.logger.info(f"开始处理任务 {self.name} 的数据...")
        
        # OperationPipeline是一个可选的辅助工具，用于串联多个操作。
        pipeline = OperationPipeline(name="ExampleInternalPipeline")
        pipeline.add_operation(DataCleaningOperation())
        pipeline.add_operation(FeatureEngineeringOperation())
        
        processed_data = await pipeline.apply(data)
        
        self.logger.info(f"任务 {self.name} 数据处理完成。")
        return processed_data
    
    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        """获取示例数据（实际应从数据库或文件读取）"""
        self.logger.info(f"为任务 {self.name} 获取示例数据...")
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
        
        self.logger.info(f"获取示例数据完成，行数: {len(data)}")
        return data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存结果（示例实现）"""
        self.logger.info(f"开始保存任务 {self.name} 的处理结果，行数: {len(data)}")
        # 在实际应用中，这里会通过 self.db_connection 保存到数据库
        print("处理结果预览:")
        print(data.head())
        self.logger.info(f"任务 {self.name} 的结果保存完成。")


# 示例3: 使用处理引擎 (Engine)
async def example_usage():
    """使用示例"""
    print("=== 新架构使用示例 ===\n")
    
    # 1. 单独使用 Operation
    print("--- 1. 单独使用原子操作 ---")
    sample_data = pd.DataFrame({
        'price': [10, 11, None, 12, 13, 11],
        'volume': [1000, 1100, 1200, 1300, 1400, 1100]
    })
    
    cleaning_op = DataCleaningOperation()
    cleaned_data = await cleaning_op.apply(sample_data)
    print(f"单独清洗操作后，数据行数: {len(cleaned_data)}\n")
    
    # 2. 使用 OperationPipeline 辅助工具
    print("--- 2. 使用操作流水线辅助工具 ---")
    op_pipeline = OperationPipeline("ExampleOperationPipeline")
    op_pipeline.add_operation(DataCleaningOperation())
    op_pipeline.add_operation(FeatureEngineeringOperation())
    
    result_df = await op_pipeline.apply(sample_data)
    print(f"操作流水线处理后，结果包含 'ma_5' 列: {'ma_5' in result_df.columns}\n")
    
    # 3. 使用处理引擎执行完整任务
    print("--- 3. 使用处理引擎执行一个完整的任务 ---")
    # 引擎负责调度和执行在系统中注册的任务
    engine = ProcessorEngine(max_workers=2)
    
    try:
        # 通过任务名称执行
        result = await engine.execute_task("example_processor")
        print(f"任务 'example_processor' 执行结果: {result['status']}")
        print(f"处理的总行数: {result.get('rows', 0)}\n")
        
        # 批量执行任务
        print("--- 4. 批量执行任务 ---")
        batch_config = { "example_processor": {} } # 可以为任务传递特定参数
        results = await engine.execute_batch(batch_config, parallel=True)
        
        for task_name, batch_result in results.items():
            print(f"批量任务 '{task_name}' 执行状态: {batch_result['status']}")
        
    finally:
        # 优雅地关闭引擎
        engine.shutdown()


if __name__ == "__main__":
    # 运行示例
    asyncio.run(example_usage())
