#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
完整的端到端示例
演示如何使用三层架构创建和执行数据处理任务
"""

import asyncio
import pandas as pd
import numpy as np
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath('../../..'))

from alphahome.processors import Operation, OperationPipeline, ProcessorTaskBase, ProcessorEngine, task_register
from alphahome.common.db_manager import DBManager
from alphahome.common.task_system import UnifiedTaskFactory
from alphahome.common.config_manager import get_database_url


# 步骤1: 创建自定义操作
class DataCleaningOperation(Operation):
    """数据清洗操作：移除空值和异常值"""
    
    def __init__(self, name: str = "DataCleaning"):
        super().__init__(name)
    
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """执行数据清洗"""
        result = data.copy()
        
        # 移除空值
        original_count = len(result)
        result = result.dropna()
        
        # 移除异常值（示例：移除超出3个标准差的值）
        if 'value' in result.columns:
            mean = result['value'].mean()
            std = result['value'].std()
            result = result[abs(result['value'] - mean) <= 3 * std]
        
        self.logger.info(f"数据清洗完成：{original_count} -> {len(result)} 行")
        return result


class FeatureEngineeringOperation(Operation):
    """特征工程操作：创建新特征"""
    
    def __init__(self, name: str = "FeatureEngineering"):
        super().__init__(name)
    
    async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """执行特征工程"""
        result = data.copy()
        
        # 创建新特征
        if 'value' in result.columns:
            result['value_squared'] = result['value'] ** 2
            result['value_log'] = result['value'].apply(lambda x: np.log(x) if x > 0 else 0)
            result['value_normalized'] = (result['value'] - result['value'].mean()) / result['value'].std()
        
        new_features = len(result.columns) - len(data.columns)
        self.logger.info(f"特征工程完成，新增 {new_features} 个特征")
        return result


# 步骤2: 创建自定义任务
@task_register()
class CompleteExampleTask(ProcessorTaskBase):
    """完整示例任务"""
    
    name = "complete_example_task"
    table_name = "complete_example_result"
    description = "完整的数据处理示例任务"
    
    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        """获取数据"""
        # 生成模拟数据
        np.random.seed(42)  # 确保结果可重现
        
        data = pd.DataFrame({
            'id': range(1, 101),
            'value': np.random.normal(100, 15, 100),
            'category': np.random.choice(['A', 'B', 'C'], 100),
            'timestamp': pd.date_range('2023-01-01', periods=100, freq='D')
        })
        
        # 人为添加一些空值和异常值
        data.loc[5:10, 'value'] = np.nan  # 添加空值
        data.loc[95, 'value'] = 1000  # 添加异常值
        
        self.logger.info(f"获取到 {len(data)} 行模拟数据")
        return data
    
    async def process_data(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """处理数据"""
        # 创建操作流水线
        pipeline = OperationPipeline("CompleteExamplePipeline")
        
        # 添加操作到流水线
        pipeline.add_operation(DataCleaningOperation())
        pipeline.add_operation(FeatureEngineeringOperation())
        
        # 执行流水线
        processed_data = await pipeline.apply(data, **kwargs)
        
        self.logger.info(f"数据处理完成，最终数据形状: {processed_data.shape}")
        return processed_data
    
    async def save_result(self, data: pd.DataFrame, **kwargs):
        """保存结果"""
        # 在实际项目中，这里会保存到数据库
        # 这里只是打印结果作为示例
        self.logger.info(f"保存 {len(data)} 行数据到表 {self.table_name}")
        
        print(f"\n=== 处理结果预览 ===")
        print(f"数据形状: {data.shape}")
        print(f"列名: {data.columns.tolist()}")
        print("\n前5行数据:")
        print(data.head())
        
        print(f"\n数据统计:")
        print(data.describe())


# 步骤3: 执行示例
async def run_complete_example():
    """运行完整示例"""
    print("=== AlphaHome 数据处理模块完整示例 ===\n")
    
    db_manager = None
    engine = None
    
    try:
        # 1. 初始化数据库连接（如果有配置）
        db_url = get_database_url()
        if db_url:
            print("使用配置的数据库连接...")
            db_manager = DBManager(db_url)
            await db_manager.connect()
        else:
            print("未配置数据库，使用模拟数据库管理器...")
            # 创建一个模拟的数据库管理器用于演示
            class MockDBManager:
                async def connect(self): pass
                async def close(self): pass
            db_manager = MockDBManager()
        
        # 2. 初始化任务工厂
        if db_url:
            await UnifiedTaskFactory.initialize(db_url=db_url)
        else:
            # 模拟初始化
            print("模拟任务工厂初始化...")
        
        # 3. 创建处理引擎
        engine = ProcessorEngine(db_manager=db_manager, max_workers=2)
        
        # 4. 执行任务
        print("开始执行完整示例任务...")
        result = await engine.execute_task("complete_example_task")
        
        # 5. 检查结果
        print(f"\n=== 执行结果 ===")
        print(f"任务状态: {result['status']}")
        print(f"处理行数: {result.get('rows', 0)}")
        
        if 'engine_metadata' in result:
            metadata = result['engine_metadata']
            print(f"执行时间: {metadata.get('execution_time', 0):.2f}秒")
            print(f"开始时间: {metadata.get('start_time', 'N/A')}")
            print(f"结束时间: {metadata.get('end_time', 'N/A')}")
        
        # 6. 显示引擎统计
        if hasattr(engine, 'get_stats'):
            stats = engine.get_stats()
            print(f"\n=== 引擎统计 ===")
            print(f"总任务数: {stats.get('total_tasks', 0)}")
            print(f"成功任务数: {stats.get('successful_tasks', 0)}")
            print(f"失败任务数: {stats.get('failed_tasks', 0)}")
            print(f"成功率: {stats.get('success_rate', 0):.2%}")
        
        print(f"\n🎉 完整示例执行成功！")
        
    except Exception as e:
        print(f"❌ 示例执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # 7. 清理资源
        if engine and hasattr(engine, 'shutdown'):
            engine.shutdown()
            print("引擎已关闭")
        
        if db_manager and hasattr(db_manager, 'close'):
            await db_manager.close()
            print("数据库连接已关闭")
    
    return True


if __name__ == "__main__":
    print("运行AlphaHome数据处理模块完整示例...")
    success = asyncio.run(run_complete_example())
    
    if success:
        print("\n✅ 示例运行成功！")
        print("\n📚 更多信息请参考:")
        print("- README.md: 详细的开发指南")
        print("- usage_example.py: 基础使用示例")
        print("- 项目文档: 完整的API文档")
    else:
        print("\n❌ 示例运行失败，请检查错误信息")
    
    sys.exit(0 if success else 1)
