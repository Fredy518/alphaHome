#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试重构后的三层架构
"""

import asyncio
import pandas as pd
import sys
import os
from alphahome.common.db_manager import DBManager
from alphahome.common.task_system import UnifiedTaskFactory, task_register
from alphahome.processors import ProcessorEngine, ProcessorTaskBase
from alphahome.common.config_manager import get_database_url # 假设您有这个

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath('.'))

async def test_architecture():
    """测试三层架构的基本功能"""
    print("=== 测试重构后的三层架构 ===\n")
    
    try:
        # 1. 测试导入
        print("1. 测试核心组件导入...")
        from alphahome.processors import Operation, OperationPipeline, ProcessorTaskBase, ProcessorEngine
        print("✅ 核心组件导入成功")
        
        # 2. 测试Operation层
        print("\n2. 测试Operation层...")
        
        class TestOperation(Operation):
            async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
                result = data.copy()
                result['test_column'] = 'processed'
                return result
        
        test_data = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})
        test_op = TestOperation()
        processed_data = await test_op.apply(test_data)
        
        assert 'test_column' in processed_data.columns
        print("✅ Operation层测试成功")
        
        # 3. 测试OperationPipeline
        print("\n3. 测试OperationPipeline...")
        
        class SecondOperation(Operation):
            async def apply(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
                result = data.copy()
                result['second_column'] = result['value'] * 2
                return result
        
        pipeline = OperationPipeline("TestPipeline")
        pipeline.add_operation(TestOperation())
        pipeline.add_operation(SecondOperation())
        
        pipeline_result = await pipeline.apply(test_data)
        
        assert 'test_column' in pipeline_result.columns
        assert 'second_column' in pipeline_result.columns
        print("✅ OperationPipeline测试成功")
        
        # 4. 测试ProcessorTaskBase
        print("\n4. 测试ProcessorTaskBase...")
        
        class TestTask(ProcessorTaskBase):
            name = "test_task"
            table_name = "test_table"
            description = "测试任务"
            
            async def fetch_data(self, **kwargs):
                return pd.DataFrame({'id': [1, 2, 3], 'value': [100, 200, 300]})
            
            async def process_data(self, data: pd.DataFrame, **kwargs):
                pipeline = OperationPipeline("TestTaskPipeline")
                pipeline.add_operation(TestOperation())
                return await pipeline.apply(data)
            
            async def save_result(self, data: pd.DataFrame, **kwargs):
                print(f"模拟保存 {len(data)} 行数据到 {self.table_name}")
        
        test_task = TestTask()
        task_result = await test_task.execute()
        
        assert task_result['status'] == 'success'
        print("✅ ProcessorTaskBase测试成功")
        
        # 5. 测试ProcessorEngine
        print("\n5. 测试ProcessorEngine...")
        
        # 注册测试任务
        @task_register()
        class RegisteredTestTask(ProcessorTaskBase):
            name = "registered_test_task"
            table_name = "registered_test_table"
            description = "注册的测试任务"
            
            async def fetch_data(self, **kwargs):
                return pd.DataFrame({'id': [1, 2], 'value': [1000, 2000]})
            
            async def process_data(self, data: pd.DataFrame, **kwargs):
                result = data.copy()
                result['engine_processed'] = True
                return result
            
            async def save_result(self, data: pd.DataFrame, **kwargs):
                print(f"引擎测试：保存 {len(data)} 行数据")
        
        db_manager = None
        try:
            # 步骤 1: 初始化数据库和任务工厂
            db_url = get_database_url() # 从您的配置中获取URL
            if not db_url:
                raise ValueError("测试需要一个有效的数据库URL")

            db_manager = DBManager(db_url)
            await db_manager.connect()
            await UnifiedTaskFactory.initialize(db_url=db_url)

            # 步骤 2: 将已初始化的 db_manager 注入到 ProcessorEngine
            # 这就是修复您遇到的 TypeError 的关键
            engine = ProcessorEngine(db_manager=db_manager, max_workers=2)

            # 步骤 3: 执行您的测试
            print("正在执行引擎测试...")
            engine_result = await engine.execute_task("registered_test_task")
            
            # 步骤 4: 断言结果
            assert engine_result['status'] == 'success'
            print("✅ ProcessorEngine 测试成功！")

        finally:
            # 步骤 5: 优雅地关闭资源
            if db_manager:
                await db_manager.close()
            print("测试资源已清理。")
        
        engine.shutdown()
        
        print("\n🎉 所有测试通过！三层架构重构成功！")
        
        # 6. 架构总结
        print("\n=== 架构总结 ===")
        print("✅ Engine层：负责任务调度和执行")
        print("✅ Task层：负责业务流程和Operation编排")
        print("✅ Operation层：负责原子级数据转换")
        print("✅ 架构清晰，职责分离，易于扩展")
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(test_architecture())
    sys.exit(0 if success else 1)
