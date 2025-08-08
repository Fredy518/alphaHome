"""
ResearchPipeline - 声明式的研究流水线框架

将研究过程分解为一系列可编程的步骤，确保研究的可复现性
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

# 设置日志
logger = logging.getLogger(__name__)


class Step(ABC):
    """
    流水线步骤的基类
    
    每个步骤负责执行研究流程中的一个具体环节
    """
    
    def __init__(self, context):
        """
        初始化步骤
        
        Args:
            context: ResearchContext实例，提供对核心系统的访问
        """
        self.context = context
        self.name = self.__class__.__name__
        
    @abstractmethod
    def run(self, **kwargs) -> Optional[Dict[str, Any]]:
        """
        执行步骤的主要逻辑
        
        Args:
            **kwargs: 来自上一步骤的输出或初始参数
            
        Returns:
            Dict[str, Any]: 输出数据，将传递给下一个步骤
        """
        pass
        
    # 移除复杂的验证机制，保持简单
        
    def __repr__(self):
        return f"<Step: {self.name}>"


class ResearchPipeline:
    """
    研究流水线
    
    将多个步骤组织成一个完整的研究流程
    """
    
    def __init__(self, steps: List[Step], name: Optional[str] = None):
        """
        初始化流水线
        
        Args:
            steps: 步骤列表，按顺序执行
            name: 流水线名称
        """
        self.steps = steps
        self.name = name or "ResearchPipeline"
        self.results = []
        self.execution_time = 0
        
    def add_step(self, step: Step):
        """添加步骤到流水线末尾"""
        self.steps.append(step)
        
    def run(self, initial_params: Optional[Dict[str, Any]] = None):
        """
        运行流水线

        Args:
            initial_params: 初始参数，传递给第一个步骤
        """
        logger.info(f"开始执行流水线: {self.name}")
        start_time = time.time()

        # 清空之前的结果
        self.results = []

        # 初始参数
        current_params = initial_params or {}

        # 依次执行每个步骤
        for i, step in enumerate(self.steps):
            logger.info(f"执行步骤 {i+1}/{len(self.steps)}: {step.name}")

            try:
                # 执行步骤
                result = step.run(**current_params)

                # 记录结果
                self.results.append({
                    'step': step.name,
                    'status': 'success',
                    'output': result
                })

                # 更新参数（如果有输出）
                if result:
                    current_params.update(result)

                logger.info(f"步骤 {step.name} 执行完成")

            except Exception as e:
                logger.error(f"步骤 {step.name} 执行失败: {e}")

                # 记录失败
                self.results.append({
                    'step': step.name,
                    'status': 'failed',
                    'error': str(e)
                })

                # 停止执行
                raise

        # 记录总执行时间
        self.execution_time = time.time() - start_time
        logger.info(f"流水线 {self.name} 执行完成，耗时 {self.execution_time:.2f}秒")
            
    def get_summary(self) -> Dict[str, Any]:
        """获取流水线执行摘要"""
        successful_steps = sum(1 for r in self.results if r['status'] == 'success')
        failed_steps = sum(1 for r in self.results if r['status'] == 'failed')
        
        return {
            'pipeline_name': self.name,
            'total_steps': len(self.steps),
            'executed_steps': len(self.results),
            'successful_steps': successful_steps,
            'failed_steps': failed_steps,
            'execution_time': self.execution_time,
            'results': self.results
        }
        
    def save_results(self, filepath: str):
        """保存流水线执行结果"""
        import json

        summary = self.get_summary()
        summary['timestamp'] = datetime.now().isoformat()

        # 过滤掉不能JSON序列化的对象（如DataFrame）
        def make_serializable(obj):
            if hasattr(obj, 'to_dict'):  # DataFrame等pandas对象
                return f"<{type(obj).__name__} with shape {getattr(obj, 'shape', 'unknown')}>"
            elif hasattr(obj, '__dict__'):  # 复杂对象
                return f"<{type(obj).__name__} object>"
            else:
                return obj

        # 递归处理嵌套结构
        def clean_for_json(data):
            if isinstance(data, dict):
                return {k: clean_for_json(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [clean_for_json(item) for item in data]
            else:
                try:
                    json.dumps(data)  # 测试是否可序列化
                    return data
                except (TypeError, ValueError):
                    return make_serializable(data)

        clean_summary = clean_for_json(summary)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(clean_summary, f, indent=2, ensure_ascii=False)

        logger.info(f"流水线结果已保存至 {filepath}")


# 简化的基础步骤类
class DataLoadStep(Step):
    """数据加载步骤的基类"""

    def __init__(self, context):
        super().__init__(context)
        self.start_date = None
        self.end_date = None
        self.stock_list = []
        self.table_name = 'stock_daily'

    def run(self, **kwargs):
        """子类需要实现具体的数据加载逻辑"""
        raise NotImplementedError("子类需要实现run方法")


class DataValidationStep(Step):
    """数据验证步骤的基类"""

    def __init__(self, context):
        super().__init__(context)
        self.checks = ['null_check', 'duplicate_check']

    def run(self, **kwargs):
        """子类需要实现具体的验证逻辑"""
        raise NotImplementedError("子类需要实现run方法")


class SaveResultsStep(Step):
    """保存结果步骤的基类"""

    def __init__(self, context):
        super().__init__(context)
        self.save_to_csv = True
        self.save_to_db = False
        self.output_dir = 'output'

    def run(self, **kwargs):
        """子类需要实现具体的保存逻辑"""
        raise NotImplementedError("子类需要实现run方法")
