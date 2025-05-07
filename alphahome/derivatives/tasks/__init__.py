# alphahome/derivatives/tasks/__init__.py
# 这个目录 (tasks) 用于存放所有具体的衍生品计算任务模块。

# DerivativeTaskFactory 会扫描此文件（以及此目录下的其他模块）以发现和注册任务。
# 当您在此 'tasks' 目录中创建新的衍生品任务模块时，请确保它们是可导入的，
# 并且任务类使用了 @derivative_task_register 装饰器。

# 例如，导入 'example_ma_task.py' 中定义的示例任务，使其可被工厂发现。
from .example_ma_task import ExampleMovingAverageTask

# __all__ 定义了当执行 from alphahome.derivatives.tasks import * 时导入的名称。
# 对于任务发现，通常不需要在这里显式列出所有任务，因为工厂会通过遍历模块来发现。
# 但如果希望这些任务类也能直接从 tasks 包导入，则可以将它们添加到 __all__。
__all__ = [
    "ExampleMovingAverageTask", # 使示例任务可以通过 from .tasks import ExampleMovingAverageTask 导入
] 