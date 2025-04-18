# 示例任务

本目录包含示例任务，用于演示如何创建和注册自定义任务。

## 说明

- `custom_task_example.py`: 包含两个示例任务类
  - `CustomExampleTask`: 使用带参数的装饰器注册
  - `AnotherExampleTask`: 使用不带参数的装饰器注册

## 当前状态

这些示例任务已在 `__init__.py` 中被禁用，原因是它们在实际的全局更新脚本中会导致错误，因为它们需要 `api_token` 参数，而这个参数在当前的 `Task` 类实现中不支持。

## 使用方法

如果需要重新启用这些示例任务，请修改 `__init__.py` 文件，取消注释相关导入语句和 `__all__` 列表中的条目。同时，您需要更新 `custom_task_example.py` 中的任务实现，使其兼容当前的 `Task` 类接口。

## 错误信息参考

运行全局更新脚本时，这些示例任务会产生以下错误：

```
TypeError: Task.__init__() got an unexpected keyword argument 'api_token'
``` 