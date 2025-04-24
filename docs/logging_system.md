# 日志系统说明文档

## 1. 日志系统概述

本项目使用Python标准库的`logging`模块实现日志记录功能。日志系统设计为分层结构，通过不同的前缀（Logger名称）区分不同模块和组件产生的日志，便于问题追踪和调试。

日志输出同时发送至控制台和日志文件，默认日志文件保存在项目根目录的`logs`文件夹中，文件名格式为`{任务名}_{日期}.log`。

## 2. 日志命名规则

系统中的日志前缀遵循一定的命名规则，主要基于以下原则：

1. **模块路径命名**：使用模块的路径作为Logger名称，例如`data_module.sources.tushare.tushare_api`
2. **任务名称前缀**：数据任务类使用`task.{任务名称}`格式，例如`task.tushare_fund_daily`
3. **类名称**：更新脚本通常使用类名作为Logger名称，例如`FundDailyUpdater`
4. **组件名称**：特定组件使用专用名称，例如`db_manager`、`task_factory`等

## 3. 常见日志前缀解释

在运行过程中，你可能会看到多种不同前缀的日志，以下是常见日志前缀的含义：

| 日志前缀 | 含义 | 示例 |
|---------|------|------|
| `task.{任务名}` | 数据任务执行相关日志 | `task.tushare_fund_daily` |
| `data_module.sources.tushare.tushare_api` | Tushare API调用相关日志 | API请求、响应和速率限制信息 |
| `{类名}` | 更新脚本类产生的日志 | `FundDailyUpdater`、`IndexBasicUpdater` |
| `db_manager` | 数据库管理相关日志 | 连接、查询和事务信息 |
| `task_factory` | 任务工厂相关日志 | 任务创建和配置加载信息 |

## 4. 日志前缀切换原因

在一次完整的数据更新过程中，日志前缀可能会发生多次切换，这是因为：

1. **调用链**：任务执行过程涉及多个组件的调用链，每个组件使用自己的Logger
2. **并发执行**：并发任务同时产生日志，可能导致不同前缀的日志交替出现
3. **特定操作**：特定操作（如API调用）会产生专门的日志前缀

例如，执行`update_funddaily.py`时，日志流程大致如下：
- 开始时使用`FundDailyUpdater`前缀（更新脚本类）
- 调用任务执行时切换到`task.tushare_fund_daily`前缀（任务类）
- API调用时切换到`data_module.sources.tushare.tushare_api`前缀
- 数据库操作时可能出现`db_manager`前缀
- 完成后回到`FundDailyUpdater`前缀总结结果

## 5. 日志级别说明

系统使用标准的日志级别，从低到高依次为：

- **DEBUG**：详细调试信息，通常仅在调试时启用
- **INFO**：常规操作信息，表示正常执行流程
- **WARNING**：警告信息，表示可能的问题，但不影响主要功能
- **ERROR**：错误信息，表示发生了影响功能的问题
- **CRITICAL**：严重错误，表示系统可能无法继续运行

默认日志级别为INFO，可以通过修改配置或环境变量调整。

## 6. 日志示例解读

以下是一些常见日志示例及其含义：

```
2023-01-01 10:00:00 - FundDailyUpdater - INFO - 开始执行任务: tushare_fund_daily
```
- 更新脚本开始执行任务

```
2023-01-01 10:00:01 - task.tushare_fund_daily - INFO - 获取数据，参数: {'start_date': '20230101', 'end_date': '20230101'}
```
- 任务类开始获取数据

```
2023-01-01 10:00:02 - data_module.sources.tushare.tushare_api - INFO - API fund_daily 共获取 650 条记录
```
- Tushare API成功获取了650条记录

```
2023-01-01 10:00:03 - task.tushare_fund_daily - INFO - 批次 1/10: o保存数据到表 tushare_fund_daily
```
- 任务类将数据保存到数据库

```
2023-01-01 10:00:04 - db_manager - INFO - 数据库连接池已关闭
```
- 数据库管理器关闭连接池

## 7. 日志系统配置

默认的日志配置在各个主要组件的初始化代码中设置，主要包括：

1. **TaskUpdaterBase**：设置基本日志格式和文件输出
2. **Task**：创建任务特定的Logger
3. **TushareAPI**：创建API调用相关的Logger

如需调整日志级别或格式，可以修改相应组件的初始化代码。 