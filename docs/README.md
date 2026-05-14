# AlphaHome 文档中心

本目录保存 AlphaHome 当前使用文档与历史设计记录。当前文档以代码现状为准；`archive/` 和 `docs/tasks/` 中的大量材料保留为历史记录，不再代表当前可执行入口。

## 当前入口

| 文档 | 说明 |
| --- | --- |
| [项目概览](../README.md) | 当前模块状态、常用入口和项目结构 |
| [安装指南](setup/installation.md) | 本地开发环境、依赖安装和最小验证 |
| [配置指南](setup/configuration.md) | `~/.alphahome/config.json`、环境变量、数据库/API 配置 |
| [用户指南](user/user_guide.md) | GUI、生产脚本、PIT、Features 的日常使用 |
| [FAQ](user/faq.md) | 常见安装、配置、数据更新和脚本问题 |
| [CLI 下线说明](CLI_USAGE_GUIDE.md) | `ah` / `alphahome-cli` / `refresh-materialized-view` 的替代入口 |

## 架构与开发

| 文档 | 说明 |
| --- | --- |
| [系统架构](architecture/system_overview.md) | 当前模块边界和数据流 |
| [任务系统](architecture/task_system.md) | `BaseTask -> FetcherTask -> 数据源 Task -> 具体任务` |
| [Features 模块设计](architecture/features_module_design.md) | processors 下线和 features 迁移的设计/验收记录 |
| [新任务开发指南](new_task_development_guide.md) | 新增 Tushare/AkShare/Tinysoft/Excel 采集任务 |
| [TDD 指南](development/tdd_guide.md) | 测试驱动开发实践 |
| [贡献指南](development/contributing.md) | 开发流程、测试和提交规范 |
| [财务数据处理指南](development/financial_data_processing_guide.md) | 财务单位、TTM、PIT 相关处理约定 |

## 数据与生产

| 文档 | 说明 |
| --- | --- |
| [数据源说明](business/data_sources.md) | AlphaDB 主要数据表和数据来源说明 |
| [数据质量](business/data_quality.md) | 数据验证机制和改进方案 |
| [PIT 增量更新](pit_incremental_update_guide.md) | 当前 PIT 生产脚本与调度建议 |
| [生产脚本说明](../scripts/production/README.md) | 日常数据更新、PIT、P/G 因子、数据库维护脚本 |

## 历史记录

- `docs/development/archive/` 保存已完成或废弃的设计草稿。
- `docs/tasks/` 保存任务甄别、审查清单和一次性实施记录。
- `archive/` 保存旧版文档和迁移前资料。

## 文档维护规则

1. 当前入口文档只描述仍可运行的命令、路径和 API。
2. 历史文档不做大幅重写；如与当前实现冲突，在开头添加归档说明。
3. 新增脚本时同步更新对应目录 README。
4. 链接必须指向仓库中真实存在的文件。
