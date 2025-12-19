# AlphaHome 统一 CLI 实施总结

## 项目完成情况

### ✅ 已完成的工作

#### Phase 1: 搭建统一CLI骨架
- [x] 创建 `alphahome/cli/` 目录结构
- [x] 实现统一的命令行解析框架（基于 argparse）
- [x] 定义统一的退出码规范（0/1/2/3/4/130）
- [x] 实现统一的异常处理和日志配置
- [x] 在 pyproject.toml 中注册 `ah` 和 `alphahome-cli` 入口点

#### Phase 2: 集成现有工具
- [x] 将 DolphinDB CLI (`alphahome-ddb`) 集成为 `ah ddb` 子命令
- [x] 将物化视图 CLI (`refresh-materialized-view`) 集成为 `ah mv` 子命令
- [x] 修正 `refresh-materialized-view` 入口指向问题
- [x] 保持旧命令继续可用（向后兼容）

#### Phase 3: 生产脚本透传
- [x] 实现 `ah prod run <alias> -- <args>` 的 subprocess 透传层
- [x] 覆盖 6 个最常用的生产脚本：
  - data-collection
  - pit-update
  - g-factor 和 g-factor-quarterly
  - p-factor 和 p-factor-quarterly

#### Phase 4: 代码治理
- [x] 创建 `alphahome/production/` 包和改造指南
- [x] 提供脚本改造的路线图
- [x] 当前采用轻量级设计（subprocess 透传），为未来改造预留扩展点

#### Phase 5: 测试与文档
- [x] 创建完整的测试套件（14 个测试，全部通过）
- [x] 测试覆盖：命令解析、执行、帮助文本、退出码
- [x] 创建详细的使用指南 (CLI_USAGE_GUIDE.md)
- [x] 修复 logging_utils.py 中的 IO 错误

#### Phase 3: 兼容与迁移策略
- [x] 验证所有原有入口点仍正常工作
- [x] 在旧命令中添加迁移提示（指向ah命令）
- [x] 更新文档强调ah作为推荐入口
- [x] 保持向后兼容，不破坏现有工作流

### 📊 交付物清单

```
alphahome/cli/
├── __init__.py                  # CLI 包初始化
├── main.py                      # 主入口（100+ 行）
├── core/
│   ├── exitcodes.py            # 退出码定义
│   ├── logging_config.py        # 日志配置
│   ├── config.py               # 配置管理
│   └── exceptions.py           # 异常定义
└── commands/
    ├── base.py                 # 命令组基类
    ├── prod.py                 # 生产脚本命令组
    ├── ddb.py                  # DolphinDB 命令组
    ├── mv.py                   # 物化视图命令组
    ├── gui.py                  # GUI 命令组
    └── registry.py             # 命令注册表

tests/cli/
├── __init__.py
└── test_cli_integration.py      # 14 个集成测试

docs/
└── CLI_USAGE_GUIDE.md           # 完整使用指南（360+ 行）
```

### 📈 测试结果

```
============================= test session starts =============================
tests/cli/test_cli_integration.py::TestCLIParser                    8 passed
tests/cli/test_cli_integration.py::TestCLIExecution               3 passed
tests/cli/test_cli_integration.py::TestExitCodes                  1 passed
tests/cli/test_cli_integration.py::TestCLIHelp                    2 passed

====================== 14 passed in 2.58s ==============================
```

## 使用示例

### 基础命令

```bash
# 查看帮助
ah --help
ah prod --help
ah ddb --help

# 列出生产脚本
ah prod list

# 运行数据采集
ah prod run data-collection -- --workers 5 --log_level DEBUG

# DolphinDB 操作
ah ddb init-kline5m
ah ddb import-hikyuu-5min --codes "000001.SZ"

# 物化视图管理
ah mv refresh-all
ah mv status pit_financial_indicators_mv

# 启动 GUI
ah gui
```

## 技术架构

### 命令树结构

```
ah (main)
├── --log-level [DEBUG|INFO|WARNING|ERROR]
├── --format [text|json]
├── --config <path>
└── subcommands:
    ├── prod
    │   ├── list
    │   └── run <alias> -- [args...]
    ├── ddb
    │   ├── init-kline5m
    │   ├── import-hikyuu-5min
    │   └── drop-db
    ├── mv
    │   ├── refresh <view>
    │   ├── refresh-all
    │   ├── status <view>
    │   └── status-all
    └── gui
```

### 退出码规范

| 代码 | 含义 | 用途 |
|-----|------|------|
| 0 | SUCCESS | 命令成功完成 |
| 1 | FAILURE | 业务失败 |
| 2 | INVALID_ARGS | 参数错误 |
| 3 | UNAVAILABLE | 资源不可用 |
| 4 | INTERNAL_ERROR | 内部错误 |
| 130 | INTERRUPTED | 用户中断（Ctrl-C） |

## 设计决策

### 1. 独立的 CLI 入口（`ah` 命令）
- **理由**：保持 `alphahome` 命令只用于启动 GUI，避免语义混淆
- **优势**：清晰的命令职责划分，易于扩展
- **后果**：用户需要学习新命令，但提供别名以兼容

### 2. 子命令组架构
- **理由**：按功能领域组织命令，易于管理和扩展
- **优势**：层次清晰，便于添加新功能
- **后果**：多层嵌套可能增加学习成本，但通过帮助缓解

### 3. Subprocess 透传方式
- **理由**：快速启用，无需改造现有脚本
- **优势**：最小化改动，风险低，立刻可用
- **后果**：存在进程开销，日志不统一；但为未来改造预留扩展点

### 4. 统一的参数契约
- **理由**：提供一致的用户体验
- **优势**：易学易用，便于自动化
- **后果**：需要在各子命令中遵循约定

## 后续优化计划

### 短期（1-2周）
1. **脚本改造**：
   - 改造首批 2-3 个关键脚本为包内模块
   - 更新 `ah prod` 命令改为直接调用包内模块

2. **功能增强**：
   - 支持命令补全（bash/zsh）
   - 添加 `--dry-run` 全局参数

### 中期（1个月）
1. **命令扩展**：
   - 新增 `ah factor` 命令组（因子管理和计算）
   - 新增 `ah data` 命令组（数据查询和导出）

2. **输出格式**：
   - 支持 JSON、CSV、表格等输出格式
   - 实现 `--output` 全局参数

### 长期（2-3个月）
1. **完全改造**：
   - 将所有生产脚本改造为包内模块
   - 消除 subprocess 开销，提升性能

2. **高级特性**：
   - 支持命令链和管道
   - 添加配置文件支持（全局配置）
   - 实现插件系统

## 知识积累与可复用资产

### 文件模板和样本
- `alphahome/cli/commands/base.py` - 命令组基类模板
- `alphahome/cli/commands/prod.py` - 完整的子命令实现示例
- `alphahome/cli/core/` - CLI 核心模块库

### 最佳实践
1. **命令组设计模式**：继承 CommandGroup 基类
2. **参数处理**：使用 argparse 的 add_subparsers 和 set_defaults
3. **异常处理**：统一通过 CLIError 及其子类
4. **日志管理**：使用 setup_cli_logging() 统一配置

### 测试策略
- `TestCLIParser` - 参数解析单元测试
- `TestCLIExecution` - 命令执行单元测试
- `TestExitCodes` - 退出码规范测试
- `TestCLIIntegration` - 集成测试（支持 pytest markers）

## 如何扩展

### 添加新的命令组

1. 创建新文件：`alphahome/cli/commands/newcommand.py`
2. 实现 CommandGroup 子类
3. 注册到 `alphahome/cli/commands/registry.py`
4. 编写对应的测试

### 修改现有命令

1. 编辑对应的 `alphahome/cli/commands/*.py` 文件
2. 更新测试用例
3. 更新 CLI_USAGE_GUIDE.md 文档
4. 提交 git commit

## 验收标准

✅ **所有检查项均已完成**

- [x] CLI 框架完整可用
- [x] 所有命令组正常工作
- [x] 参数透传正确无误
- [x] 退出码符合规范
- [x] 测试覆盖率达成目标
- [x] 文档完整详细
- [x] 向后兼容性保证
- [x] 未来扩展预留足够空间
- [x] 旧命令迁移提示正常显示
- [x] 所有原有入口点继续可用

## 版本信息

- **CLI 版本**：1.0
- **实施日期**：2025-12-19
- **包版本**：alphahome 1.2
- **Python 版本**：3.9+

## 联系与反馈

如有问题或建议，请：
1. 参考 `docs/CLI_USAGE_GUIDE.md` 中的故障排除部分
2. 查看已有的测试用例了解使用方式
3. 检查 git 提交历史了解实施过程

---

**项目完成日期**：2025-12-19  
**总实施时间**：约 2 小时  
**代码新增**：1300+ 行  
**测试覆盖**：14 个测试，全部通过
