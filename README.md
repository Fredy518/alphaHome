# AlphaHome - 量化投研一体化平台

**集数据获取、处理、分析与回测于一体的量化投研系统**

## 🎯 **项目定位**

AlphaHome 是一个完整的量化投研平台，提供从数据到策略的全流程支持：

- **数据获取**：多源数据自动化抓取（Tushare、AkShare、通达信等）
- **数据处理**：清洗、转换、标准化、PIT 时点数据
- **数据存储**：PostgreSQL + DolphinDB 双引擎
- **基金分析**：绩效指标、回撤分析、归因分析、可视化
- **组合回测**：场外基金组合回测框架
- **风险模型**：Barra 多因子风险模型
- **脚本/模块入口**：通过 `scripts/` 与 `python -m ...` 运行批处理与工具

## 🏗️ **模块状态**

| 模块 | 状态 | 说明 |
|------|------|------|
| 📊 `fetchers/` | ✅ **可用** | 数据获取，支持 Tushare、AkShare、PyTDX 等多数据源 |
| ⚙️ `common/` | ✅ **可用** | 核心工具（数据库、配置、日志、任务系统） |
| 🖥️ `cli/` | ❌ **已下线** | 仅保留空壳包（历史兼容），不再提供统一命令行入口点 |
| 📉 `barra/` | ✅ **可用** | Barra 多因子风险模型（协方差估计、归因连接） |
| 🔗 `integrations/` | ✅ **可用** | 外部系统集成（DolphinDB 5分钟K线加速层） |
| 🔍 `providers/` | 🔶 部分可用 | 数据访问接口，基础功能可用 |
| 🔧 `processors/` | ❌ **已删除** | Phase 3 已移除，相关能力已迁移/下线 |
| 🧮 `factors/` | 🚧 开发中 | 因子计算库，G/P因子已可用 |
| 🖥️ `gui/` | 🚧 开发中 | 图形界面，基础功能可用但不稳定 |

---


## 📊 **fetchers - 数据获取模块**

### **支持的数据源**

- **Tushare Pro**：A股行情、财务、指数等全面数据
- **AkShare**：免费数据源，涨停原因、宏观数据等
- **Tinysoft (pyTSL)**：分钟级行情与 TSL 数据服务
- **PyTDX**：通达信实时/历史行情

### **核心特性**

- ✅ 统一的任务框架，支持全量/增量/智能更新
- ✅ 自动重试和错误处理
- ✅ 交易日历感知，自动跳过非交易日
- ✅ 并发控制和限流保护
- ✅ 数据验证和质量检查

### **快速开始**

```bash
# 1. 安装依赖
pip install -e .

# 2. 配置数据库和API（复制并编辑配置文件）
cp config.example.json ~/.alphahome/config.json

# 3. 启动GUI运行数据任务
python run.py

# 命令行/批处理：优先使用 scripts/ 下脚本或 python -m 模块入口（见下文）
```

---

## 🖥️ **cli - 已下线**

统一 CLI（如 `ah` / `alphahome-cli` / `refresh-materialized-view` 等入口点）已在 Phase 3 下线。

当前建议使用：

- DolphinDB / Hikyuu 5min：`./scripts/import_all_hikyuu_to_ddb.ps1` 或 `python -m alphahome.integrations.dolphindb.cli ...`
- 物化视图（MV）初始化：`python scripts/initialize_materialized_views.py`

说明见：[CLI 使用指南（已下线）](docs/CLI_USAGE_GUIDE.md)

---

## 📉 **barra - 多因子风险模型**

Barra 风格的多因子风险模型，支持风险归因和组合优化。

### **核心功能**
- ✅ 因子协方差矩阵估计
- ✅ 特质方差估计
- ✅ 组合风险分解
- ✅ 多期收益归因连接（Carino/Menchero）

### **快速开始**

```python
from alphahome.barra import RiskModel, RiskModelConfig

# 配置风险模型
config = RiskModelConfig(
    half_life_factor=60,
    half_life_specific=120,
)

# 估计协方差
model = RiskModel(config)
factor_cov = model.estimate_factor_covariance(factor_returns)
specific_var = model.estimate_specific_variance(residuals)

# 计算组合风险
portfolio_risk = model.compute_portfolio_risk(weights, exposures)
```

---

## ⚙️ **配置说明**

配置文件路径：`~/.alphahome/config.json`

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "alphahome",
    "user": "your_user",
    "password": "your_password"
  },
  "tushare": {
    "token": "your_tushare_token"
  },
  "dolphindb": {
    "host": "localhost",
    "port": 8848,
    "username": "admin",
    "password": "123456"
  }
}
```

## 🚀 **安装**

```bash
# 克隆项目
git clone https://github.com/your-repo/alphahome.git
cd alphahome

# 安装（开发模式）
pip install -e .

# 运行测试
pytest tests/unit/ -v -m "unit and not requires_db and not requires_api"
```

## 📁 **项目结构**

```
alphahome/
├── common/           # 核心工具（数据库、配置、日志、任务系统）
├── cli/              # ❌ 已下线（空壳包，仅历史兼容）
├── fetchers/         # ✅ 数据获取模块
│   ├── sources/      #    数据源实现（Tushare、AkShare、PyTDX）
│   ├── tasks/        #    数据任务定义
│   └── tools/        #    辅助工具（交易日历等）
├── barra/            # ✅ Barra 多因子风险模型
├── integrations/     # ✅ 外部系统集成（DolphinDB）
├── features/         # ✅ Features/MV 等核心能力
├── factors/          # 🚧 因子计算模块（开发中）
├── providers/        # 🔶 数据访问接口（部分可用）
└── gui/              # 🚧 图形界面（开发中）
```

## 📚 **文档**

- [CLI 使用指南（已下线）](docs/CLI_USAGE_GUIDE.md)
- [任务开发指南](docs/new_task_development_guide.md)
- [回测框架设计](docs/backtest_framework_design.md)
- [Hikyuu 集成](docs/hikyuu_integration_guide.md)
- [更多文档](docs/README.md)

## 📄 **许可证**

MIT License - 详见 [LICENSE](LICENSE) 文件

---

**AlphaHome** - 量化投研，简单可靠 🚀
