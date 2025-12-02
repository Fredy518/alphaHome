# AlphaHome - 量化数据基础设施

**专注于金融数据获取、处理和管理的量化数据平台**

> ⚠️ **当前状态**：项目处于活跃开发中，**目前仅 `fetchers` 模块完整可用**。其他模块（processors、factors、gui 等）仍在开发或重构中，暂不建议在生产环境使用。

## 🎯 **项目定位**

AlphaHome 定位为 **量化数据基础设施**，专注于：

- **数据获取**：多源数据自动化抓取（Tushare、AkShare、同花顺等）
- **数据处理**：清洗、转换、标准化
- **数据存储**：PostgreSQL 数据库管理
- **数据访问**：统一的数据查询接口

**不在范围内**：回测引擎、策略开发、交易执行等功能。

## 🏗️ **模块状态**

| 模块 | 状态 | 说明 |
|------|------|------|
| 📊 `fetchers/` | ✅ **可用** | 数据获取，支持 Tushare、AkShare 等多数据源 |
| ⚙️ `common/` | ✅ **可用** | 核心工具（数据库、配置、日志、任务系统） |
| 🔍 `providers/` | 🔶 部分可用 | 数据访问接口，基础功能可用 |
| 🔧 `processors/` | 🚧 开发中 | 数据处理引擎，功能开发中 |
| 🧮 `factors/` | 🚧 开发中 | 因子计算库，功能开发中 |
| 🖥️ `gui/` | 🚧 开发中 | 图形界面，基础功能可用但不稳定 |

## 📊 **Fetchers - 数据获取模块（推荐使用）**

### **支持的数据源**

- **Tushare Pro**：A股行情、财务、指数等全面数据
- **AkShare**：免费数据源，涨停原因、宏观数据等
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

# 或使用命令行
alphahome task run tushare_stock_daily --update-type smart
```

### **任务示例**

```python
# 命令行运行任务
alphahome task run tushare_stock_daily --start-date 20240101 --end-date 20241231
alphahome task run akshare_stock_limitup_reason --update-type smart

# 查看可用任务
alphahome task list
```

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
make test-unit
```

## 📁 **项目结构**

```
alphahome/
├── common/           # 核心工具（数据库、配置、日志、任务系统）
├── fetchers/         # ✅ 数据获取模块（推荐使用）
│   ├── sources/      #    数据源实现（Tushare、AkShare、PyTDX）
│   ├── tasks/        #    数据任务定义
│   └── tools/        #    辅助工具（交易日历等）
├── processors/       # 🚧 数据处理模块（开发中）
├── factors/          # 🚧 因子计算模块（开发中）
├── providers/        # 🔶 数据访问接口（部分可用）
└── gui/              # 🚧 图形界面（开发中）
```

## 📄 **许可证**

MIT License - 详见 [LICENSE](LICENSE) 文件

---

**AlphaHome** - 量化数据，简单可靠 🚀