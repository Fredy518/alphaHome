# AlphaHome 常见问题解答

本文档收集了用户在使用 AlphaHome 系统过程中遇到的常见问题和解决方案。

## 🚀 **安装和配置问题**

### **Q1: Python版本兼容性问题**
**问题**: 系统要求Python 3.10+，但我的环境是Python 3.8/3.9
**解决方案**:
```bash
# 方法1: 使用pyenv管理多个Python版本
curl https://pyenv.run | bash
pyenv install 3.10.12
pyenv local 3.10.12

# 方法2: 使用conda创建新环境
conda create -n alphahome python=3.10
conda activate alphahome

# 方法3: 直接安装Python 3.10
# Windows: 从python.org下载安装包
# macOS: brew install python@3.10
# Ubuntu: sudo apt install python3.10
```

### **Q2: 依赖安装失败**
**问题**: pip install 时出现编译错误或网络超时
**解决方案**:
```bash
# 1. 使用国内镜像源
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/

# 2. 升级pip和setuptools
pip install --upgrade pip setuptools wheel

# 3. 清理pip缓存
pip cache purge

# 4. 分步安装关键依赖
pip install numpy pandas asyncpg
pip install -r requirements.txt

# 5. 使用conda安装科学计算包
conda install numpy pandas scipy matplotlib
pip install -r requirements.txt
```

### **Q3: 如何设置和连接到 PostgreSQL 数据库？**
**回答**:
1.  **安装 PostgreSQL**: 如果您尚未安装，请从 [PostgreSQL 官网](https://www.postgresql.org/download/) 下载并安装。
2.  **创建数据库和用户**: 使用 `psql` 或您喜欢的数据库管理工具执行以下 SQL 命令。
    ```sql
    -- 建议创建一个专门用于本应用的用户
    CREATE USER alphahome WITH PASSWORD 'your_strong_password';

    -- 创建数据库
    CREATE DATABASE alphadb;

    -- 将数据库的所有权和权限授予新用户
    GRANT ALL PRIVILEGES ON DATABASE alphadb TO alphahome;
    ```
3.  **配置 AlphaHome**:
    在您的用户配置目录中找到或创建 `config.json` 文件（例如 `C:/Users/YourUser/AppData/Local/trademaster/alphahome/config.json`），并按以下格式填入数据库连接信息：
    ```json
    {
        "database": {
            "url": "postgresql://alphahome:your_strong_password@localhost:5432/alphadb"
        },
        "api": {
            "tushare_token": "your_tushare_token_here"
        }
    }
    ```
4. **(重要) 从旧版本升级?**
   如果您是从旧版本升级，并且现有的数据库名为 `tusharedb`，请在更新 `config.json` 文件**之前**，先从项目根目录运行迁移脚本来自动重命名数据库：
   ```bash
   python scripts/migrate_db_name.py
   ```
   该脚本将安全地引导您完成数据库的重命名过程。重命名后，再更新您的 `config.json` 文件。

### **Q4: 配置文件问题**
**问题**: config.json配置不生效或格式错误
**解决方案**:
```bash
# 1. 验证JSON格式
python -c "
import json
with open('config.json', 'r') as f:
    config = json.load(f)
print('配置文件格式正确')
"

# 2. 检查配置文件位置
python -c "
from alphahome.common.config_manager import ConfigManager
config = ConfigManager()
print(f'配置文件路径: {config.config_path}')
"

# 3. 使用环境变量
export ALPHAHOME_DB_URL="postgresql://user:pass@localhost/db"
export ALPHAHOME_TUSHARE_TOKEN="your_token"
```

## 📊 **数据获取问题**

### **Q5: Tushare API调用失败**
**问题**: API返回错误或超时
**解决方案**:
```python
# 1. 验证Token有效性
import tushare as ts
ts.set_token('your_token')
pro = ts.pro_api()

# 测试API调用
try:
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
    print(f'获取到 {len(df)} 条股票数据')
except Exception as e:
    print(f'API调用失败: {e}')

# 2. 检查API配额和频率限制
# 登录Tushare官网查看API调用次数和权限

# 3. 调整请求频率
# 在config.json中设置更长的延迟时间
{
    "tasks": {
        "tushare_stock_daily": {
            "delay_seconds": 2,
            "rate_limit_delay": 60
        }
    }
}
```

### **Q6: 数据更新不完整**
**问题**: 某些日期的数据缺失或更新不及时
**解决方案**:
```python
# 1. 检查数据覆盖情况
from alphahome.common.data_quality_checker import DataQualityChecker
from alphahome.common.db_manager import create_sync_manager
import asyncio

async def check_data():
    db = create_sync_manager()
    checker = DataQualityChecker(db)
    results = await checker.check_tables(['tushare_stock_daily'])
    print(results)

asyncio.run(check_data())

# 2. 手动指定日期范围更新
# 在GUI中选择"手动增量"模式，指定具体日期范围

# 3. 检查交易日历
# 确认缺失的日期是否为交易日
```

### **Q7: 内存不足错误**
**问题**: 处理大量数据时出现内存溢出
**解决方案**:
```json
// 1. 调整批处理大小
{
    "tasks": {
        "tushare_stock_daily": {
            "batch_size": 50,  // 减小批处理大小
            "concurrent_limit": 3  // 减少并发数
        }
    }
}

// 2. 启用数据库连接池优化
{
    "database": {
        "pool_config": {
            "max_size": 10,  // 减少连接池大小
            "command_timeout": 60
        }
    }
}
```

## 🖥️ **GUI使用问题**

### **Q8: GUI启动失败**
**问题**: 运行python run.py时出现错误
**解决方案**:
```bash
# 1. 检查tkinter是否安装
python -c "import tkinter; print('tkinter可用')"

# 2. 安装tkinter（如果缺失）
# Ubuntu: sudo apt-get install python3-tk
# CentOS: sudo yum install tkinter
# macOS: 通常随Python自带

# 3. 检查依赖
pip install -r requirements.txt

# 4. 使用详细模式启动
python -v run.py
```

### **Q9: 任务执行卡住**
**问题**: GUI中任务执行后长时间无响应
**解决方案**:
```python
# 1. 检查任务日志
# 在GUI的"任务日志"标签页查看详细日志

# 2. 检查数据库连接
# 确认数据库服务正常，网络连接稳定

# 3. 调整超时设置
{
    "database": {
        "pool_config": {
            "command_timeout": 300  // 增加超时时间
        }
    }
}

# 4. 重启应用
# 关闭GUI，重新启动
```

### **Q10: 结果显示异常**
**问题**: 任务执行成功但结果显示不正确
**解决方案**:
```python
# 1. 刷新界面
# 点击"刷新"按钮或重新选择任务

# 2. 检查数据库数据
# 直接查询数据库确认数据是否正确保存

# 3. 清理缓存
# 重启应用清理内存缓存
```

## 🔧 **开发和调试问题**

### **Q11: 测试运行失败**
**问题**: make test 或 pytest 执行失败
**解决方案**:
```bash
# 1. 检查测试环境
python -m pytest --version

# 2. 安装测试依赖
pip install pytest pytest-asyncio pytest-cov

# 3. 运行特定测试
pytest tests/unit/test_config_manager.py -v

# 4. 跳过需要外部资源的测试
pytest tests/unit/ -m "not requires_db and not requires_api"

# 5. 查看详细错误信息
pytest tests/unit/ -v --tb=long
```

### **Q12: 导入错误**
**问题**: ModuleNotFoundError或ImportError
**解决方案**:
```bash
# 1. 检查Python路径
python -c "import sys; print('\n'.join(sys.path))"

# 2. 安装包到开发模式
pip install -e .

# 3. 设置PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# 4. 检查包结构
find . -name "__init__.py" | head -10
```

### **Q13: 性能问题**
**问题**: 系统运行缓慢或资源占用过高
**解决方案**:
```python
# 1. 启用性能监控
{
    "performance": {
        "enable_monitoring": true,
        "log_slow_operations": true,
        "slow_operation_threshold": 5.0
    }
}

# 2. 优化数据库查询
# 检查慢查询日志，添加必要的索引

# 3. 调整并发参数
{
    "tasks": {
        "tushare_stock_daily": {
            "concurrent_limit": 5,  // 根据系统资源调整
            "batch_size": 100
        }
    }
}

# 4. 使用性能分析工具
python -m cProfile -o profile.stats your_script.py
```

## 🔍 **故障排除步骤**

### **通用排查流程**

1. **检查日志文件**
```bash
# 查看最新日志
tail -f logs/alphahome.log

# 搜索错误信息
grep -i error logs/alphahome.log
```

2. **验证配置**
```bash
# 检查配置文件
python -m alphahome.tools.config_checker
```

3. **测试连接**
```bash
# 测试数据库连接
python -c "
from alphahome.common.db_manager import create_sync_manager
db = create_sync_manager()
print('连接状态:', db.test_connection())
"
```

4. **检查系统资源**
```bash
# 检查内存使用
free -h  # Linux
# 检查磁盘空间
df -h
```

5. **重启服务**
```bash
# 重启PostgreSQL
sudo systemctl restart postgresql

# 重启应用
# 关闭GUI，重新启动
```

## 📞 **获取更多帮助**

### **文档资源**
- [用户指南](./user_guide.md) - 详细使用说明
- [安装指南](../setup/installation.md) - 安装配置步骤
- [配置指南](../setup/configuration.md) - 配置参数说明

### **社区支持**
- GitHub Issues - 报告问题和功能请求
- 项目Wiki - 社区贡献的文档和教程
- 讨论区 - 用户交流和经验分享

### **联系方式**
- 技术支持邮箱: support@alphahome.com
- 项目主页: https://github.com/your-repo/alphahome
- 文档网站: https://alphahome.readthedocs.io

---

**提示**: 如果您的问题没有在此文档中找到答案，请在GitHub上提交Issue，我们会及时回复并更新此FAQ。
