# AlphaHome 命令行工具指南

本文档介绍 AlphaHome 系统提供的各种命令行工具和Make命令的使用方法。

## 🛠️ **Make 命令**

AlphaHome 提供了完整的 Makefile，包含开发、测试、部署等常用命令。

### **查看所有可用命令**
```bash
make help
```

### **开发环境命令**

#### **安装依赖**
```bash
# 安装项目依赖
make install

# 等价于：
pip install --upgrade pip
pip install -r requirements.txt
```

#### **代码质量检查**
```bash
# 运行所有代码质量检查
make lint

# 包含以下检查：
# - flake8 语法检查
# - black 格式检查
# - isort 导入排序检查
```

#### **代码格式化**
```bash
# 自动格式化代码
make format

# 包含：
# - black 代码格式化
# - isort 导入排序
```

#### **安全检查**
```bash
# 运行安全扫描
make security

# 包含：
# - safety 依赖漏洞检查
# - bandit 代码安全扫描
```

### **测试命令**

#### **运行所有测试**
```bash
# 运行完整测试套件
make test
```

#### **单元测试**
```bash
# 运行单元测试（不需要数据库和API）
make test-unit

# 等价于：
pytest tests/unit/ -v -m "unit and not requires_db and not requires_api"
```

#### **集成测试**
```bash
# 运行集成测试
make test-integration

# 等价于：
pytest tests/integration/ -v -m "integration"
```

#### **测试覆盖率**
```bash
# 运行测试并生成覆盖率报告
make test-cov

# 生成HTML覆盖率报告
# 报告位置：htmlcov/index.html
```

#### **快速测试**
```bash
# 运行快速测试（跳过慢速测试）
make test-fast

# 等价于：
pytest tests/unit/ -v -m "unit and not slow and not requires_db and not requires_api"
```

#### **测试监视模式**
```bash
# 启动测试监视模式（需要安装pytest-watch）
make test-watch

# 文件变化时自动运行测试
```

### **报告生成**

#### **生成测试报告**
```bash
# 生成HTML测试报告
make test-report

# 报告文件：test_report.html
```

#### **依赖检查**
```bash
# 检查依赖漏洞
make check-deps

# 使用pip-audit检查已知漏洞
```

### **清理命令**

```bash
# 清理临时文件
make clean

# 清理内容：
# - Python缓存文件 (__pycache__, *.pyc)
# - 测试缓存 (.pytest_cache)
# - 覆盖率文件 (.coverage, htmlcov/)
# - 构建文件 (*.egg-info, dist/)
```

### **综合命令**

```bash
# 运行所有质量检查
make quality

# 包含：lint + security + test-unit
```

## 🐍 **Python 模块命令**

### **启动GUI应用**
```bash
# 方法1：使用run.py
python run.py

# 方法2：使用模块方式
python -m alphahome.gui.main_window

# 方法3：安装后使用命令（如果已安装）
alphahome
```

### **配置管理**
```bash
# 检查配置文件
python -m alphahome.tools.config_checker

# 检查特定配置项
python -m alphahome.tools.config_checker --check database
python -m alphahome.tools.config_checker --check api

# 显示配置信息
python -c "
from alphahome.common.config_manager import ConfigManager
config = ConfigManager()
print(config.get_debug_info())
"
```

### **数据库操作**
```bash
# 测试数据库连接
python -c "
from alphahome.common.db_manager import create_sync_manager
db = create_sync_manager()
print('数据库连接成功!' if db.test_connection() else '数据库连接失败!')
"

# 列出所有数据表
python scripts/debug_list_tables.py

# 数据质量检查
python -c "
from alphahome.common.data_quality_checker import DataQualityChecker
from alphahome.common.db_manager import create_sync_manager
import asyncio

async def check_quality():
    db = create_sync_manager()
    checker = DataQualityChecker(db)
    results = await checker.check_all_tables()
    print(f'检查完成，共检查 {len(results)} 个表')

asyncio.run(check_quality())
"
```

### **任务执行**
```bash
# 运行特定任务（示例）
python -c "
from alphahome.fetchers.tasks.stock.tushare_stock_basic import TushareStockBasicTask
from alphahome.common.db_manager import create_async_manager
import asyncio

async def run_task():
    db = create_async_manager()
    task = TushareStockBasicTask(db)
    result = await task.execute()
    print(f'任务执行结果: {result}')

asyncio.run(run_task())
"
```

## 🔧 **开发工具脚本**

### **测试脚本**
```bash
# 测试数据处理重构
python scripts/test_data_processing_refactor.py

# 测试ETF任务
python scripts/test_etf_tasks.py

# 测试验证架构
python scripts/test_unified_validation_architecture.py

# 验证所有任务报告模式
python scripts/verify_all_tasks_report_mode.py
```

### **数据库维护脚本**
```bash
# 删除复权列（示例）
python scripts/drop_qfq_columns.py

# 调试列表表格
python scripts/debug_list_tables.py
```

## 📊 **性能分析工具**

### **性能监控**
```bash
# 启用性能监控运行任务
python -c "
from alphahome.common.performance_monitor import PerformanceMonitor
import time

monitor = PerformanceMonitor()
monitor.start_monitoring()

# 模拟任务执行
time.sleep(2)

stats = monitor.stop_monitoring()
monitor.print_stats(stats)
"
```

### **内存使用分析**
```bash
# 使用memory_profiler分析内存使用
pip install memory_profiler

# 分析特定函数内存使用
python -m memory_profiler your_script.py
```

## 🐛 **调试工具**

### **日志分析**
```bash
# 查看最新日志
tail -f logs/alphahome.log

# 搜索错误日志
grep -i error logs/alphahome.log

# 分析日志统计
grep -c "INFO\|WARNING\|ERROR" logs/alphahome.log
```

### **数据库调试**
```bash
# 连接到PostgreSQL
psql -U username -d tusharedb

# 查看表结构
\d table_name

# 查看表数据
SELECT * FROM table_name LIMIT 10;

# 检查表大小
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## 🔄 **CI/CD 命令**

### **本地CI检查**
```bash
# 模拟CI环境检查
make quality

# 检查是否通过所有CI测试
echo "运行质量检查..." && make lint && \
echo "运行安全检查..." && make security && \
echo "运行单元测试..." && make test-unit && \
echo "所有检查通过！"
```

### **构建和打包**
```bash
# 构建Python包
python -m build

# 安装本地包
pip install -e .

# 卸载包
pip uninstall alphahome
```

## 📝 **自定义脚本**

### **创建自定义任务脚本**
```python
#!/usr/bin/env python
# custom_task.py

import asyncio
from alphahome.common.db_manager import create_async_manager
from alphahome.fetchers.tasks.stock.tushare_stock_daily import TushareStockDailyTask

async def main():
    """自定义任务执行脚本"""
    db = create_async_manager()
    
    # 创建任务
    task = TushareStockDailyTask(db)
    
    # 执行任务
    result = await task.execute()
    
    print(f"任务执行完成: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

### **批量任务脚本**
```python
#!/usr/bin/env python
# batch_tasks.py

import asyncio
from alphahome.common.task_factory import TaskFactory
from alphahome.common.db_manager import create_async_manager

async def run_batch_tasks():
    """批量运行多个任务"""
    db = create_async_manager()
    factory = TaskFactory(db)
    
    # 定义要运行的任务
    task_names = [
        'tushare_stock_basic',
        'tushare_stock_daily',
        'tushare_index_basic'
    ]
    
    # 并发执行任务
    tasks = [factory.create_task(name).execute() for name in task_names]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 输出结果
    for name, result in zip(task_names, results):
        if isinstance(result, Exception):
            print(f"{name}: 失败 - {result}")
        else:
            print(f"{name}: 成功 - {result}")

if __name__ == "__main__":
    asyncio.run(run_batch_tasks())
```

## 💡 **使用技巧**

### **1. 开发工作流**
```bash
# 标准开发流程
make install          # 安装依赖
make format          # 格式化代码
make lint            # 检查代码质量
make test-unit       # 运行单元测试
make test-cov        # 检查覆盖率
```

### **2. 调试技巧**
```bash
# 详细输出模式
python -v your_script.py

# 启用调试日志
export ALPHAHOME_LOG_LEVEL=DEBUG
python your_script.py
```

### **3. 性能优化**
```bash
# 使用cProfile分析性能
python -m cProfile -o profile.stats your_script.py

# 分析profile结果
python -c "
import pstats
p = pstats.Stats('profile.stats')
p.sort_stats('cumulative').print_stats(10)
"
```

---

**提示**: 建议将常用的命令组合创建为shell脚本或别名，提高开发效率。
