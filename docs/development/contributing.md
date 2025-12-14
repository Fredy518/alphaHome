# AlphaHome 贡献指南

欢迎为 AlphaHome 智能量化投研系统贡献代码！本文档将指导您如何参与项目开发。

## 🎯 **贡献方式**

我们欢迎以下形式的贡献：

- **代码贡献**: 新功能开发、Bug修复、性能优化
- **文档改进**: 文档更新、教程编写、翻译工作
- **问题报告**: Bug报告、功能请求、使用反馈
- **测试贡献**: 测试用例编写、测试覆盖率提升
- **设计贡献**: UI/UX改进、架构设计建议

## 🚀 **快速开始**

### **1. 环境准备**

```bash
# 1. Fork项目到您的GitHub账号
# 2. 克隆您的Fork
git clone https://github.com/YOUR_USERNAME/alphahome.git
cd alphahome

# 3. 添加上游仓库
git remote add upstream https://github.com/original-repo/alphahome.git

# 4. 创建开发环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 5. 安装依赖
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .  # 开发模式，便于本地调试
```

### **2. 开发工作流**

```bash
# 1. 同步最新代码
git fetch upstream
git checkout main
git merge upstream/main

# 2. 创建功能分支
git checkout -b feature/your-feature-name
# 或者: git checkout -b fix/issue-number

# 3. 进行开发
# ... 编写代码 ...

# 4. 运行测试与静态检查
pytest tests/unit/ -v -m "unit and not requires_db and not requires_api"
flake8 alphahome/
black --check alphahome/ tests/
isort --check-only alphahome/ tests/

# 5. 提交代码
git add .
git commit -m "feat: add new feature description"

# 6. 推送到您的Fork
git push origin feature/your-feature-name

# 7. 创建Pull Request
# 在GitHub上创建PR到主仓库
```

## 📋 **开发规范**

### **代码风格**

我们使用以下工具确保代码质量：

```bash
# 代码格式化
black alphahome/ tests/
isort alphahome/ tests/

# 包含：
# - black: Python代码格式化
# - isort: 导入语句排序

# 代码质量检查
flake8 alphahome/ --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 alphahome/ --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
mypy alphahome/  # 如启用类型检查

# 包含：
# - flake8: 语法和风格检查
# - mypy: 类型检查（可选）
```

**代码风格要求**:
- 遵循PEP 8规范
- 使用black进行代码格式化
- 行长度限制为88字符
- 使用类型注解（推荐）

### **提交信息规范**

使用[Conventional Commits](https://conventionalcommits.org/)规范：

```bash
# 格式: <type>(<scope>): <description>

# 示例:
git commit -m "feat(fetchers): add new data source support"
git commit -m "fix(gui): resolve task execution hang issue"
git commit -m "docs(readme): update installation instructions"
git commit -m "test(db): add database connection tests"
git commit -m "refactor(common): improve error handling"
```

**提交类型**:
- `feat`: 新功能
- `fix`: Bug修复
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建工具、依赖更新等

### **分支命名规范**

```bash
# 功能开发
feature/add-wind-data-source
feature/improve-gui-performance

# Bug修复
fix/database-connection-timeout
fix/issue-123-memory-leak

# 文档更新
docs/update-api-documentation
docs/add-deployment-guide

# 重构
refactor/task-system-architecture
refactor/database-layer
```

## 🧪 **测试要求**

### **测试驱动开发(TDD)**

我们鼓励使用TDD方法开发新功能：

```python
# 1. 先写测试（RED）
def test_new_feature():
    """测试新功能的预期行为"""
    # 这个功能还不存在，测试会失败
    result = new_feature(input_data)
    assert result == expected_output

# 2. 实现功能（GREEN）
def new_feature(input_data):
    """实现最简单的版本让测试通过"""
    return expected_output

# 3. 重构优化（REFACTOR）
def new_feature(input_data):
    """优化实现，保持测试通过"""
    # 改进的实现
    pass
```

### **测试覆盖率要求**

- 新功能代码覆盖率应达到90%以上
- 核心模块覆盖率应保持在80%以上
- 所有公共API必须有测试

```bash
# 运行测试并检查覆盖率
pytest tests/ --cov=alphahome --cov-report=html --cov-report=term-missing

# 查看覆盖率报告
open htmlcov/index.html
```

### **测试分类**

```python
import pytest

# 单元测试
@pytest.mark.unit
def test_config_manager():
    """测试配置管理器功能"""
    pass

# 集成测试
@pytest.mark.integration
def test_database_integration():
    """测试数据库集成功能"""
    pass

# 需要数据库的测试
@pytest.mark.requires_db
def test_data_fetching():
    """测试数据获取功能"""
    pass

# 需要API的测试
@pytest.mark.requires_api
def test_tushare_api():
    """测试Tushare API调用"""
    pass
```

## 📝 **文档贡献**

### **文档类型**

1. **API文档**: 代码中的docstring
2. **用户文档**: 使用指南和教程
3. **开发文档**: 架构设计和开发指南
4. **示例代码**: 使用示例和最佳实践

### **文档规范**

```python
def fetch_stock_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取股票历史数据
    
    Args:
        symbol: 股票代码，如'000001.SZ'
        start_date: 开始日期，格式'YYYY-MM-DD'
        end_date: 结束日期，格式'YYYY-MM-DD'
    
    Returns:
        包含股票数据的DataFrame，包含以下列：
        - date: 交易日期
        - open: 开盘价
        - high: 最高价
        - low: 最低价
        - close: 收盘价
        - volume: 成交量
    
    Raises:
        ValueError: 当日期格式不正确时
        APIError: 当API调用失败时
    
    Example:
        >>> df = fetch_stock_data('000001.SZ', '2023-01-01', '2023-12-31')
        >>> print(df.head())
    """
    pass
```

## 🐛 **问题报告**

### **Bug报告模板**

```markdown
## Bug描述
简要描述遇到的问题

## 复现步骤
1. 执行操作A
2. 点击按钮B
3. 观察到错误C

## 预期行为
描述您期望发生的情况

## 实际行为
描述实际发生的情况

## 环境信息
- 操作系统: Windows 10 / macOS 12 / Ubuntu 20.04
- Python版本: 3.10.5
- AlphaHome版本: 1.0.0
- 相关依赖版本: pandas 1.5.0, asyncpg 0.27.0

## 错误日志
```
粘贴相关的错误日志或截图
```

## 附加信息
其他可能有用的信息
```

### **功能请求模板**

```markdown
## 功能描述
简要描述您希望添加的功能

## 使用场景
描述这个功能的使用场景和价值

## 详细设计
如果有具体的设计想法，请详细描述

## 替代方案
是否考虑过其他解决方案

## 附加信息
其他相关信息或参考资料
```

## 🔍 **代码审查**

### **审查清单**

**功能性**:
- [ ] 功能是否按预期工作
- [ ] 边界条件是否处理正确
- [ ] 错误处理是否完善

**代码质量**:
- [ ] 代码是否清晰易读
- [ ] 是否遵循项目编码规范
- [ ] 是否有适当的注释和文档

**测试**:
- [ ] 是否包含足够的测试用例
- [ ] 测试是否覆盖主要功能路径
- [ ] 测试是否能够通过

**性能**:
- [ ] 是否存在性能问题
- [ ] 是否有不必要的资源消耗
- [ ] 是否考虑了扩展性

**安全性**:
- [ ] 是否存在安全漏洞
- [ ] 输入验证是否充分
- [ ] 敏感信息是否得到保护

## 🏆 **贡献者认可**

### **贡献者类型**

- **核心维护者**: 长期活跃的核心开发者
- **功能贡献者**: 贡献重要功能的开发者
- **文档贡献者**: 专注于文档改进的贡献者
- **测试贡献者**: 专注于测试质量的贡献者
- **社区支持者**: 帮助用户解决问题的贡献者

### **认可方式**

- 在README中列出贡献者
- 在发布说明中感谢贡献者
- 颁发贡献者徽章
- 邀请参与项目决策

## 📞 **获取帮助**

### **开发支持**

- **GitHub Discussions**: 技术讨论和问题求助
- **Issue Tracker**: Bug报告和功能请求
- **开发者邮件列表**: dev@alphahome.com
- **实时聊天**: Discord/Slack频道

### **资源链接**

- [TDD实践指南](./tdd_guide.md)
- [架构设计文档](../architecture/system_overview.md)
- [API文档](../api/) - 正在完善中

## 🎉 **感谢**

感谢您对 AlphaHome 项目的关注和贡献！每一个贡献都让这个项目变得更好。

我们特别感谢：
- 所有代码贡献者
- 文档改进者
- 问题报告者
- 社区支持者

---

**让我们一起构建更好的量化投研工具！** 🚀
