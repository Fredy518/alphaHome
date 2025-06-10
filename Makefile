# AlphaHome项目开发工具

.PHONY: help install test test-unit test-integration test-cov lint format security clean

# 默认目标
help:
	@echo "AlphaHome项目开发命令："
	@echo ""
	@echo "  make install          安装依赖"
	@echo "  make test             运行所有测试"
	@echo "  make test-unit        运行单元测试"
	@echo "  make test-integration 运行集成测试"
	@echo "  make test-cov         运行测试并生成覆盖率报告"
	@echo "  make lint             代码质量检查"
	@echo "  make format           代码格式化"
	@echo "  make security         安全检查"
	@echo "  make clean            清理临时文件"
	@echo ""

# 安装依赖
install:
	pip install --upgrade pip
	pip install -r requirements.txt

# 运行所有测试
test:
	pytest tests/ -v

# 运行单元测试
test-unit:
	pytest tests/unit/ -v -m "unit and not requires_db and not requires_api"

# 运行集成测试
test-integration:
	pytest tests/integration/ -v -m "integration"

# 运行测试并生成覆盖率报告
test-cov:
	pytest tests/ --cov=alphahome --cov-report=html --cov-report=term-missing

# 代码质量检查
lint:
	@echo "运行flake8..."
	flake8 alphahome/ --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 alphahome/ --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics
	@echo "检查black格式..."
	black --check --diff alphahome/
	@echo "检查isort..."
	isort --check-only --diff alphahome/

# 代码格式化
format:
	@echo "运行black格式化..."
	black alphahome/ tests/
	@echo "运行isort排序..."
	isort alphahome/ tests/

# 安全检查
security:
	@echo "运行safety检查..."
	safety check --ignore 51457
	@echo "运行bandit安全扫描..."
	bandit -r alphahome/ -f text

# 清理临时文件
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -name ".coverage" -delete
	find . -type d -name "htmlcov" -exec rm -rf {} +

# TDD相关命令
test-watch:
	@echo "启动测试监视模式（需要安装pytest-watch）..."
	ptw -- tests/unit/ -v -m "unit and not requires_db"

# 快速测试（跳过慢速测试）
test-fast:
	pytest tests/unit/ -v -m "unit and not slow and not requires_db and not requires_api"

# 生成测试报告
test-report:
	pytest tests/ --html=test_report.html --self-contained-html

# 检查依赖漏洞
check-deps:
	pip-audit --desc

# 运行所有质量检查
quality: lint security test-unit
	@echo "所有质量检查完成！" 