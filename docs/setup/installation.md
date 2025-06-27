# AlphaHome 安装指南

本文档将指导您完成 AlphaHome 智能量化投研系统的安装和初始配置。

## 📋 **系统要求**

### **硬件要求**
- **CPU**: 4核心以上（推荐8核心）
- **内存**: 8GB以上（推荐16GB）
- **存储**: 50GB可用空间（推荐SSD）
- **网络**: 稳定的互联网连接

### **软件要求**
- **操作系统**: Windows 10/11, macOS 10.15+, Ubuntu 18.04+
- **Python**: 3.10 或更高版本
- **PostgreSQL**: 12.0 或更高版本
- **Git**: 用于代码管理

## 🚀 **快速安装**

### **步骤1: 安装Python环境**

**Windows:**
```bash
# 下载并安装Python 3.10+
# 从 https://python.org 下载官方安装包
# 安装时勾选"Add Python to PATH"
```

**macOS:**
```bash
# 使用Homebrew安装
brew install python@3.10
```

**Ubuntu:**
```bash
# 更新包列表
sudo apt update

# 安装Python 3.10
sudo apt install python3.10 python3.10-pip python3.10-venv
```

### **步骤2: 安装PostgreSQL**

**Windows:**
```bash
# 下载并安装PostgreSQL
# 从 https://postgresql.org/download/windows/ 下载
# 记住设置的密码，后续配置需要使用
```

**macOS:**
```bash
# 使用Homebrew安装
brew install postgresql
brew services start postgresql
```

**Ubuntu:**
```bash
# 安装PostgreSQL
sudo apt install postgresql postgresql-contrib

# 启动服务
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### **步骤3: 克隆项目**

```bash
# 克隆项目到本地
git clone https://github.com/your-repo/alphahome.git
cd alphahome
```

### **步骤4: 创建虚拟环境**

```bash
# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate
```

### **步骤5: 安装依赖**

```bash
# 升级pip
pip install --upgrade pip

# 安装项目依赖
pip install -r requirements.txt

# 或使用Make命令（如果支持）
make install
```

## 🔧 **详细配置**

### **数据库配置**

1. **创建数据库**
```sql
-- 连接到PostgreSQL
psql -U postgres

-- 创建数据库
CREATE DATABASE tusharedb;

-- 创建用户（可选）
CREATE USER alphahome WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE tusharedb TO alphahome;
```

2. **配置连接**
```bash
# 复制配置模板
cp config.example.json config.json

# 编辑配置文件
# 修改数据库连接信息
```

### **API配置**

1. **获取Tushare Token**
   - 访问 [Tushare官网](https://tushare.pro)
   - 注册账号并获取API Token
   - 将Token填入配置文件

2. **配置示例**
```json
{
    "database": {
        "url": "postgresql://username:password@localhost:5432/tusharedb"
    },
    "api": {
        "tushare_token": "your_tushare_token_here"
    }
}
```

## ✅ **验证安装**

### **运行测试**
```bash
# 运行单元测试
make test-unit

# 或直接使用pytest
pytest tests/unit/ -v
```

### **启动GUI**
```bash
# 启动图形界面
python run.py

# 或使用模块方式
python -m alphahome.gui.main_window
```

### **测试数据库连接**
```bash
# 运行数据库连接测试
python -c "
from alphahome.common.db_manager import create_sync_manager
db = create_sync_manager()
print('数据库连接成功!' if db.test_connection() else '数据库连接失败!')
"
```

## 🔍 **常见问题**

### **Python版本问题**
```bash
# 检查Python版本
python --version

# 如果版本不对，使用特定版本
python3.10 -m venv venv
```

### **依赖安装失败**
```bash
# 清理pip缓存
pip cache purge

# 使用国内镜像源
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/
```

### **数据库连接失败**
1. 检查PostgreSQL服务是否启动
2. 验证用户名和密码
3. 确认数据库名称正确
4. 检查防火墙设置

### **权限问题**
```bash
# Windows: 以管理员身份运行命令提示符
# macOS/Linux: 使用sudo（谨慎使用）
sudo pip install -r requirements.txt
```

## 🎯 **下一步**

安装完成后，建议您：

1. 阅读[配置指南](./configuration.md)进行详细配置
2. 查看[用户指南](../user/user_guide.md)了解系统使用
3. 运行第一个数据采集任务测试系统功能

## 📞 **获取帮助**

如果安装过程中遇到问题：

1. 查看[常见问题](../user/faq.md)
2. 检查系统日志文件
3. 在GitHub上提交Issue
4. 联系技术支持

---

**恭喜！** 您已成功安装 AlphaHome 系统。现在可以开始您的量化投研之旅了！ 🚀
