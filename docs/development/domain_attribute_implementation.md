# 数据采集任务domain属性实施文档

## 概述

本文档记录了为AlphaHome系统数据采集任务添加`domain`（业务域）属性的完整实施过程，旨在解决非tushare任务业务类型识别错误的问题。

**实施日期**: 2025-07-02  
**版本**: v1.0  
**影响模块**: 任务系统核心、GUI服务层、常量定义  

## 问题背景

### 原始问题
在原有的GUI任务筛选机制中，存在严重的业务类型识别缺陷：

```python
# 原有有问题的逻辑
if task_type == 'fetch':
    parts = name.split('_')
    if parts[0] == "tushare" and len(parts) > 1:
        task_type = parts[1]  # ✅ tushare任务正确
    elif parts[0] != "tushare":
        task_type = parts[0]  # ❌ 非tushare任务错误
```

### 具体影响
- `ifind_stock_basic` 被错误识别为 "ifind" 类型而非 "stock"
- `wind_fund_nav` 被错误识别为 "wind" 类型而非 "fund"  
- `jqdata_macro_cpi` 被错误识别为 "jqdata" 类型而非 "macro"
- 用户无法按业务领域有效筛选任务

## 解决方案设计

### 核心理念
1. **显式声明优于隐式推断**: 任务可显式声明业务域
2. **统一推断逻辑**: 所有数据源使用相同的命名推断规则
3. **向后兼容**: 现有任务无需修改即可正常工作
4. **可扩展性**: 支持复杂的业务分类需求

### 技术架构
```
BaseTask.domain (显式属性)
    ↓
BaseTask.get_business_domain() (统一接口)
    ↓
GUI Services (业务类型推断)
    ↓
用户界面 (筛选功能)
```

## 实施详情

### 1. 核心任务系统修改

#### 文件: `alphahome/common/task_system/base_task.py`

**添加domain属性**:
```python
# 在data_source属性后添加
domain: Optional[str] = None  # 业务域标识（如'stock', 'fund', 'macro'等）
```

**新增get_business_domain方法**:
```python
def get_business_domain(self) -> str:
    """获取任务的业务域
    
    优先级：
    1. 显式定义的domain属性
    2. 从任务名称推断（改进的推断逻辑）
    3. 回退到数据源
    
    Returns:
        str: 业务域标识（如'stock', 'fund', 'macro'等）
    """
    # 优先使用显式定义的domain属性
    if self.domain:
        return self.domain
    
    # 回退到从任务名称推断（改进版逻辑）
    if self.name:
        parts = self.name.split('_')
        if len(parts) > 1:
            # 统一使用第二部分作为业务域，无论是什么数据源
            return parts[1]  # stock, fund, macro, etc.
        elif len(parts) == 1:
            # 如果只有一部分，使用该部分
            return parts[0]
    
    # 最后回退到数据源
    return self.data_source or "unknown"
```

### 2. 业务域常量定义

#### 文件: `alphahome/common/constants.py`

**BusinessDomain枚举**:
```python
from enum import Enum

class BusinessDomain(Enum):
    """定义业务域枚举，用于任务分类。"""
    STOCK = "stock"        # 股票
    FUND = "fund"          # 基金
    MACRO = "macro"        # 宏观经济
    INDEX = "index"        # 指数
    FUTURE = "future"      # 期货
    OPTION = "option"      # 期权
    BOND = "bond"          # 债券
    HK = "hk"              # 港股
    FOREX = "forex"        # 外汇
    CRYPTO = "crypto"      # 数字货币
    NEWS = "news"          # 新闻资讯
    RESEARCH = "research"  # 研究报告
    OTHERS = "others"      # 其他
```

**友好显示名称映射**:
```python
DOMAIN_DISPLAY_NAMES = {
    BusinessDomain.STOCK.value: "📈 股票",
    BusinessDomain.FUND.value: "💰 基金",
    BusinessDomain.MACRO.value: "🏛️ 宏观",
    BusinessDomain.INDEX.value: "📊 指数",
    BusinessDomain.FUTURE.value: "📋 期货",
    BusinessDomain.OPTION.value: "⚡ 期权",
    BusinessDomain.BOND.value: "🏦 债券",
    BusinessDomain.HK.value: "🇭🇰 港股",
    BusinessDomain.FOREX.value: "💱 外汇",
    BusinessDomain.CRYPTO.value: "₿ 数字货币",
    BusinessDomain.NEWS.value: "📰 新闻",
    BusinessDomain.RESEARCH.value: "📊 研报",
    BusinessDomain.OTHERS.value: "📁 其他",
}
```

### 3. GUI服务层集成

#### 文件: `alphahome/gui/services/task_registry_service.py`

**handle_get_collection_tasks函数修改**:
```python
# 原有逻辑（已替换）
task_type = getattr(task_instance, 'task_type', 'fetch')
if task_type == 'fetch':
    parts = name.split('_')
    if parts[0] == "tushare" and len(parts) > 1:
        task_type = parts[1]
    elif parts[0] != "tushare":
        task_type = parts[0]  # 问题所在

# 新的逻辑
task_instance = await UnifiedTaskFactory.get_task(name)
# 使用新的业务域获取逻辑
task_domain = task_instance.get_business_domain()

# 推断任务子类型（为了向后兼容，保留task_type逻辑）
task_type = getattr(task_instance, 'task_type', 'fetch')
if task_type == 'fetch':
    # 使用业务域作为更准确的类型
    task_type = task_domain
```

**缓存数据结构增强**:
```python
new_cache.append({
    "name": name,
    "type": task_type,
    "data_source": data_source,
    "domain": task_domain,  # 添加业务域信息
    "description": getattr(task_instance, "description", ""),
    "selected": existing_selection.get(name, False),
    "table_name": getattr(task_instance, "table_name", None),
})
```

**_get_single_task_details函数修改**:
```python
# 为 'fetch' 任务推断更具体的子类型，使用业务域逻辑
if details["task_type"] == 'fetch':
    if hasattr(task_instance, 'get_business_domain'):
        details["task_type"] = task_instance.get_business_domain()
    else:
        # 回退到改进的命名推断逻辑
        parts = task_name.split('_')
        if len(parts) > 1:
            details["task_type"] = parts[1]  # 统一使用第二部分
        else:
            details["task_type"] = parts[0]
```

### 4. 示例任务domain属性添加

#### 文件: `alphahome/fetchers/tasks/stock/ifind_stock_basic.py`

```python
# 1. 核心属性
name: str = "ifind_stock_basic"
description = "获取上市公司基本资料（同花顺 iFind）"
table_name: str = "ifind_stock_basic"
primary_keys = ["stock_code_a"]
date_column = None
data_source = "ifind"
domain = "stock"  # 业务域：股票
```

## 验证结果

### 修复前后对比

| 任务名称 | 修复前类型 | 修复后类型 | 状态 |
|---------|-----------|-----------|------|
| `tushare_stock_basic` | stock | stock | ✅ 保持正确 |
| `ifind_stock_basic` | ifind | stock | ✅ 已修复 |
| `wind_fund_nav` | wind | fund | ✅ 已修复 |
| `jqdata_macro_cpi` | jqdata | macro | ✅ 已修复 |

### 功能验证

1. **显式domain属性**: `iFindStockBasicTask.domain = "stock"` ✅
2. **get_business_domain方法**: 返回正确的业务域 ✅
3. **GUI筛选逻辑**: 使用业务域进行准确分类 ✅
4. **向后兼容**: 无domain属性的任务仍能正常工作 ✅

## 技术优势

### 即时收益
- **修复分类问题**: 解决非tushare任务分类错误
- **提升用户体验**: GUI筛选功能按业务域准确工作
- **增强可维护性**: 明确的业务分类标准

### 长期价值
- **支持复杂分类**: 可实现多维度业务分类
- **便于功能扩展**: 支持按业务域的高级筛选功能
- **提升代码质量**: 强制开发者明确思考任务的业务归属

## 实施策略

### 渐进式迁移
1. **阶段1**: 核心基础设施就绪（已完成）
2. **阶段2**: 关键任务添加domain属性（进行中）
3. **阶段3**: 完整覆盖和高级功能

### 最佳实践指南
1. **新任务**: 必须显式声明domain属性
2. **现有任务**: 逐步添加domain属性，优先处理有歧义的任务
3. **命名约定**: 继续遵循"数据源_业务类型_功能"格式

## 影响评估

### 系统稳定性
- **零破坏性变更**: 所有现有功能保持正常
- **向后兼容**: 无domain属性的任务仍能正常工作
- **渐进式升级**: 可分阶段实施完整覆盖

### 性能影响
- **计算开销**: 微不足道（简单字符串操作）
- **内存占用**: 每个任务增加一个字符串属性
- **响应时间**: 无可感知影响

## 未来扩展

### 多层级业务域
```python
domain = "equity.stock.basic"  # 支持层级分类
```

### 高级筛选功能
- 多domain筛选
- domain层级筛选
- 自定义business分类

### UI优化
- 业务域图标和颜色
- 分组显示
- 智能推荐

## 总结

本次实施成功解决了GUI数据采集任务筛选机制中的核心问题，通过引入domain属性和统一的业务域推断逻辑，确保了所有任务（无论数据源）都能按照业务领域正确分类。

**关键成果**:
- ✅ 修复了非tushare任务的分类错误
- ✅ 建立了可扩展的业务域管理框架
- ✅ 保持了完全的向后兼容性
- ✅ 为未来高级功能奠定了基础

该实施为AlphaHome系统的任务管理和用户体验带来了显著改进，同时为系统的长期发展提供了坚实的架构基础。