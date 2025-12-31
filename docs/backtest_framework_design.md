# 基金组合回测框架设计文档

## 一、逻辑提炼与口径清单

### 1.1 原脚本执行流程（步骤化）

#### 第一阶段：数据加载
1. **读取 Excel 数据源**
   - `Nav` sheet → 基金净值面板 `nav_df` (index=Date, columns=fund_code)
   - `Idx` sheet → 指数收益率 `idx_df` (用于基准计算)
   - `费率` sheet → 基金费率 `fee_df` (申购费率、赎回费率)
   - `调仓记录` sheet → 调仓明细 `trade_df` (组合、代码、证券名称、持仓权重、调仓时间)
   - `组合` sheet → 策略配置 `strat_df` (渠道、固定费率、上线日期、基准构成等)
   - `渠道` sheet → 渠道配置 `channel_df` (调仓效率、费用折扣、起投金额)
   - `拼接业绩` sheet → 历史业绩拼接 `paste_df` (用于延长净值序列)

2. **数据预处理**
   - 合并 `strat_df` 与 `channel_df` 得到完整策略配置
   - 为每个策略的调仓记录添加 `下次调仓` 和 `调仓编号` 字段
   - 筛选策略所需的基金净值子集 `strat_cmfnav`

#### 第二阶段：回测主循环 (backtest 函数)
3. **初始化账户** (`init_account`)
   - 创建账户 DataFrame，初始只有现金头寸
   - 字段：name, type, volume, amount, nav, cost, weight, frozen_amt, frozen_vol, chg_id, update

4. **逐日循环处理** (按 nav 日期遍历)
   - Step 4.1: 确定当日有效的目标持仓 (根据调仓时间区间筛选)
   - Step 4.2: 计算调仓生效日 `chg_dt1` (赎回/建仓日) 和 `chg_dt2` (申购日)
   - Step 4.3: 执行交易确认 (`make_trade`) - 将前日冻结的申赎订单按费率结算
   - Step 4.4: 判断是否为调仓日，生成交易指令
     - 建仓日(chg_id=1): 生成申购指令 → 冻结现金
     - 调仓赎回日(chg_id>1, dt=chg_dt1): 生成赎回指令 → 冻结份额
     - 调仓申购日(chg_id>1, dt=chg_dt2): 生成申购指令 → 冻结现金
   - Step 4.5: 更新账户 (`update_account`) - 更新净值、扣管理费、计算权重

#### 第三阶段：绩效计算
5. **计算组合净值序列** (`calc_ret`)
   - 从账户记录汇总每日市值 = amount + frozen_amt
   - 计算日收益率序列
   - 拼接历史业绩（如有配置）
   - 计算基准收益率（加权指数）

6. **生成绩效报告** (`week_report`, `calc_stat`)
   - 周/月/季/年度收益率
   - 年化收益、年化波动率、最大回撤、夏普比率

---

### 1.2 隐含假设与口径清单

| 编号 | 假设/口径 | 当前实现 | 建议默认值 | 需确认? |
|------|----------|---------|-----------|--------|
| A1 | **调仓生效日规则** | 统一T+1生效（可配置） | T+1 | - |
| A2 | **申购确认日** | T+N (N=rebalance_delay，默认2) | T+2 | - |
| A3 | **赎回确认日** | T+N (同申购) | T+2 | - |
| A4 | **申购费用计算** | `申购金额 × 统一申购费率` | 0.15% | - |
| A5 | **赎回费用计算** | `赎回金额 × 统一赎回费率` | 0% | - |
| A6 | **管理费扣除** | 按日计提，从现金扣除 | - | - |
| A7 | **权重归一化** | 自动归一化，偏离>1%记录警告 | 强制归一 | - |
| A8 | **缺失净值处理** | 前值填充(ffill) | 前值填充 | - |
| A9 | **现金处理** | 现金不计息，nav=1 | - | - |
| A10 | **份额精度** | 保留2位小数 | 可配置 | - |
| A11 | **金额精度** | 保留2位小数 | 可配置 | - |
| A12 | **估值日历** | 使用 others_calendar 交易日历 | 交易日历 | - |
| A13 | **分红处理** | 使用复权净值(adj_nav) | 红利再投 | - |
| A14 | **调仓权重类型** | 目标权重 | 支持权重/金额/份额 | - |
| A15 | **成本计算** | 加权平均成本法 | - | - |

---

### 1.3 最小可复用纯计算函数签名

```python
# ============ 核心计算函数（无副作用）============

def calc_target_units(
    current_holdings: pd.DataFrame,  # columns: [fund_id, units, nav, amount]
    target_weights: pd.DataFrame,    # columns: [fund_id, weight]
    total_value: float,
    nav_series: pd.Series,           # index=fund_id, values=nav
) -> pd.DataFrame:
    """计算目标持仓份额，返回 [fund_id, target_units, delta_units, direction]"""
    ...

def calc_trade_amount(
    units: float,
    nav: float,
    fee_rate: float,
    direction: Literal['buy', 'sell']
) -> Tuple[float, float, float]:
    """计算交易金额，返回 (gross_amount, fee, net_amount)"""
    ...

def calc_portfolio_value(
    holdings: pd.DataFrame,  # [fund_id, units, frozen_units]
    nav_series: pd.Series,
    cash: float,
    frozen_cash: float
) -> float:
    """计算组合总市值"""
    ...

def calc_portfolio_nav(
    values: pd.Series,  # index=date, values=market_value
    initial_value: float
) -> pd.Series:
    """计算组合净值序列"""
    ...

def calc_returns(nav_series: pd.Series) -> pd.Series:
    """计算收益率序列"""
    ...

def calc_annualized_return(returns: pd.Series, periods_per_year: int = 250) -> float:
    """计算年化收益率"""
    ...

def calc_annualized_volatility(returns: pd.Series, periods_per_year: int = 250) -> float:
    """计算年化波动率"""
    ...

def calc_max_drawdown(nav_series: pd.Series) -> Tuple[float, date, date]:
    """计算最大回撤及起止日期"""
    ...

def calc_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    """计算夏普比率"""
    ...

def align_nav_panel(
    nav_panel: pd.DataFrame,
    calendar: pd.DatetimeIndex,
    fill_method: Literal['ffill', 'bfill', None] = 'ffill'
) -> pd.DataFrame:
    """对齐净值面板到指定日历"""
    ...

def normalize_weights(weights: pd.Series, tolerance: float = 0.01) -> pd.Series:
    """权重归一化"""
    ...
```

---

## 二、AlphaDB 数据层接口与字段要求

### 2.1 现有表结构映射

| 框架需求 | AlphaDB 表 | Schema | 关键字段 |
|---------|-----------|--------|---------|
| 基金净值 | `fund_nav` | tushare | ts_code, nav_date, unit_nav, accum_nav, adj_nav |
| 基金基本信息 | `fund_basic` | tushare | ts_code, name, fund_type, status, m_fee, c_fee |
| 策略基本信息 | `fastrategy_basic` | excel | strategy_code, strategy_name, channel_code, fee_rate, setup_date |
| 策略组合持仓 | `fastrategy_portfolio` | excel | channel_code, strategy_code, fund_code, weight, rebalancing_date |

### 2.2 需要确认/补充的字段清单

| 序号 | 缺失项 | 说明 | 建议方案 |
|------|-------|------|---------|
| 1 | **申购费率** | 使用配置统一设置 | ✓ 已实现 |
| 2 | **赎回费率** | 使用配置统一设置 | ✓ 已实现 |
| 3 | **交易日历** | 使用 others_calendar 表 | ✓ 已实现 |
| 4 | **分红处理** | 使用复权净值(adj_nav) | ✓ 已实现 |
| 5 | **权重归一化** | 自动归一化并警告 | ✓ 已实现 |
| 6 | **管理费扣除** | 从现金扣除 | ✓ 已实现 |
| 7 | **调仓生效日** | T+1可配置 | ✓ 已实现 |

### 2.3 DataProvider 抽象接口设计

```python
from abc import ABC, abstractmethod
from typing import Optional, List
import pandas as pd

class DataProvider(ABC):
    """数据提供者抽象基类"""
    
    @abstractmethod
    def get_fund_nav(
        self,
        fund_ids: List[str],
        start_date: str,
        end_date: str,
        nav_type: str = 'unit_nav'  # unit_nav | accum_nav | adj_nav
    ) -> pd.DataFrame:
        """
        获取基金净值面板
        
        Returns:
            DataFrame with index=date, columns=fund_id, values=nav
        """
        pass
    
    @abstractmethod
    def get_rebalance_records(
        self,
        portfolio_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取调仓记录
        
        Returns:
            DataFrame with columns:
            - rebalance_date: 调仓日期
            - effective_date: 生效日期 (可选，若无则由引擎计算)
            - fund_id: 基金代码
            - target_weight: 目标权重 (0-1)
            - target_amount: 目标金额 (可选)
            - target_units: 目标份额 (可选)
        """
        pass
    
    @abstractmethod
    def get_fund_fee(
        self,
        fund_ids: List[str]
    ) -> pd.DataFrame:
        """
        获取基金费率
        
        Returns:
            DataFrame with columns:
            - fund_id: 基金代码
            - purchase_fee: 申购费率
            - redeem_fee: 赎回费率 (简化版，不考虑持有期)
            - management_fee: 管理费率
            - custody_fee: 托管费率
        """
        pass
    
    @abstractmethod
    def get_calendar(
        self,
        start_date: str,
        end_date: str,
        calendar_type: str = 'nav'  # nav | trade
    ) -> pd.DatetimeIndex:
        """
        获取日历
        
        Args:
            calendar_type: 
                'nav' - 有净值的日期
                'trade' - 交易日
        """
        pass
    
    @abstractmethod
    def get_portfolio_config(
        self,
        portfolio_id: str
    ) -> dict:
        """
        获取组合配置
        
        Returns:
            {
                'portfolio_id': str,
                'portfolio_name': str,
                'channel_code': str,
                'initial_cash': float,
                'setup_date': str,
                'rebalance_delay': int,  # T+N
                'purchase_discount': float,
                'redeem_discount': float,
                'management_fee': float,
                'fee_fund_id': str,  # 扣费货基
            }
        """
        pass
```

### 2.4 AlphaDB 访问方式建议

```python
# 推荐使用 SQLAlchemy + 连接池
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    "postgresql://user:pass@host:port/alphadb",
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10
)

# SQL 模板示例
SQL_GET_FUND_NAV = """
SELECT 
    nav_date as date,
    ts_code as fund_id,
    {nav_type} as nav
FROM tushare.fund_nav
WHERE ts_code = ANY(%(fund_ids)s)
  AND nav_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY nav_date, ts_code
"""

SQL_GET_REBALANCE = """
SELECT 
    rebalancing_date as rebalance_date,
    fund_code as fund_id,
    weight as target_weight,
    strategy_code as portfolio_id
FROM excel.fastrategy_portfolio
WHERE strategy_code = %(portfolio_id)s
  AND rebalancing_date BETWEEN %(start_date)s AND %(end_date)s
ORDER BY rebalancing_date, fund_code
"""
```

---

## 三、框架设计

### 3.1 模块划分与职责边界

```
alphahome/backtest/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── engine.py          # BacktestEngine - 回测引擎主类
│   ├── portfolio.py       # Portfolio - 组合状态管理
│   ├── position.py        # Position - 单个持仓
│   └── order.py           # Order - 交易指令
├── data/
│   ├── __init__.py
│   ├── provider.py        # DataProvider 抽象基类
│   ├── alphadb_provider.py # AlphaDBDataProvider 实现
│   ├── memory_provider.py  # MemoryDataProvider (测试用)
│   └── excel_adapter.py    # ExcelAdapter (迁移/调试用)
├── execution/
│   ├── __init__.py
│   ├── executor.py        # TradeExecutor - 交易执行
│   ├── fee.py             # FeeCalculator - 费用计算
│   └── settlement.py      # Settlement - 结算规则
├── valuation/
│   ├── __init__.py
│   └── valuator.py        # Valuator - 估值计算
├── analysis/
│   ├── __init__.py
│   ├── performance.py     # PerformanceAnalyzer - 绩效分析
│   └── report.py          # ReportGenerator - 报告生成
├── strategy/
│   ├── __init__.py
│   └── rebalance.py       # RebalanceStrategy - 调仓策略
└── utils/
    ├── __init__.py
    ├── calendar.py        # 日历工具
    └── math.py            # 数学计算工具
```

### 3.2 模块职责说明

| 模块 | 职责 | 依赖 |
|------|------|------|
| `engine` | 回测主循环、事件调度、状态协调 | portfolio, executor, valuator |
| `portfolio` | 持仓状态、现金管理、冻结资产 | position |
| `data/provider` | 数据抽象层，隔离数据源 | 无 |
| `execution/executor` | 生成订单、执行交易、处理冻结/解冻 | fee, settlement |
| `execution/fee` | 费用计算（申购/赎回/管理费） | 无 |
| `execution/settlement` | 结算规则（T+N、确认日计算） | calendar |
| `valuation/valuator` | 每日估值、净值计算 | 无 |
| `analysis/performance` | 绩效指标计算 | 无 |
| `strategy/rebalance` | 从调仓记录生成目标持仓 | 无 |

### 3.3 关键数据结构定义

```python
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, List
import pandas as pd

class OrderSide(Enum):
    BUY = 1
    SELL = -1
    FEE = 0  # 管理费扣除

class OrderStatus(Enum):
    PENDING = "pending"      # 待执行
    FROZEN = "frozen"        # 已冻结
    FILLED = "filled"        # 已成交
    CANCELLED = "cancelled"  # 已取消

@dataclass
class Order:
    """交易指令"""
    order_id: str
    portfolio_id: str
    fund_id: str
    side: OrderSide
    amount: Optional[Decimal] = None   # 申购金额
    units: Optional[Decimal] = None    # 赎回份额
    nav: Optional[Decimal] = None      # 执行净值
    fee: Decimal = Decimal(0)
    status: OrderStatus = OrderStatus.PENDING
    create_date: date = None
    settle_date: date = None           # 结算日
    rebalance_id: Optional[int] = None # 关联的调仓编号

@dataclass
class Position:
    """单个持仓"""
    fund_id: str
    fund_name: str = ""
    units: Decimal = Decimal(0)        # 可用份额
    frozen_units: Decimal = Decimal(0) # 冻结份额(待赎回)
    nav: Decimal = Decimal(1)
    cost: Decimal = Decimal(0)         # 平均成本
    last_update: date = None
    
    @property
    def market_value(self) -> Decimal:
        return (self.units + self.frozen_units) * self.nav
    
    @property
    def total_units(self) -> Decimal:
        return self.units + self.frozen_units
```

```python
@dataclass
class Portfolio:
    """组合状态"""
    portfolio_id: str
    portfolio_name: str = ""
    cash: Decimal = Decimal(0)
    frozen_cash: Decimal = Decimal(0)  # 冻结现金(待申购)
    positions: Dict[str, Position] = field(default_factory=dict)
    last_update: date = None
    
    @property
    def market_value(self) -> Decimal:
        pos_value = sum(p.market_value for p in self.positions.values())
        return self.cash + self.frozen_cash + pos_value
    
    def get_weights(self) -> pd.Series:
        mv = self.market_value
        if mv == 0:
            return pd.Series(dtype=float)
        weights = {fid: float(p.market_value / mv) for fid, p in self.positions.items()}
        weights['cash'] = float((self.cash + self.frozen_cash) / mv)
        return pd.Series(weights)

@dataclass
class RebalanceRecord:
    """调仓记录"""
    rebalance_id: int
    rebalance_date: date
    effective_date: Optional[date] = None  # 生效日，若无则由引擎计算
    fund_id: str = ""
    target_weight: Optional[float] = None
    target_amount: Optional[Decimal] = None
    target_units: Optional[Decimal] = None
    note: str = ""

@dataclass
class PortfolioConfig:
    """组合配置"""
    portfolio_id: str
    portfolio_name: str
    initial_cash: Decimal
    setup_date: date
    rebalance_delay: int = 2           # T+N 申购确认
    purchase_fee_rate: float = 0.015   # 统一申购费率
    redeem_fee_rate: float = 0.005     # 统一赎回费率
    management_fee: float = 0.0        # 年化管理费率
    rebalance_effective_delay: int = 1 # T+N 调仓生效（默认T+1）

@dataclass 
class BacktestResult:
    """回测结果"""
    portfolio_id: str
    nav_series: pd.Series              # index=date, values=nav
    returns: pd.Series                 # 日收益率
    trades: pd.DataFrame               # 交易记录
    holdings_history: pd.DataFrame     # 持仓历史
    metrics: Dict[str, float]          # 绩效指标
```

### 3.4 NAV 面板与对齐规则

```python
# NAV 面板格式
nav_panel: pd.DataFrame
# index: DatetimeIndex (估值日期)
# columns: fund_id (基金代码)
# values: float (单位净值)

# 对齐规则
class NavAlignmentRule(Enum):
    FFILL = "ffill"      # 前值填充（默认）
    BFILL = "bfill"      # 后值填充
    DROP = "drop"        # 丢弃缺失
    RAISE = "raise"      # 抛出异常
```

---

## 四、需要确认的问题清单

1. **调仓生效日规则**：当前实现区分"已上线"和"未上线"，框架是否需要保留此逻辑？
   - 建议：统一为 T+1 生效，通过配置项控制

2. **管理费扣除方式**：当前从货基份额扣除，是否改为从现金扣除？
   - 建议：默认从现金扣除，可配置从指定货基扣除

3. **权重归一化**：输入权重不为1时如何处理？
   - 建议：自动归一化并记录警告

4. **分红处理**：是否需要支持？
   - 建议：V1 版本使用复权净值(adj_nav)，暂不单独处理分红

5. **费率数据来源**：当前 alphadb 无完整费率表，如何处理？
   - 建议：新增 `fund_fee` 表，或在回测时传入费率配置

6. **交易日历**：使用 nav 日期还是独立日历表？
   - 建议：默认使用 nav 日期去重，支持配置独立日历


---

## 五、代码骨架与目录结构

### 5.1 Package 目录结构

```
alphahome/backtest/
├── __init__.py                    # 模块入口，导出主要类
├── core/
│   ├── __init__.py
│   ├── engine.py                  # BacktestEngine 回测引擎
│   ├── portfolio.py               # Portfolio, Position 组合/持仓
│   └── order.py                   # Order, OrderSide, OrderStatus
├── data/
│   ├── __init__.py
│   ├── provider.py                # DataProvider 抽象基类
│   ├── memory_provider.py         # MemoryDataProvider (测试用)
│   ├── alphadb_provider.py        # AlphaDBDataProvider (生产用)
│   └── excel_adapter.py           # ExcelAdapter (迁移/调试用)
├── execution/
│   ├── __init__.py
│   ├── executor.py                # TradeExecutor 交易执行
│   └── fee.py                     # FeeCalculator 费用计算
├── valuation/
│   ├── __init__.py
│   └── valuator.py                # Valuator 估值计算
├── analysis/
│   ├── __init__.py
│   └── performance.py             # PerformanceAnalyzer 绩效分析
└── examples/
    ├── __init__.py
    └── simple_backtest.py         # 简单回测示例
```

### 5.2 关键类实现摘要

| 类名 | 文件 | 职责 |
|------|------|------|
| `BacktestEngine` | core/engine.py | 回测主循环、状态协调 |
| `Portfolio` | core/portfolio.py | 组合状态、现金/持仓管理 |
| `Position` | core/portfolio.py | 单个持仓状态 |
| `Order` | core/order.py | 交易指令 |
| `DataProvider` | data/provider.py | 数据抽象接口 |
| `MemoryDataProvider` | data/memory_provider.py | 内存数据源(测试) |
| `AlphaDBDataProvider` | data/alphadb_provider.py | 数据库数据源(生产) |
| `ExcelAdapter` | data/excel_adapter.py | Excel数据源(迁移) |
| `FeeCalculator` | execution/fee.py | 费用计算 |
| `Valuator` | valuation/valuator.py | 估值计算 |
| `PerformanceAnalyzer` | fund_analysis/performance.py | 绩效指标计算 |

### 5.3 使用示例

```python
# 示例1: 使用内存数据源进行回测
from alphahome.fund_backtest import BacktestEngine, MemoryDataProvider, PortfolioConfig

# 准备数据
nav_panel = pd.DataFrame(...)  # index=date, columns=fund_id
rebalance_records = pd.DataFrame(...)  # rebalance_date, fund_id, target_weight

# 创建数据提供者
provider = MemoryDataProvider(
    nav_panel=nav_panel,
    rebalance_records={'portfolio_1': rebalance_records}
)

# 创建引擎并添加组合
engine = BacktestEngine(provider)
engine.add_portfolio(PortfolioConfig(
    portfolio_id='portfolio_1',
    portfolio_name='测试组合',
    initial_cash=1000000,
    setup_date='2023-01-01',
    rebalance_delay=2
))

# 运行回测
results = engine.run('2023-01-01', '2023-12-31')

# 获取结果
result = results['portfolio_1']
print(f"年化收益: {result.metrics['annualized_return']:.2%}")
print(f"最大回撤: {result.metrics['max_drawdown']:.2%}")
```

```python
# 示例2: 使用 AlphaDB 数据源
from alphahome.fund_backtest.data.alphadb_provider import AlphaDBDataProvider

provider = AlphaDBDataProvider(
    connection_string="postgresql://user:pass@localhost:5432/alphadb"
)

engine = BacktestEngine(provider)
# ... 后续同上
```

```python
# 示例3: 从 Excel 迁移数据
from alphahome.fund_backtest.data.excel_adapter import ExcelAdapter

adapter = ExcelAdapter(r"E:\stock\天府银行实盘组合跟踪.xlsm")
adapter.load()

# 获取组合列表
portfolios = adapter.get_portfolio_list()

# 使用 adapter 作为数据源进行回测
engine = BacktestEngine(adapter)
# ...
```

---

## 六、扩展点

### 6.1 手续费扩展
- 支持阶梯费率（按持有天数）
- 支持后端收费模式
- 支持认购费/申购费区分

### 6.2 结算规则扩展
- 支持不同基金类型的 T+N 规则
- 支持节假日顺延
- 支持大额赎回限制

### 6.3 分红处理扩展
- 现金分红
- 红利再投资
- 分红税处理

### 6.4 多组合并行
- 当前实现已支持多组合
- 可扩展为多进程/多线程并行

---

## 七、后续工作

1. **补充 alphadb 费率表**：新增 `fund_fee` 表存储申购/赎回费率
2. **完善交易日历**：新增独立的 `trade_calendar` 表
3. **添加基准配置表**：支持组合基准收益计算
4. **单元测试**：为核心计算函数编写测试用例
5. **性能优化**：大规模回测时的向量化优化


---

## 八、已实现的改进（根据用户要求）

### 8.1 费率统一设置 ✓
- 在 `PortfolioConfig` 中增加 `purchase_fee_rate` 和 `redeem_fee_rate` 字段
- 默认值：申购费率 1.5%，赎回费率 0.5%
- 不再依赖 alphadb 的费率表，简化配置

### 8.2 交易日历支持 ✓
- 使用 `tushare.others_calendar` 表获取交易日历
- 支持指定交易所（默认 SSE 上交所）
- SQL 查询仅返回 `is_open=1` 的交易日

### 8.3 权重自动归一化 ✓
- 在 `_normalize_weights` 方法中实现
- 按调仓日期分组归一化
- 权重总和偏离 1.0 超过 1% 时记录警告日志

### 8.4 分红处理 ✓
- 使用复权净值 `adj_nav` 替代单位净值 `unit_nav`
- 在 `BacktestEngine.run()` 方法中通过 `use_adj_nav=True` 参数控制
- 复权净值已包含分红再投资效果

### 8.5 管理费从现金扣除 ✓
- 在 `_deduct_management_fee` 方法中实现
- 按日计提：`市值 × 年化费率 / 365 × 天数`
- 直接从 `portfolio.cash` 扣除
- 现金不足时记录警告

### 8.6 调仓生效日可配置 ✓
- 新增 `rebalance_effective_delay` 配置项（默认 1，即 T+1）
- 在 `_check_rebalance` 方法中使用
- 赎回生效日 = 调仓日 + `rebalance_effective_delay`
- 申购生效日 = 调仓日 + `rebalance_delay`

---

## 九、配置示例

```python
from alphahome.fund_backtest import PortfolioConfig

config = PortfolioConfig(
    portfolio_id='my_portfolio',
    portfolio_name='我的组合',
    initial_cash=1000000.0,
    setup_date='2023-01-01',
    
    # 费率设置（统一）
    purchase_fee_rate=0.0015,      # 0.15% 申购费
    redeem_fee_rate=0.0,           # 0% 赎回费
    management_fee=0.005,          # 0.5% 年化管理费
    
    # 调仓规则
    rebalance_effective_delay=1,   # T+1 调仓生效
    rebalance_delay=2,             # T+2 申购确认
)

# 运行回测（使用复权净值）
results = engine.run(
    start_date='2023-01-01',
    end_date='2023-12-31',
    use_adj_nav=True  # 使用复权净值处理分红
)
```

---

## 十、后续优化方向

1. **基准收益计算**：支持组合基准配置和超额收益分析
2. **归因分析**：资产配置归因、选券归因
3. **风险指标**：VaR、CVaR、下行风险等
4. **多进程回测**：支持大规模组合并行回测
5. **实时监控**：支持实盘组合跟踪和偏离度监控
