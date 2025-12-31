"""回测引擎"""

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import logging
import pandas as pd

from .portfolio import Portfolio, Position
from .order import Order, OrderSide, OrderStatus
from ..data.provider import DataProvider
from ..execution.executor import TradeExecutor
from ..execution.fee import FeeCalculator
from ..valuation.valuator import Valuator
from alphahome.fund_analysis import PerformanceAnalyzer


logger = logging.getLogger(__name__)


@dataclass
class PortfolioConfig:
    """组合配置"""
    portfolio_id: str
    portfolio_name: str
    initial_cash: float
    setup_date: str  # YYYY-MM-DD
    rebalance_delay: int = 2
    purchase_fee_rate: float = 0.015  # 统一申购费率（默认1.5%）
    redeem_fee_rate: float = 0.005    # 统一赎回费率（默认0.5%）
    management_fee: float = 0.0       # 年化管理费率
    rebalance_effective_delay: int = 1  # 调仓生效日延迟（T+N，默认T+1）
    redeem_settle_delay: int = 3      # 赎回到账延迟（T+N，默认T+3）


@dataclass
class BacktestResult:
    """回测结果"""
    portfolio_id: str
    nav_series: pd.Series
    returns: pd.Series
    trades: pd.DataFrame
    holdings_history: pd.DataFrame
    metrics: Dict[str, float]


class BacktestEngine:
    """
    回测引擎主类
    
    职责：
    - 协调数据加载、交易执行、估值计算
    - 管理回测主循环
    - 生成回测结果
    """
    
    def __init__(self, data_provider: DataProvider):
        self.data_provider = data_provider
        self.executor = TradeExecutor()
        self.fee_calculator = FeeCalculator()
        self.valuator = Valuator()
        self.analyzer = PerformanceAnalyzer()
        
        self._portfolios: Dict[str, Portfolio] = {}
        self._configs: Dict[str, PortfolioConfig] = {}
        self._pending_orders: Dict[str, List[Order]] = {}  # portfolio_id -> orders
        self._trade_records: List[dict] = []
        self._holdings_history: List[pd.DataFrame] = []
        self._last_fee_date: Dict[str, Optional[date]] = {}
    
    def add_portfolio(self, config: PortfolioConfig) -> None:
        """添加待回测的组合"""
        portfolio = Portfolio(
            portfolio_id=config.portfolio_id,
            portfolio_name=config.portfolio_name,
            cash=Decimal(str(config.initial_cash))
        )
        self._portfolios[config.portfolio_id] = portfolio
        self._configs[config.portfolio_id] = config
        self._pending_orders[config.portfolio_id] = []
        self._last_fee_date[config.portfolio_id] = None
        logger.info(f"添加组合: {config.portfolio_id}, 初始资金: {config.initial_cash}")
    
    def run(self, start_date: str, end_date: str, use_adj_nav: bool = True) -> Dict[str, BacktestResult]:
        """
        执行回测
        
        Args:
            start_date: 回测开始日期 (YYYY-MM-DD)
            end_date: 回测结束日期 (YYYY-MM-DD)
            use_adj_nav: 是否使用复权净值（默认True，用于处理分红）
        
        Returns:
            Dict[portfolio_id, BacktestResult]
        """
        logger.info(f"开始回测: {start_date} -> {end_date}")
        logger.info(f"使用复权净值: {use_adj_nav}")

        # 每次运行前清理累积状态，避免多次 run() 结果相互污染
        self._trade_records = []
        self._holdings_history = []
        for pid in self._portfolios:
            self._pending_orders[pid] = []
            self._last_fee_date[pid] = None
            # 重置组合状态
            config = self._configs[pid]
            self._portfolios[pid] = Portfolio(
                portfolio_id=config.portfolio_id,
                portfolio_name=config.portfolio_name,
                cash=Decimal(str(config.initial_cash))
            )
        
        # 1. 获取日历（使用交易日历）
        calendar = self.data_provider.get_calendar(start_date, end_date, calendar_type='trade')
        logger.info(f"回测日历: {len(calendar)} 个交易日")
        
        # 2. 收集所有需要的基金代码
        all_fund_ids = self._collect_fund_ids(start_date, end_date)
        
        # 3. 加载净值数据（使用复权净值处理分红）
        nav_type = 'adj_nav' if use_adj_nav else 'unit_nav'
        nav_panel = self.data_provider.get_fund_nav(list(all_fund_ids), start_date, end_date, nav_type=nav_type)
        nav_panel = self._align_nav_panel(nav_panel, calendar)
        logger.info(f"净值面板: {nav_panel.shape}, 净值类型: {nav_type}")
        
        # 4. 不再加载费率数据，使用配置中的统一费率
        # 费率在 PortfolioConfig 中统一设置
        logger.info("使用配置中的统一费率")
        
        # 5. 加载调仓记录并归一化权重
        rebalance_records = {}
        for pid in self._portfolios:
            records = self.data_provider.get_rebalance_records(pid, start_date, end_date)
            records = self._process_rebalance_records(records, pid)
            # 权重归一化
            records = self._normalize_weights(records, pid)
            rebalance_records[pid] = records
        
        # 6. 主循环
        value_series = {pid: [] for pid in self._portfolios}
        
        for dt in calendar:
            dt_date = dt.date() if hasattr(dt, 'date') else dt
            nav_today = nav_panel.loc[dt] if dt in nav_panel.index else None
            
            for pid, portfolio in self._portfolios.items():
                config = self._configs[pid]
                
                # 6.0 结算到期的待到账赎回资金
                settled = portfolio.settle_pending_redeem(dt_date)
                if settled > 0:
                    logger.debug(f"组合 {pid} 赎回资金到账: {float(settled):.2f}")
                
                # 6.1 结算前日冻结的订单
                self._settle_orders(portfolio, config, nav_today, dt_date, calendar)
                
                # 6.2 检查是否有调仓
                if pid in rebalance_records:
                    self._check_rebalance(portfolio, config, rebalance_records[pid], 
                                         nav_today, dt_date, calendar)
                
                # 6.2.1 立即结算当天创建的赎回订单（T日赎回，T日确认净值）
                self._settle_orders(portfolio, config, nav_today, dt_date, calendar)
                
                # 6.3 更新净值
                if nav_today is not None:
                    portfolio.update_nav(nav_today, dt_date)
                
                # 6.4 扣除管理费
                self._deduct_management_fee(portfolio, config, dt_date)
                
                # 6.5 记录市值
                value_series[pid].append({
                    'date': dt_date,
                    'market_value': float(portfolio.market_value)
                })
                
                # 6.6 记录持仓快照
                self._holdings_history.append(portfolio.to_dataframe())
        
        # 7. 生成结果
        results = {}
        for pid in self._portfolios:
            results[pid] = self._generate_result(pid, value_series[pid])
        
        logger.info("回测完成")
        return results

    def _normalize_weights(self, records: pd.DataFrame, portfolio_id: str) -> pd.DataFrame:
        """
        归一化调仓记录中的权重
        
        对每个调仓日期的权重进行归一化，确保总和为1。
        如果权重总和偏离1超过1%，记录警告。
        """
        if records.empty or 'target_weight' not in records.columns:
            return records
        
        records = records.copy()
        
        # 按调仓日期分组归一化
        for rebalance_date in records['rebalance_date'].unique():
            mask = records['rebalance_date'] == rebalance_date
            weights = records.loc[mask, 'target_weight']
            weight_sum = weights.sum()
            
            # 检查权重总和
            if abs(weight_sum - 1.0) > 0.01:
                logger.warning(
                    f"组合 {portfolio_id} 在 {rebalance_date} 的权重总和为 {weight_sum:.4f}，"
                    f"偏离1.0超过1%，将自动归一化"
                )
            
            # 归一化
            if weight_sum > 0:
                records.loc[mask, 'target_weight'] = weights / weight_sum
            else:
                logger.error(f"组合 {portfolio_id} 在 {rebalance_date} 的权重总和为0，无法归一化")
        
        return records
    
    def _collect_fund_ids(self, start_date: str, end_date: str) -> set:
        """收集所有需要的基金代码"""
        fund_ids = set()
        for pid in self._portfolios:
            records = self.data_provider.get_rebalance_records(pid, start_date, end_date)
            if 'fund_id' in records.columns:
                fund_ids.update(records['fund_id'].unique())
        return fund_ids
    
    def _align_nav_panel(self, nav_panel: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
        """对齐净值面板到日历"""
        nav_panel = nav_panel.reindex(calendar)
        nav_panel = nav_panel.ffill()  # 前值填充
        return nav_panel
    
    def _process_rebalance_records(self, records: pd.DataFrame, portfolio_id: str) -> pd.DataFrame:
        """处理调仓记录，添加调仓编号和下次调仓日期"""
        if records.empty:
            return records
        
        records = records.copy()
        # 确保日期格式
        if 'rebalance_date' in records.columns:
            records['rebalance_date'] = pd.to_datetime(records['rebalance_date'])
        
        # 添加调仓编号
        unique_dates = records['rebalance_date'].unique()
        unique_dates = sorted(unique_dates)
        date_to_id = {d: i+1 for i, d in enumerate(unique_dates)}
        records['rebalance_id'] = records['rebalance_date'].map(date_to_id)
        
        # 添加下次调仓日期
        next_dates = {unique_dates[i]: unique_dates[i+1] for i in range(len(unique_dates)-1)}
        next_dates[unique_dates[-1]] = pd.Timestamp.max
        records['next_rebalance_date'] = records['rebalance_date'].map(next_dates)
        
        return records
    
    def _check_rebalance(self, portfolio: Portfolio, config: PortfolioConfig,
                         records: pd.DataFrame, nav_today: pd.Series, 
                         dt: date, calendar: pd.DatetimeIndex) -> None:
        """检查并执行调仓"""
        if records.empty or nav_today is None:
            return
        
        # 找到当前有效的目标持仓
        dt_ts = pd.Timestamp(dt)
        mask = (records['rebalance_date'] <= dt_ts) & (records['next_rebalance_date'] > dt_ts)
        current_target = records[mask]
        
        if current_target.empty:
            return
        
        rebalance_id = current_target['rebalance_id'].iloc[0]
        rebalance_date = current_target['rebalance_date'].iloc[0]

        # 计算生效/确认日期：以 rebalance_date 对齐到“最近的下一交易日”，再按交易日偏移
        calendar_index = pd.DatetimeIndex(calendar)
        calendar_list = list(calendar_index)
        rebalance_ts = pd.Timestamp(rebalance_date)

        base_idx = int(calendar_index.searchsorted(rebalance_ts, side='left'))
        if base_idx >= len(calendar_list):
            return

        effective_date_redeem = calendar_list[
            min(base_idx + config.rebalance_effective_delay, len(calendar_list) - 1)
        ]
        effective_date_purchase = calendar_list[
            min(base_idx + config.rebalance_delay, len(calendar_list) - 1)
        ]
        effective_date_initial = effective_date_purchase

        effective_date_redeem = effective_date_redeem.date() if hasattr(effective_date_redeem, 'date') else effective_date_redeem
        effective_date_purchase = effective_date_purchase.date() if hasattr(effective_date_purchase, 'date') else effective_date_purchase
        effective_date_initial = effective_date_initial.date() if hasattr(effective_date_initial, 'date') else effective_date_initial
        
        # 首次建仓（使用 rebalance_delay，即 T+N 确认）
        if rebalance_id == 1 and dt == effective_date_initial:
            self._generate_purchase_orders(portfolio, config, current_target, nav_today, dt, rebalance_id)
        # 调仓赎回日（使用 rebalance_effective_delay，即 T+1 生效）
        elif rebalance_id > 1 and dt == effective_date_redeem:
            self._generate_redeem_orders(portfolio, config, current_target, nav_today, dt, rebalance_id)
        # 调仓申购日
        elif rebalance_id > 1 and dt == effective_date_purchase:
            self._generate_purchase_orders(portfolio, config, current_target, nav_today, dt, rebalance_id)
    
    def _generate_redeem_orders(self, portfolio: Portfolio, config: PortfolioConfig,
                                target: pd.DataFrame, nav_today: pd.Series, 
                                dt: date, rebalance_id: int) -> None:
        """生成赎回订单"""
        target_weights = target.set_index('fund_id')['target_weight'].to_dict()
        total_value = float(portfolio.market_value)
        
        for fund_id, pos in list(portfolio.positions.items()):
            target_weight = target_weights.get(fund_id, 0)
            current_weight = float(pos.market_value) / total_value if total_value > 0 else 0
            
            if current_weight > target_weight:
                # 需要赎回
                redeem_value = (current_weight - target_weight) * total_value
                nav = float(nav_today.get(fund_id, pos.nav))
                units_to_redeem = Decimal(str(redeem_value / nav))
                units_to_redeem = min(units_to_redeem, pos.units)
                
                if units_to_redeem > 0:
                    order = Order(
                        portfolio_id=portfolio.portfolio_id,
                        fund_id=fund_id,
                        fund_name=pos.fund_name,
                        side=OrderSide.SELL,
                        units=units_to_redeem,
                        nav=Decimal(str(nav)),
                        create_date=dt,
                        rebalance_id=rebalance_id
                    )
                    if not portfolio.freeze_units(fund_id, units_to_redeem):
                        logger.warning(
                            f"组合 {portfolio.portfolio_id} 赎回冻结失败: {fund_id}, "
                            f"units={float(units_to_redeem):.4f}"
                        )
                        continue
                    order.status = OrderStatus.FROZEN
                    self._pending_orders[portfolio.portfolio_id].append(order)

    def _generate_purchase_orders(self, portfolio: Portfolio, config: PortfolioConfig,
                                  target: pd.DataFrame, nav_today: pd.Series,
                                  dt: date, rebalance_id: int) -> None:
        """生成申购订单"""
        target_weights = target.set_index('fund_id')['target_weight'].to_dict()
        fund_names = target.set_index('fund_id').get('fund_name', pd.Series()).to_dict()
        total_value = float(portfolio.market_value)
        available_cash = float(portfolio.cash)
        
        orders_to_create = []
        total_purchase_amount = Decimal(0)
        
        for fund_id, target_weight in target_weights.items():
            pos = portfolio.get_position(fund_id)
            current_weight = float(pos.market_value) / total_value if pos and total_value > 0 else 0
            
            if target_weight > current_weight:
                # 需要申购
                purchase_value = (target_weight - current_weight) * total_value
                orders_to_create.append({
                    'fund_id': fund_id,
                    'fund_name': fund_names.get(fund_id, ''),
                    'amount': Decimal(str(purchase_value))
                })
                total_purchase_amount += Decimal(str(purchase_value))
        
        # 按可用现金比例调整
        if total_purchase_amount > 0 and available_cash > 0:
            scale = min(Decimal(str(available_cash)) / total_purchase_amount, Decimal(1))
            
            for order_info in orders_to_create:
                amount = (order_info['amount'] * scale).quantize(Decimal('0.01'))
                if amount > 0:
                    nav = Decimal(str(nav_today.get(order_info['fund_id'], 1)))
                    order = Order(
                        portfolio_id=portfolio.portfolio_id,
                        fund_id=order_info['fund_id'],
                        fund_name=order_info['fund_name'],
                        side=OrderSide.BUY,
                        amount=amount,
                        nav=nav,
                        create_date=dt,
                        rebalance_id=rebalance_id
                    )
                    if not portfolio.freeze_cash(amount):
                        logger.warning(
                            f"组合 {portfolio.portfolio_id} 申购冻结失败: {order_info['fund_id']}, "
                            f"amount={float(amount):.2f}, cash={float(portfolio.cash):.2f}"
                        )
                        continue
                    order.status = OrderStatus.FROZEN
                    self._pending_orders[portfolio.portfolio_id].append(order)
    
    def _settle_orders(self, portfolio: Portfolio, config: PortfolioConfig,
                       nav_today: pd.Series, dt: date, calendar: pd.DatetimeIndex) -> None:
        """结算冻结的订单"""
        pending = self._pending_orders.get(portfolio.portfolio_id, [])
        settled = []
        
        # 转换日历为列表用于计算到账日期
        calendar_list = list(calendar)
        calendar_dates = [d.date() if hasattr(d, 'date') else d for d in calendar_list]
        
        for order in pending:
            if order.status != OrderStatus.FROZEN:
                continue
            
            nav = Decimal(str(nav_today.get(order.fund_id, order.nav))) if nav_today is not None else order.nav

            # 净值缺失/非法时，取消订单并解冻，避免冻结状态卡死或 Decimal 运算异常
            nav_invalid = (
                nav is None
                or (isinstance(nav, Decimal) and (not nav.is_finite()))
                or (isinstance(nav, Decimal) and nav.is_finite() and nav <= 0)
            )
            if nav_invalid:
                if order.side == OrderSide.BUY and order.amount is not None:
                    portfolio.unfreeze_cash(order.amount)
                elif order.side == OrderSide.SELL and order.units is not None:
                    portfolio.unfreeze_units(order.fund_id, order.units)

                order.status = OrderStatus.CANCELLED
                order.settle_date = dt
                logger.warning(
                    f"组合 {portfolio.portfolio_id} 订单因净值缺失/非法被取消: "
                    f"fund_id={order.fund_id}, side={order.side}, date={dt}"
                )
                settled.append(order)
                continue
            
            if order.side == OrderSide.BUY:
                # 申购结算 - 使用配置的统一费率
                fee_rate = Decimal(str(config.purchase_fee_rate))
                fee = (order.amount * fee_rate).quantize(Decimal('0.01'))
                
                units = portfolio.execute_purchase(
                    order.fund_id, order.amount, nav, fee,
                    order.fund_name, order.rebalance_id
                )
                order.units = units
                order.fee = fee
                order.nav = nav
                order.settle_date = dt
                order.status = OrderStatus.FILLED
                
            elif order.side == OrderSide.SELL:
                # 赎回结算 - 使用配置的统一费率
                # T日确认净值，T+redeem_settle_delay日资金到账
                fee_rate = Decimal(str(config.redeem_fee_rate))
                gross = order.units * nav
                fee = (gross * fee_rate).quantize(Decimal('0.01'))
                
                # 执行赎回但不直接加到现金
                amount = portfolio.execute_redeem(order.fund_id, order.units, nav, fee, add_to_cash=False)
                
                # 计算资金到账日期（T+redeem_settle_delay）
                try:
                    current_idx = calendar_dates.index(dt)
                    settle_idx = min(current_idx + config.redeem_settle_delay, len(calendar_dates) - 1)
                    cash_settle_date = calendar_dates[settle_idx]
                except ValueError:
                    # 如果当前日期不在日历中，使用当前日期
                    cash_settle_date = dt
                
                # 将资金加入待到账队列
                portfolio.add_pending_redeem(amount, cash_settle_date)
                logger.debug(
                    f"组合 {portfolio.portfolio_id} 赎回结算 {order.fund_id}: "
                    f"金额={float(amount):.2f}, 结算日={dt}, 到账日={cash_settle_date}"
                )
                
                order.amount = amount
                order.fee = fee
                order.nav = nav
                order.settle_date = dt
                order.status = OrderStatus.FILLED
            
            settled.append(order)
            self._trade_records.append(order.to_dict())
        
        # 移除已结算订单
        self._pending_orders[portfolio.portfolio_id] = [
            o for o in pending if o.status == OrderStatus.FROZEN
        ]
    
    def _deduct_management_fee(self, portfolio: Portfolio, config: PortfolioConfig, dt: date) -> None:
        """
        扣除管理费（从现金扣除）
        
        按日计提管理费，从现金账户扣除。
        """
        if config.management_fee <= 0:
            return

        last_fee_date = self._last_fee_date.get(portfolio.portfolio_id)
        if last_fee_date is None:
            # 首日不扣费，只记录起点
            self._last_fee_date[portfolio.portfolio_id] = dt
            return

        days = (dt - last_fee_date).days
        if days <= 0:
            return
        
        # 计算管理费
        daily_fee = float(portfolio.market_value) * config.management_fee / 365 * days
        fee_amount = Decimal(str(daily_fee))
        
        # 从现金扣除
        if portfolio.cash >= fee_amount:
            portfolio.cash -= fee_amount
            logger.debug(f"组合 {portfolio.portfolio_id} 扣除管理费: {float(fee_amount):.2f}")
        else:
            # 现金不足时仅记录 debug 级别日志，避免大量警告
            logger.debug(
                f"组合 {portfolio.portfolio_id} 现金不足以支付管理费 {float(fee_amount):.2f}，"
                f"当前现金: {float(portfolio.cash):.2f}"
            )

        # 无论是否足额扣费，都推进计提日期，避免后续重复按更长天数累计扣费
        self._last_fee_date[portfolio.portfolio_id] = dt
    
    def _generate_result(self, portfolio_id: str, value_series: List[dict]) -> BacktestResult:
        """生成回测结果"""
        df = pd.DataFrame(value_series)
        df.set_index('date', inplace=True)
        
        initial_value = self._configs[portfolio_id].initial_cash
        nav_series = df['market_value'] / initial_value
        returns = nav_series.pct_change().dropna()
        
        metrics = self.analyzer.calculate_metrics(returns, nav_series)
        
        trades_df = pd.DataFrame(self._trade_records)
        trades_df = trades_df[trades_df['portfolio_id'] == portfolio_id] if not trades_df.empty else trades_df
        
        holdings_df = pd.concat(self._holdings_history, ignore_index=True) if self._holdings_history else pd.DataFrame()
        holdings_df = holdings_df[holdings_df['portfolio_id'] == portfolio_id] if not holdings_df.empty else holdings_df
        
        return BacktestResult(
            portfolio_id=portfolio_id,
            nav_series=nav_series,
            returns=returns,
            trades=trades_df,
            holdings_history=holdings_df,
            metrics=metrics
        )
