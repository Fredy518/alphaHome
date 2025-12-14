from .tushare_fund_adjfactor import TushareFundAdjFactorTask
from .tushare_fund_basic import TushareFundBasicTask
from .tushare_fund_daily import TushareFundDailyTask
from .tushare_fund_etf_basic import TushareFundEtfBasicTask
from .tushare_fund_etf_index import TushareFundEtfIndexTask
from .tushare_fund_factor_pro import TushareFundFactorProTask
from .tushare_fund_manager import TushareFundManagerTask
from .tushare_fund_nav import TushareFundNavTask
from .tushare_fund_portfolio import TushareFundPortfolioTask
from .tushare_fund_share import TushareFundShareTask
from .tushare_fund_dividend import TushareFundDividendTask
from .akshare_fund_cf_em import AkShareFundCfEmTask
# from .ifind_fund_market_forwardlook import iFindFundMarketforwardlookTask
# from .ifind_fund_market_outlook import iFindFundMarketOutlookTask

__all__ = [
    "TushareFundBasicTask",
    "TushareFundDailyTask",
    "TushareFundShareTask",
    "TushareFundNavTask",
    "TushareFundAdjFactorTask",
    "TushareFundPortfolioTask",
    "TushareFundEtfBasicTask",
    "TushareFundEtfIndexTask",
    "TushareFundFactorProTask",
    "TushareFundManagerTask",
    "TushareFundDividendTask",
    "AkShareFundCfEmTask",
    # "iFindFundMarketforwardlookTask",
    # "iFindFundMarketOutlookTask",
]
