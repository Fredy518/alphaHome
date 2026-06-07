#!/usr/bin/env python
# -*- coding: utf-8 -*-

from alphahome.fetchers.sources.akshare.akshare_task import AkShareNoDateSingleBatchTask
from alphahome.fetchers.tasks.cbond.tushare_cbond_rate import TushareCBondRateTask
from alphahome.fetchers.tasks.fund.akshare_fund_cf_em import AkShareFundCfEmTask
from alphahome.fetchers.tasks.fund.akshare_fund_purchase_em import AkShareFundPurchaseEmTask
from alphahome.fetchers.tasks.index.akshare_index_csindex_all import AkShareIndexCsindexAllTask
from alphahome.fetchers.tasks.index.akshare_index_stock_cons_csindex import (
    AkShareIndexStockConsCsindexTask,
)
from alphahome.fetchers.tasks.index.akshare_index_stock_cons_weight_csindex import (
    AkShareIndexStockConsWeightCsindexTask,
)
from alphahome.fetchers.tasks.index.tushare_index_cimember import TushareIndexCiMemberTask
from alphahome.fetchers.tasks.index.tushare_index_swmember import TushareIndexSwmemberTask
from alphahome.fetchers.tasks.stock.tushare_stock_pledgestat import TushareStockPledgeStatTask
from alphahome.fetchers.tasks.stock.tushare_stock_thsindex import TushareStockThsIndexTask
from alphahome.fetchers.tasks.stock.tushare_stock_thsmember import TushareStockThsMemberTask


def test_tushare_full_like_tasks_use_smart_refresh_interval():
    assert TushareCBondRateTask.smart_refresh_interval_days == 30
    assert TushareIndexCiMemberTask.smart_refresh_interval_days == 30
    assert TushareIndexSwmemberTask.smart_refresh_interval_days == 30
    assert TushareStockPledgeStatTask.smart_refresh_interval_days == 30
    assert TushareStockThsIndexTask.smart_refresh_interval_days == 30
    assert TushareStockThsMemberTask.smart_refresh_interval_days == 30


def test_akshare_recent_update_tasks_use_smart_refresh_interval():
    assert AkShareNoDateSingleBatchTask.smart_refresh_interval_days == 1
    assert AkShareFundPurchaseEmTask.smart_refresh_interval_days == 1
    assert AkShareFundCfEmTask.smart_refresh_interval_days == 30
    assert AkShareIndexCsindexAllTask.smart_refresh_interval_days == 30
    assert AkShareIndexStockConsCsindexTask.smart_refresh_interval_days == 30
    assert AkShareIndexStockConsWeightCsindexTask.smart_refresh_interval_days == 30
