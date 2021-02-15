from .stock_task import *


def update(day=3):
    #stock_task.sync_stock_basic()
    #stock_task.sync_trade_cal()
    #stock_task.sync_stock_company()
    #stock_task.sync_stk_managers()
    sync_stock_daily(day_limit=day)
    sync_stock_hkhold(day_limit=day)
    sync_stock_daily_basic(day_limit=day)

if __name__ == '__main__':
    update()
    # stock_task.sync_stock_basic()
    # stock_task.sync_trade_cal()
    # stock_task.sync_stock_daily(day_limit=30)
    # stock_task.sync_stock_daily_basic(day_limit=30)
    # stock_task.sync_stock_weekly(day_limit=30)
