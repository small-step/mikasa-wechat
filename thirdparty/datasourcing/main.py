import datasourcing
import stock_task
from model import *


def main():
    stock_task.sync_stock_basic()
    stock_task.sync_trade_cal()
    stock_task.sync_stock_company()
    stock_task.sync_stk_managers()
    stock_task.sync_stock_daily(day_limit=30)
    stock_task.sync_stock_hkhold(day_limit=30)
    stock_task.sync_stock_daily_basic(day_limit=30)
    # datasourcing.stock.init_stock_weekly()
    # datasourcing.stock.init_stock_monthly()
    # datasourcing.fund.init_fund_basic()
    # datasourcing.fund.init_fund_company()
    # datasourcing.fund.init_fund_manager()
    # datasourcing.fund.init_fund_share()

if __name__ == '__main__':
    main()