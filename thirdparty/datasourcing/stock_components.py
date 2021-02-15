import tushare as ts
from .model import *
import datetime

token = "adb473ce63b4b7fde30d7af35947b3b7fb1ec517d5898ce063a64e9d"
#token = '54d49770ed4383ddefb0b1411494660c257830a6dd2c295b265fde22'
ts.set_token(token)
pro = ts.pro_api()

def is_trade_date(date:datetime.date) -> bool:
    r = TradeCal.get_or_none(TradeCal.cal_date == date, is_open=1)
    return r is not None

def find_nearest_trade_date(date:datetime.date) -> datetime.date:
    cur_date = date
    for _i in range(100):
        if is_trade_date(cur_date):
            return cur_date
        else:
            cur_date -= datetime.timedelta(days=1)

    return cur_date

def get_rec_stock_list(cur_date=None, topk=5, daydelta=1):
    if cur_date is None:
        # 获取所有股票港资持股比例
        cur_date = find_nearest_trade_date(datetime.datetime.now().date() - datetime.timedelta(days=daydelta))
    else:
        cur_date = find_nearest_trade_date(cur_date)

    cur_hk_result = StockHKHold.select()\
        .where(
            StockHKHold.trade_date==cur_date,
            ((StockHKHold.exchange == 'SZ') | (StockHKHold.exchange == 'SH'))
            ).order_by(StockHKHold.ratio)
    stock_list = {}
    stock_vol = {}
    for each_result in cur_hk_result:
        if each_result.ratio is not None and float(each_result.ratio) / 100.0 > 0.1:
            stock_list[each_result.code] = float(each_result.ratio) / 100.0  - 0.1
            stock_vol[each_result.code] = float(each_result.vol)

    # 持仓情况
    # 3, 5, 10, 60
    last_day3 = find_nearest_trade_date(cur_date-datetime.timedelta(days=3))
    day3_hk_result = StockHKHold.select()\
        .where(
            StockHKHold.trade_date==last_day3,
            ((StockHKHold.exchange == 'SZ') | (StockHKHold.exchange == 'SH'))
            ).order_by(-StockHKHold.vol)
    
    for each_result in day3_hk_result:
        if each_result.code in stock_list:
            cur_price = StockDaily.get_or_none(
                StockDaily.trade_date == cur_date,
                StockDaily.code == each_result.code
            )
            cur_price = float(cur_price.close)
            change3 = stock_vol[each_result.code] - float(each_result.vol)
            
            stock_list[each_result.code] += 0.2 * change3 * cur_price / 1e8
            if change3 < 0:
                del stock_list[each_result.code]

    last_day5 = find_nearest_trade_date(cur_date-datetime.timedelta(days=5))
    day5_hk_result = StockHKHold.select()\
        .where(
            StockHKHold.trade_date==last_day5,
            ((StockHKHold.exchange == 'SZ') | (StockHKHold.exchange == 'SH'))
            ).order_by(-StockHKHold.vol)
    
    for each_result in day5_hk_result:
        if each_result.code in stock_list:
            cur_price = StockDaily.get_or_none(
                StockDaily.trade_date == cur_date,
                StockDaily.code == each_result.code
            )
            cur_price = float(cur_price.close)
            change5 = stock_vol[each_result.code] - float(each_result.vol)
            stock_list[each_result.code] += 0.3 * change5 * cur_price / 1e8

            if change5 < 0:
                del stock_list[each_result.code]

    last_day10 = find_nearest_trade_date(cur_date-datetime.timedelta(days=10))
    day10_hk_result = StockHKHold.select()\
        .where(
            StockHKHold.trade_date==last_day10,
            ((StockHKHold.exchange == 'SZ') | (StockHKHold.exchange == 'SH'))
            ).order_by(-StockHKHold.vol)

    for each_result in day10_hk_result:
        if each_result.code in stock_list:
            cur_price = StockDaily.get_or_none(
                StockDaily.trade_date == cur_date,
                StockDaily.code == each_result.code
            )
            cur_price = float(cur_price.close)
            change10 = stock_vol[each_result.code] - float(each_result.vol)
            stock_list[each_result.code] += 0.3 * change10 * cur_price / 1e8
    
    last_day60 = find_nearest_trade_date(cur_date-datetime.timedelta(days=60))
    day60_hk_result = StockHKHold.select()\
        .where(
            StockHKHold.trade_date==last_day60,
            ((StockHKHold.exchange == 'SZ') | (StockHKHold.exchange == 'SH'))
            ).order_by(-StockHKHold.vol)
    
    for each_result in day60_hk_result:
        if each_result.code in stock_list:
            cur_price = StockDaily.get_or_none(
                StockDaily.trade_date == cur_date,
                StockDaily.code == each_result.code
            )
            cur_price = float(cur_price.close)
            change60 = stock_vol[each_result.code] - float(each_result.vol)
            stock_list[each_result.code] += 0.2 * change60 * cur_price / 1e8

    ret_stock_list = []
    for k, v in sorted(stock_list.items(), key=lambda item: -item[1]):
        r = StockBasic.get_or_none(StockBasic.code == k)
        dr = StockDaily.get_or_none(
            StockDaily.code == k,
            StockDaily.trade_date == cur_date
            )
        br = StockDailyBasic.get_or_none(
            StockDailyBasic.code == k,
            StockDailyBasic.trade_date == cur_date
        )
        each_data = {
            'code': k,
            'name': r.name,
            'rec': v,
            'close': float(dr.close),
            'total_mv': float(br.total_mv),
        }
        if r is not None:
            ret_stock_list.append(each_data)

    print(ret_stock_list[:topk])
    return ret_stock_list[:topk]

if __name__ == "__main__":
    get_rec_stock_list()
