
import tqdm
import os
import json
import datetime
import time
import csv
from typing import Union, List, Dict
import traceback
from .model import *
from . import datasourcing

from itertools import zip_longest

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def sync_stock_basic():
    data:list = []
    data.extend(datasourcing.stock.stock_basic(list_status='L'))
    data.extend(datasourcing.stock.stock_basic(list_status='D'))
    data.extend(datasourcing.stock.stock_basic(list_status='P'))

    for each_data in data:
        record, created = StockBasic.get_or_create(
            code=each_data['code'],
            defaults={
                'symbol': each_data['symbol'],
                'name': each_data['name'],
                'area': each_data['area'],
                'industry': each_data['industry'],
                'fullname': each_data['fullname'],
                'enname': each_data['enname'],
                'market': each_data['market'],
                'exchange': each_data['exchange'],
                'curr_type': each_data['curr_type'],
                'list_status': each_data['list_status'],
                'list_date': each_data['list_date'],
                'delist_date': each_data['delist_date'],
                'is_hs': each_data['is_hs'],
            }
            )

def sync_stock_company():
    sse_data = datasourcing.stock.stock_company(exchange='SSE')
    szse_data = datasourcing.stock.stock_company(exchange='SZSE')
    for each_data in sse_data:
        record, created = StockCompany.get_or_create(
            code=each_data['code'],
            defaults={
                'exchange': each_data['exchange'],
                'chairman': each_data['chairman'],
                'manager': each_data['manager'],
                'secretary': each_data['secretary'],
                'reg_capital': each_data['reg_capital'],
                'setup_date': each_data['setup_date'],
                'province': each_data['province'],
                'city': each_data['city'],
                'introduction': each_data['introduction'],
                'website': each_data['website'],
                'email': each_data['email'],
                'office': each_data['office'],
                'employees': each_data['employees'],
                'main_business': each_data['main_business'],
                'business_scope': each_data['business_scope'],
                }
            )
    for each_data in szse_data:
        record, created = StockCompany.get_or_create(
            code=each_data['code'],
            defaults={
                'exchange': each_data['exchange'],
                'chairman': each_data['chairman'],
                'manager': each_data['manager'],
                'secretary': each_data['secretary'],
                'reg_capital': each_data['reg_capital'],
                'setup_date': each_data['setup_date'],
                'province': each_data['province'],
                'city': each_data['city'],
                'introduction': each_data['introduction'],
                'website': each_data['website'],
                'email': each_data['email'],
                'office': each_data['office'],
                'employees': each_data['employees'],
                'main_business': each_data['main_business'],
                'business_scope': each_data['business_scope'],
                }
            )

def sync_trade_cal(day_limit:Union[int, None]=None):
    if day_limit is not None:
        start_datetime = datetime.datetime.now()
        start_datetime = start_datetime - datetime.timedelta(days=day_limit)

        data:list = []
        data.extend(datasourcing.stock.trade_cal(exchange='SSE', start_date=start_datetime.strftime('%Y%m%d')))
        data.extend(datasourcing.stock.trade_cal(exchange='SZSE',start_date=start_datetime.strftime('%Y%m%d')))
    else:
        data:list = []
        data.extend(datasourcing.stock.trade_cal(exchange='SSE'))
        data.extend(datasourcing.stock.trade_cal(exchange='SZSE'))

    for each_data in data:
        record, created = TradeCal.get_or_create(
            exchange=each_data['exchange'],
            cal_date=each_data['cal_date'],
            defaults={
                'is_open': each_data['is_open'],
            }
            )

def sync_stk_managers():
    for each_stock_list in tqdm.tqdm(iterable=list(grouper(StockBasic.select(), 16))):
        each_stock_data = datasourcing.stock.stk_managers(code=','.join([each_c.code for each_c in each_stock_list if each_c is not None]))
        for each_data in each_stock_data:
            record, created = StockManagers.get_or_create(
                code=each_data['code'],
                defaults={
                    'ann_date': each_data['ann_date'],
                    'name': each_data['name'],
                    'gender': each_data['gender'],
                    'lev': each_data['lev'],
                    'title': each_data['title'],
                    'edu': each_data['edu'],
                    'national': each_data['national'],
                    'birthday': each_data['birthday'],
                    'begin_date': each_data['begin_date'],
                    'end_date': each_data['end_date'],
                    'resume': each_data['resume'],
                    }
                )

def sync_stock_daily(day_limit:Union[int, None]=None):
    date_list = []
    if day_limit is not None:
        start_datetime = datetime.datetime.now()
        for i in range(day_limit):
            cur_date = start_datetime - datetime.timedelta(days=i)
            date_list.append(cur_date.strftime('%Y%m%d'))
    else:
        for each_date in TradeCal.select().where(TradeCal.exchange=='SSE', TradeCal.is_open == 1):
            date_list.append(each_date.cal_date.strftime('%Y%m%d'))
    # 遍历所有交易日期
    for each_date in tqdm.tqdm(iterable=date_list):
        retry_cnt = 0
        data = []
        while True:
            try:
                retry_cnt += 1
                if retry_cnt > 5:
                    break
                data = datasourcing.stock.stock_daily(trade_date=each_date)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        if len(data) == 0:
            continue
        
        with db.atomic():
            for each_data in data:
                StockDaily.get_or_create(
                    code = each_data['code'],
                    trade_date = each_data['trade_date'],
                    defaults={
                            'open': each_data['open'],
                            'high': each_data['high'],
                            'low': each_data['low'],
                            'close': each_data['close'],
                            'pre_close': each_data['pre_close'],
                            'change': each_data['change'],
                            'pct_chg': each_data['pct_chg'],
                            'vol': each_data['vol'],
                            'amount': each_data['amount'],
                    }
                )


def sync_stock_daily_basic(day_limit:Union[int, None]=None):
    date_list = []
    if day_limit is not None:
        start_datetime = datetime.datetime.now()
        for i in range(day_limit):
            cur_date = start_datetime - datetime.timedelta(days=i)
            date_list.append(cur_date.strftime('%Y%m%d'))
    else:
        for each_date in TradeCal.select().where(TradeCal.exchange=='SSE', TradeCal.is_open == 1):
            date_list.append(each_date.cal_date.strftime('%Y%m%d'))
    # 遍历所有交易日期
    for each_date in tqdm.tqdm(iterable=date_list):
        retry_cnt = 0
        data = []
        while True:
            try:
                retry_cnt += 1
                if retry_cnt > 5:
                    break
                data = datasourcing.stock.daily_basic(trade_date=each_date)
                break
            except:
                time.sleep(5)

        if len(data) == 0:
            continue
        
        with db.atomic():
            for each_data in data:
                StockDailyBasic.get_or_create(
                    code = each_data['code'],
                    trade_date = each_data['trade_date'],
                    defaults={
                        'close': each_data['close'],
                        'turnover_rate': each_data['turnover_rate'],
                        'turnover_rate_f': each_data['turnover_rate_f'],
                        'volume_ratio': each_data['volume_ratio'],
                        'pe': each_data['pe'],
                        'pe_ttm': each_data['pe_ttm'],
                        'pb': each_data['pb'],
                        'ps': each_data['ps'],
                        'ps_ttm': each_data['ps_ttm'],
                        'dv_ratio': each_data['dv_ratio'],
                        'dv_ttm': each_data['dv_ttm'],
                        'total_share': each_data['total_share'],
                        'float_share': each_data['float_share'],
                        'free_share': each_data['free_share'],
                        'total_mv': each_data['total_mv'],
                        'circ_mv': each_data['circ_mv'],
                    }
                )


def sync_stock_weekly(day_limit:Union[int, None]=None):
    date_list = []
    if day_limit is not None:
        start_datetime = datetime.datetime.now()
        for i in range(day_limit):
            cur_date = start_datetime - datetime.timedelta(days=i)
            date_list.append(cur_date.strftime('%Y%m%d'))
    else:
        for each_date in TradeCal.select().where(TradeCal.exchange=='SSE', TradeCal.is_open == 1):
            date_list.append(each_date.cal_date.strftime('%Y%m%d'))
    # 遍历所有交易日期
    for each_date in tqdm.tqdm(iterable=date_list):
        while True:
            try:
                data = datasourcing.stock.stock_weekly(trade_date=each_date)
                break
            except:
                time.sleep(5)

        if len(data) == 0:
            continue
        
        with db.atomic():
            for each_data in data:
                StockWeekly.get_or_create(
                    code = each_data['code'],
                    trade_date = each_data['trade_date'],
                    defaults={
                            'open': each_data['open'],
                            'high': each_data['high'],
                            'low': each_data['low'],
                            'close': each_data['close'],
                            'pre_close': each_data['pre_close'],
                            'change': each_data['change'],
                            'pct_chg': each_data['pct_chg'],
                            'vol': each_data['vol'],
                            'amount': each_data['amount'],
                    }
                )

def sync_stock_hkhold(day_limit:Union[int, None]=None):
    date_list = []
    if day_limit is not None:
        start_datetime = datetime.datetime.now()
        for i in range(day_limit):
            cur_date = start_datetime - datetime.timedelta(days=i)
            date_list.append(cur_date.strftime('%Y%m%d'))
    else:
        for each_date in TradeCal.select().where(TradeCal.exchange=='SSE', TradeCal.is_open == 1):
            if each_date.cal_date < datetime.datetime.now().date():
                date_list.append(each_date.cal_date.strftime('%Y%m%d'))
    # 遍历所有交易日期
    for each_date in tqdm.tqdm(iterable=list(reversed(date_list))):
        retry_cnt = 0
        data = []
        while True:
            try:
                retry_cnt += 1
                if retry_cnt > 5:
                    break
                data = datasourcing.stock.hk_hold(trade_date=each_date)
                break
            except Exception as e:
                print(e)
                time.sleep(5)

        if len(data) == 0:
            continue
        
        with db.atomic():
            for each_data in data:
                StockHKHold.get_or_create(
                    code = each_data['code'],
                    trade_date = each_data['trade_date'],
                    defaults={
                            'ex_code': each_data['ex_code'],
                            'name': each_data['name'],
                            'ratio': each_data['ratio'],
                            'vol': each_data['vol'],
                            'exchange': each_data['exchange'],
                    }
                )

'''
def init_stock_monthly():
    client = pymongo.MongoClient(config['MongoDB'])
    db = client[config['DatabaseName']]
    codec_options = CodecOptions(type_registry=type_registry)
    stocks_collection = db.get_collection('stock_basic', codec_options=codec_options)
    collection = db.get_collection('stock_monthly', codec_options=codec_options)

    # create index
    if 'code' not in [index['name'] for index in collection.list_indexes()]:
        collection.create_index(
            [('code', pymongo.ASCENDING)],
            name = 'code'
        )

    if 'trade_date' not in [index['name'] for index in collection.list_indexes()]:
        collection.create_index(
            [('trade_date', pymongo.ASCENDING)],
            name = 'trade_date'
        )

    # 需要查询的股票
    stocks = [each_stock for each_stock in stocks_collection.find({'list_status': 'L'})]
    random.shuffle(stocks)

    # 因为每次最多返回5000条，每次查询一支股票
    for each_stock in tqdm.tqdm(iterable=stocks, ascii=True):
        # 读取该股票所有的记录到内存，方便快速去重
        records = collection.find(
            {
                "code": each_stock['code']
            }
        )
        code_date_pairs = [
            (each_record['code'], each_record['trade_date']) \
                for each_record in records
            ]
        data:list = None
        for _ in range(10):
            try:
                data = stock_monthly(
                    code=each_stock['code']
                    )
                break
            except Exception as e:
                print(e)
                time.sleep(30)
            except requests.exceptions.ConnectionError as e:
                print(e)
                time.sleep(10)
    
        if len(data) == 0:
            continue

        data_inserted = []
        for each_data in data:
            if (each_data['code'], each_data['trade_date']) not in code_date_pairs:
                data_inserted.append(
                    {
                        'code': each_data['code'],
                        'trade_date': each_data['trade_date'],
                        'open': each_data['open'],
                        'high': each_data['high'],
                        'low': each_data['low'],
                        'close': each_data['close'],
                        'pre_close': each_data['pre_close'],
                        'change': each_data['change'],
                        'pct_chg': each_data['pct_chg'],
                        'vol': each_data['vol'],
                        'amount': each_data['amount'],
                    }
                )
        # insert many
        if data_inserted != []:
            collection.insert_many(data_inserted)


def sync_stock_monthly():
    client = pymongo.MongoClient(config['MongoDB'])
    db = client[config['DatabaseName']]
    codec_options = CodecOptions(type_registry=type_registry)
    stocks_collection = db.get_collection('stock_basic', codec_options=codec_options)
    collection = db.get_collection('stock_monthly', codec_options=codec_options)

    # create index
    if 'code' not in [index['name'] for index in collection.list_indexes()]:
        collection.create_index(
            [('code', pymongo.ASCENDING)],
            name = 'code'
        )

    if 'trade_date' not in [index['name'] for index in collection.list_indexes()]:
        collection.create_index(
            [('trade_date', pymongo.ASCENDING)],
            name = 'trade_date'
        )


    # 因为每次最多返回5000条，同步从今天之前两个月
    start_time = datetime.datetime.now().date() - datetime.timedelta(days=60)
    date_list = []
    while start_time <= datetime.datetime.now().date():
        date_list.append(start_time)
        start_time += datetime.timedelta(days=1)
    
    for each_date in tqdm.tqdm(iterable=date_list, ascii=True):
        data:list = None
        for _ in range(10):
            try:
                data = stock_monthly(
                    trade_date=each_date.strftime('%Y%m%d'),
                    )
                break
            except Exception as e:
                print(e)
                time.sleep(30)
            except requests.exceptions.ConnectionError as e:
                print(e)
                time.sleep(10)

        for each_data in data:
            collection.update_one(
                {
                    'code': each_data['code'],
                    'trade_date': each_data['trade_date'],
                },
                {
                    '$set': {
                        'open': each_data['open'],
                        'high': each_data['high'],
                        'low': each_data['low'],
                        'close': each_data['close'],
                        'pre_close': each_data['pre_close'],
                        'change': each_data['change'],
                        'pct_chg': each_data['pct_chg'],
                        'vol': each_data['vol'],
                        'amount': each_data['amount'],
                    }
                },
                upsert=True
            )

'''
