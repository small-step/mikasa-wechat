
'''
股票数据接口
'''
import json
import time
import decimal
import datetime
import random
import tempfile
from typing import Dict, List

from lxml import etree
import tqdm
import requests
import pandas as pd
import tushare

from .data import TushareAPI

token = "e546fbc7cc7180006cd08d7dbde0e07f95b21293a924325e89ca504b"

class QuantMewDataAPI(object):
    def __init__(self):
        pass

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36'}

def not_none(s, d):
    if s is None:
        return d
    else:
        return s

'''
接口：stock_basic
描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
参数
is_hs	str	N	是否沪深港通标的，N否 H沪股通 S深股通
list_status	str	N	上市状态： L上市 D退市 P暂停上市，默认L
exchange	str	N	交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
'''
def stock_basic(is_hs: str='', list_status: str='', exchange:str='') -> List[Dict]:
    pro = TushareAPI(token)
    # 查询当前所有正常上市交易的股票列表
    df = pro.stock_basic(
        exchange=exchange,
        list_status=list_status,
        fields='ts_code,symbol,name,area,industry,fullname,\
            enname,market,exchange,curr_type,list_status,\
            list_date,delist_date,is_hs')

    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'symbol': row['symbol'],
                'name': row['name'],
                'area': row['area'],
                'industry': row['industry'],
                'fullname': row['fullname'],
                'enname': row['enname'],
                'market': row['market'],
                'exchange': row['exchange'],
                'curr_type': row['curr_type'],
                'list_status': row['list_status'],
                'list_date': row['list_date'],
                'delist_date': row['delist_date'],
                'is_hs': row['is_hs'],
            }
        )
    return json_data

'''
接口：trade_cal
描述：获取各大交易所交易日历数据,默认提取的是上交所
参数：
名称        类型    必选    描述
exchange	str	    N      交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源,IB 银行间,XHKG 港交所
start_date	str	    N	   开始日期
end_date    str	    N	   结束日期
is_open	    str	    N      是否交易 '0'休市 '1'交易
'''
def trade_cal(exchange:str='', start_date:str='', end_date:str='', is_open:str='') -> List[Dict]:
    pro = TushareAPI(token)
    df = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open=is_open)
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'exchange': row['exchange'],
                'cal_date': row['cal_date'],
                'is_open': row['is_open'] 
            }
        )
    
    return json_data

'''
接口：hs_const
描述：获取沪股通、深股通成分数据

输入参数

名称	类型	必选	描述
hs_type	str	Y	类型SH沪股通SZ深股通
is_new	str	N	是否最新 1 是 0 否 (默认1)

ts_code	str	Y	TS代码
hs_type	str	Y	沪深港通类型SH沪SZ深
in_date	str	Y	纳入日期
out_date	str	Y	剔除日期
is_new	str	Y	是否最新 1是 0否
'''
def hs_const(hs_type:str, is_new:str='1'):
    pro = TushareAPI(token)
    df = pro.query(
        'hs_const',
        parse_float='str',
        hs_type=hs_type,
        is_new=is_new)
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'ts_code': row['ts_code'],
                'hs_type': row['hs_type'],
                'in_date': row['in_date'],
                'out_date': row['out_date'],
                'is_new': row['is_new'],
            }
        )
    
    return json_data

'''
接口：stock_company
描述：获取上市公司基础信息，单次提取4000条，可以根据交易所分批提取
积分：用户需要至少120积分才可以调取，具体请参阅积分获取办法
输入参数：
名称	    类型	必须	 描述
ts_code	    str	    N	    股票代码
exchange	str	    N	    交易所代码 ，SSE上交所 SZSE深交所
'''
def stock_company(code:str='', exchange:str='') -> List[Dict]:
    pro = TushareAPI(token)
    df = pro.query(
        'stock_company',
        parse_float='str',
        ts_code=code,
        exchange=exchange,
        fields='ts_code,exchange,chairman,manager,secretary,\
            reg_capital,setup_date,province,city,introduction,\
            website,email,office,employees,main_business,business_scope'
        )
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'exchange': row['exchange'],
                'chairman': row['chairman'],
                'manager': row['manager'],
                'secretary': row['secretary'],
                'reg_capital': decimal.Decimal(row['reg_capital']),
                'setup_date': row['setup_date'],
                'province': row['province'],
                'city': row['city'],
                'introduction': row['introduction'],
                'website': row['website'],
                'email': row['email'],
                'office': row['office'],
                'employees': row['employees'],
                'main_business': row['main_business'],
                'business_scope': row['business_scope'],
            }
        )
    
    return json_data
'''
获取股票实时数据
'''
def stock_realtime():
    df = tushare.get_today_all()
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'symbol': row['code'],
                'name': row['name'],
                'changepercent': decimal.Decimal(row['changepercent']),
                'trade': decimal.Decimal(row['trade']),
                'open': decimal.Decimal(row['open']),
                'high': decimal.Decimal(row['high']),
                'low': decimal.Decimal(row['low']),
                'volume': decimal.Decimal(row['volume']),
                'turnoverratio': decimal.Decimal(row['turnoverratio']),
                'amount': decimal.Decimal(row['amount']),
                'per': decimal.Decimal(row['per']),
                'pb': decimal.Decimal(row['pb']),
                'mktcap': decimal.Decimal(row['mktcap']),
                'nmc': decimal.Decimal(row['nmc']),
            }
        )
    
    return json_data
'''
获取股票分笔数据
'''
def stock_today_ticks(symbol:str):
    now_datetime = datetime.datetime.now()
    now_date = now_datetime.date()
    df = tushare.get_today_ticks(code=symbol,retry_count=3,pause=0.01)
    json_data = []
    for _index, row in df.iterrows():
        time_obj = datetime.datetime.strptime(row['time'], '%H%M%S').time()
        
        json_data.append(
            {
                'trade_time': datetime.datetime.combine(now_date, time_obj),
                'price': decimal.Decimal(row['price']),
                'volume': decimal.Decimal(row['vol']),
                'type': row['type'],
            }
        )
    
    return json_data

'''
上市公司管理层
接口：stk_managers
描述：获取上市公司管理层

ts_code	str	N	股票代码，支持单个或多个股票输入
ann_date	str	N	公告日期（YYYYMMDD格式，下同）
start_date	str	N	公告开始日期
end_date	str	N	公告结束日期

返回
ts_code	str	Y	TS股票代码
ann_date	str	Y	公告日期
name	str	Y	姓名
gender	str	Y	性别
lev	str	Y	岗位类别
title	str	Y	岗位
edu	str	Y	学历
national	str	Y	国籍
birthday	str	Y	出生年月
begin_date	str	Y	上任日期
end_date	str	Y	离任日期
resume	str	N	个人简历
'''
def stk_managers(code:str="", ann_date:str="", start_date:str="", end_date:str=""):
    pro = TushareAPI(token)
    df = pro.query(
        'stk_managers',
        parse_float='str',
        ts_code=code,
        ann_date=ann_date,
        start_date=start_date,
        end_date=end_date,
        fields='ts_code,ann_date,name,gender,lev,title,edu,\
            national,birthday,begin_date,end_date,resume'
        )
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'ann_date': row['ann_date'],
                'name': row['name'],
                'gender': row['gender'],
                'lev': row['lev'],
                'title': row['title'],
                'edu': row['edu'],
                'national': row['national'],
                'birthday': row['birthday'],
                'begin_date': row['begin_date'],
                'end_date': row['end_date'],
                'resume': row['resume'],
            }
        )
    return json_data
    

'''
获取股票历史分笔数据
'''
def first(data):
    ret = None
    if len(data) > 0:
        ret = data[0]
    else:
        ret = None

    return ret

def is_banned(page):
    # 判断封禁
    page.encoding='utf-8'
    if '拒绝访问' in page.text:
        return True
    else:
        return False

def stock_ticks_sina(code:str, date:datetime.date, proxies:Dict=None):
    c, m = code.split('.')
    web_code = m.lower() + c
    page = 1
    while True:
        url = f'http://market.finance.sina.com.cn/transHis.php?symbol={web_code}&date={date.strftime("%Y-%m-%d")}&page={str(page)}'
        response = None
        try:
            response = requests.get(url, proxies=proxies, headers=headers, timeout=15)
        except Exception as e:
            print('Error: ' + str(e))
            time.sleep(2)
            continue

        if response.status_code != 200:
            if response.status_code == 456:
                # 判断封禁
                print('ip已被新浪封禁')
            else:
                print('ConnectionError with status code:' + str(response.status_code))
            continue
        if response is None:
            continue

        response.encoding='gb2312'
        html = etree.HTML(response.text)

        if html is None:
            print('语法树解析失败')
            continue
        
        # 二次判断封禁
        if is_banned(response):
            print('ip已被新浪封禁')
            continue

        # 没有数据了(可能是假的)
        error_info = first(html.xpath('//div[contains(text(),"输入的代码有误或没有交易数据")]'))
        
        if error_info is not None and page == 1:
            data_complete = True
            break
        elif error_info is not None and page != 1:
            print('May be fake format error, try again later.')
            continue
        
        # 没有数据了
        data = html.xpath('//table[@id="datatbl"]/tbody/tr')
        if len(data) == 0:
            data_complete = True
            break
        
        json_data = []
        for each_row in data:
            成交时间 = first(each_row.xpath('./th[1]/text()'))
            成交价 = first(each_row.xpath('./td[1]/text()'))
            价格变动 = first(each_row.xpath('./td[2]/text()'))
            成交量 = first(each_row.xpath('./td[3]/text()'))
            成交额 = first(each_row.xpath('./td[4]/text()'))
            性质 = first(each_row.xpath('./th[2]/*[name()="h6" or name()="h5" or name()="h1"]/text()'))

            时, 分, 秒 = 成交时间.split(':')
            成交价 = decimal.Decimal(成交价)
            if 价格变动 is None:
                价格变动 = decimal.Decimal(0)
            elif '--' in 价格变动:
                价格变动 = decimal.Decimal(0)
            else:
                价格变动 = decimal.Decimal(价格变动)
            成交量 = decimal.Decimal(成交量)
            if 成交额 is None:
                成交额 = decimal.Decimal(0)
            else:
                成交额 = decimal.Decimal(成交额.replace(',', ''))

            json_data.append({
                'trade_time': datetime.datetime(
                    date.year,
                    date.month,
                    date.day,
                    int(时), int(分), int(秒)),
                'price': 成交价,
                'change': 价格变动,
                'volume': 成交量,
                'amount': 成交额,
                'type': 性质
            })

        page += 1
    return json_data
def stock_ticks_tx(code:str, date:datetime.date, proxies:Dict=None):
    c, m = code.split('.')
    web_code = m.lower() + c

    now_datetime = datetime.datetime.now()
    now_date = now_datetime.date()

    url=f'http://stock.gtimg.cn/data/index.php?appn=detail&action=download&c={web_code}&d={date.strftime("%Y%m%d")}'
    reponse = requests.get(url, proxies=proxies, timeout=15)
    reponse.encoding = 'gbk'
    text = reponse.text
    json_data = []
    for i, line in enumerate(text.split('\n')):
        if i == 0:
            continue
        
        parts = line.split('\t')
        time_obj = datetime.datetime.strptime(parts[0], '%H:%M:%S').time()
        json_data.append({
            'trade_time': datetime.datetime.combine(date, time_obj),
            'price': decimal.Decimal(parts[1]),
            'change': decimal.Decimal(parts[2]),
            'volume': decimal.Decimal(parts[3]),
            'amount': decimal.Decimal(parts[4]),
            'type': parts[5]
        })

    return json_data

def stock_ticks(code:str, date:datetime.date, proxies:Dict=None, src:str='tx'):
    if src == 'tx':
        return stock_ticks_tx(code, date, proxies)
    elif src == 'sina':
        return stock_ticks_sina(code, date, proxies)
    else:
        return []

'''
接口：stock_daily
数据说明：交易日每天15点～16点之间。本接口是未复权行情，停牌期间不提供数据。
调取说明：基础积分每分钟内最多调取500次，每次5000条数据，相当于23年历史，用户获得超过5000积分正常调取无频次限制。
描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据。
参数：
名称	    类型	必选	 描述
ts_code	    str	    N	    股票代码（支持多个股票同时提取，逗号分隔）
trade_date	str	    N	    交易日期（YYYYMMDD）
start_date	str	    N	    开始日期(YYYYMMDD)
end_date	str	    N	    结束日期(YYYYMMDD)
'''
def stock_daily(code:str='', trade_date:str='', start_date:str='', end_date:str='') -> List[Dict]:
    pro = TushareAPI(token)
    df = pro.query('daily',
        parse_float='str',
        ts_code=code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date
        )
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'trade_date': row['trade_date'],
                'open': decimal.Decimal(row['open']),
                'high': decimal.Decimal(row['high']),
                'low': decimal.Decimal(row['low']),
                'close': decimal.Decimal(row['close']),
                'pre_close': decimal.Decimal(row['pre_close']),
                'change': decimal.Decimal(row['change']),
                'pct_chg': decimal.Decimal(row['pct_chg']),
                'vol': decimal.Decimal(row['vol']),
                'amount': decimal.Decimal(row['amount']) if row['amount'] is not None else None,
            }
        )
    
    return json_data

'''
接口：daily_basic
更新时间：交易日每日15点～17点之间
描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
'''
def daily_basic(code:str='', trade_date:str='', start_date:str='', end_date:str='') -> List[Dict]:
    pro = TushareAPI(token)
    df = pro.query(
        'daily_basic',
        parse_float='str',
        ts_code=code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date
        )
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'trade_date': row['trade_date'],
                'close': None if row['close'] is None else decimal.Decimal(row['close']),
                'turnover_rate': None if row['turnover_rate_f'] is None else decimal.Decimal(row['turnover_rate_f']),
                'turnover_rate_f': None if row['turnover_rate_f'] is None else decimal.Decimal(row['turnover_rate_f']),
                'volume_ratio': None if row['volume_ratio'] is None else decimal.Decimal(row['volume_ratio']),
                'pe': None if row['pe'] is None else decimal.Decimal(row['pe']),
                'pe_ttm': None if row['pe_ttm'] is None else decimal.Decimal(row['pe_ttm']),
                'pb': None if row['pb'] is None else decimal.Decimal(row['pb']),
                'ps': None if row['ps'] is None else decimal.Decimal(row['ps']),
                'ps_ttm': None if row['ps_ttm'] is None else decimal.Decimal(row['ps_ttm']),
                'dv_ratio': None if row['dv_ratio'] is None else decimal.Decimal(row['dv_ratio']),
                'dv_ttm': None if row['dv_ttm'] is None else decimal.Decimal(row['dv_ttm']),
                'total_share': None if row['total_share'] is None else decimal.Decimal(row['total_share']),
                'float_share': None if row['float_share'] is None else decimal.Decimal(row['float_share']),
                'free_share': None if row['free_share'] is None else decimal.Decimal(row['free_share']),
                'total_mv': None if row['total_mv'] is None else decimal.Decimal(row['total_mv']),
                'circ_mv': None if row['circ_mv'] is None else decimal.Decimal(row['circ_mv']),
            }
        )
    
    return json_data

'''
接口：stock_min
更新时间：交易日每日15点～17点之间
描述：获取全部股票每日重要的基本面指标，可用于选股分析、报表展示等。
'''
def stock_min(code:str='', start_date:str='', end_date:str='', freq:str='', offset:str=None, limit:str='8000'):
    pro = TushareAPI(token)
    df = pro.query(
        'stk_mins',
        parse_float='str',
        ts_code=code,
        start_date=start_date,
        end_date=end_date,
        freq=freq,
        offset=offset,
        limit=limit
        )
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'trade_time': row['trade_time'],
                'open': decimal.Decimal(row['open']),
                'high': decimal.Decimal(row['high']),
                'low': decimal.Decimal(row['low']),
                'close': decimal.Decimal(row['close']),
                'vol': decimal.Decimal(row['vol']),
                'amount': decimal.Decimal(row['amount']) if row['amount'] is not None else None,
            }
        )
    
    return json_data
'''
接口：stock_weekly
描述：获取A股周线行情
限量：单次最大3700，总量不限制
积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
输入参数：
名称	    类型	必选	描述
ts_code	    str	    N	    TS代码 （ts_code,trade_date两个参数任选一）
trade_date	str	    N	    交易日期 （每周最后一个交易日期，YYYYMMDD格式）
start_date	str	    N	    开始日期
end_date	str	    N	    结束日期
'''
def stock_weekly(code:str='', trade_date:str='', start_date:str='', end_date:str='') -> List[Dict]:
    pro = TushareAPI(token)
    df = pro.query(
        'weekly',
        parse_float='str',
        ts_code=code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date
        )
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'trade_date': row['trade_date'],
                'open': decimal.Decimal(row['open']),
                'high': decimal.Decimal(row['high']),
                'low': decimal.Decimal(row['low']),
                'close': decimal.Decimal(row['close']),
                'pre_close': decimal.Decimal(row['pre_close']),
                'change': decimal.Decimal(row['change']),
                'pct_chg': decimal.Decimal(row['pct_chg']),
                'vol': decimal.Decimal(row['vol']),
                'amount': decimal.Decimal(row['amount']),
            }
        )
    
    return json_data

'''
接口：stock_monthly
描述：获取A股月线数据
限量：单次最大3700，总量不限制
积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法
输入参数：
名称	    类型	必选	描述
ts_code	    str	    N	    TS代码 （ts_code,trade_date两个参数任选一）
trade_date	str	    N	    交易日期 （每月最后一个交易日日期，YYYYMMDD格式）
start_date	str	    N	    开始日期
end_date	str	    N	    结束日期
'''
def stock_monthly(code:str='', trade_date:str='', start_date:str='', end_date:str='') -> List[Dict]:
    pro = TushareAPI(token)
    df = pro.query(
        'monthly',
        parse_float='str',
        ts_code=code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date)
    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'trade_date': row['trade_date'],
                'open': decimal.Decimal(row['open']),
                'high': decimal.Decimal(row['high']),
                'low': decimal.Decimal(row['low']),
                'close': decimal.Decimal(row['close']),
                'pre_close': decimal.Decimal(row['pre_close']),
                'change': decimal.Decimal(row['change']),
                'pct_chg': decimal.Decimal(row['pct_chg']),
                'vol': decimal.Decimal(row['vol']),
                'amount': decimal.Decimal(row['amount']),
            }
        )
    
    return json_data

'''
接口：adj_factor
更新时间：早上9点30分
描述：获取股票复权因子，可提取单只股票全部历史复权因子，也可以提取单日全部股票的复权因子。
输入参数
名称	类型	必选	描述
ts_code	str	Y	股票代码
trade_date	str	N	交易日期(YYYYMMDD，下同)
start_date	str	N	开始日期
end_date	str	N	结束日期
'''
def adj_factor():
    pass


'''
接口：income
描述：获取上市公司财务利润表数据
积分：用户需要至少800积分才可以调取，具体请参阅积分获取办法

输入参数：
名称	    类型	必选	描述
ts_code	    str	    Y	    股票代码
ann_date	str	    N	    公告日期
start_date	str	    N	    公告开始日期
end_date	str	    N	    公告结束日期
period	    str	    N	    报告期(每个季度最后一天的日期，比如20171231表示年报)
report_type	str	    N	    报告类型： 参考下表说明
comp_type	str	    N	    公司类型：1一般工商业 2银行 3保险 4证券
返回：
ts_code	            str	    Y	TS代码
ann_date	        str	    Y	公告日期
f_ann_date	        str	    Y	实际公告日期
end_date	        str	    Y	报告期
report_type	        str	    Y	报告类型 1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表 6母公司报表 7母公司单季表 8 母公司调整单季表 9母公司调整表 10母公司调整前报表 11调整前合并报表 12母公司调整前报表
comp_type	        str	    Y	公司类型(1一般工商业2银行3保险4证券)
basic_eps	        float	Y	基本每股收益
diluted_eps	        float	Y	稀释每股收益
total_revenue	    float	Y	营业总收入
revenue	            float	Y	营业收入
int_income	        float	Y	利息收入
prem_earned	        float	Y	已赚保费
comm_income	        float	Y	手续费及佣金收入
n_commis_income	    float	Y	手续费及佣金净收入
n_oth_income	    float	Y	其他经营净收益
n_oth_b_income	    float	Y	加:其他业务净收益
prem_income	        float	Y	保险业务收入
out_prem	        float	Y	减:分出保费
une_prem_reser	    float	Y	提取未到期责任准备金
reins_income	    float	Y	其中:分保费收入
n_sec_tb_income	    float	Y	代理买卖证券业务净收入
n_sec_uw_income	    float	Y	证券承销业务净收入
n_asset_mg_income	float	Y	受托客户资产管理业务净收入
oth_b_income	    float	Y	其他业务收入
fv_value_chg_gain	float	Y	加:公允价值变动净收益
invest_income	    float	Y	加:投资净收益
ass_invest_income	float	Y	其中:对联营企业和合营企业的投资收益
forex_gain	        float	Y	加:汇兑净收益
total_cogs	        float	Y	营业总成本
oper_cost	        float	Y	减:营业成本
int_exp	            float	Y	减:利息支出
comm_exp	        float	Y	减:手续费及佣金支出
biz_tax_surchg	    float	Y	减:营业税金及附加
sell_exp	        float	Y	减:销售费用
admin_exp	        float	Y	减:管理费用
fin_exp	            float	Y	减:财务费用
assets_impair_loss	float	Y	减:资产减值损失
prem_refund	        float	Y	退保金
compens_payout	    float	Y	赔付总支出
reser_insur_liab	float	Y	提取保险责任准备金
div_payt	        float	Y	保户红利支出
reins_exp	        float	Y	分保费用
oper_exp	        float	Y	营业支出
compens_payout_refu	float	Y	减:摊回赔付支出
insur_reser_refu	float	Y	减:摊回保险责任准备金
reins_cost_refund	float	Y	减:摊回分保费用
other_bus_cost	    float	Y	其他业务成本
operate_profit	    float	Y	营业利润
non_oper_income	    float	Y	加:营业外收入
non_oper_exp	    float	Y	减:营业外支出
nca_disploss	    float	Y	其中:减:非流动资产处置净损失
total_profit	    float	Y	利润总额
income_tax	        float	Y	所得税费用
n_income	        float	Y	净利润(含少数股东损益)
n_income_attr_p	    float	Y	净利润(不含少数股东损益)
minority_gain	    float	Y	少数股东损益
oth_compr_income	float	Y	其他综合收益
t_compr_income	    float	Y	综合收益总额
compr_inc_attr_p	float	Y	归属于母公司(或股东)的综合收益总额
compr_inc_attr_m_s	float	Y	归属于少数股东的综合收益总额
ebit	            float	Y	息税前利润
ebitda	            float	Y	息税折旧摊销前利润
insurance_exp	    float	Y	保险业务支出
undist_profit	    float	Y	年初未分配利润
distable_profit	    float	Y	可分配利润
update_flag	        str	    N	更新标识，0未修改1更正过
'''
def income(ts_code:str,
        ann_date:str='',
        start_date:str='',
        end_date:str='',
        period:str='',
        report_type:str='',
        comp_type:str='') -> List[Dict]:
    pro = TushareAPI(token)
    df = pro.query(
        'income',
        parse_float='str',
        ts_code=ts_code,
        ann_date=ann_date,
        start_date=start_date,
        end_date=end_date,
        period=period,
        report_type=report_type,
        comp_type=comp_type,
        fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,\
        total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,\
        n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,\
        n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,\
        ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,\
        sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,\
        div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,\
        other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,\
        income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,\
        compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,update_flag'
        )

    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'ann_date': row['ann_date'],
                'f_ann_date': row['f_ann_date'],
                'end_date': row['end_date'],
                'report_type': row['report_type'],
                'comp_type': row['comp_type'],
                'basic_eps': decimal.Decimal(row['basic_eps']),
                'diluted_eps': decimal.Decimal(row['diluted_eps']),
                'total_revenue': decimal.Decimal(row['total_revenue']),
                'revenue': decimal.Decimal(row['revenue']),
                'int_income': decimal.Decimal(row['int_income']),
                'prem_earned': decimal.Decimal(row['prem_earned']),
                'comm_income': decimal.Decimal(row['comm_income']),
                'n_commis_income': decimal.Decimal(row['n_commis_income']),
                'n_oth_income': decimal.Decimal(row['n_oth_income']),
                'n_oth_b_income': decimal.Decimal(row['n_oth_b_income']),
                'prem_income': decimal.Decimal(row['prem_income']),
                'out_prem': decimal.Decimal(row['out_prem']),
                'une_prem_reser': decimal.Decimal(row['une_prem_reser']),
                'reins_income': decimal.Decimal(row['reins_income']),
                'n_sec_tb_income': decimal.Decimal(row['n_sec_tb_income']),
                'n_sec_uw_income': decimal.Decimal(row['n_sec_uw_income']),
                'n_asset_mg_income': decimal.Decimal(row['n_asset_mg_income']),
                'oth_b_income': decimal.Decimal(row['oth_b_income']),
                'fv_value_chg_gain': decimal.Decimal(row['fv_value_chg_gain']),
                'invest_income': decimal.Decimal(row['invest_income']),
                'ass_invest_income': decimal.Decimal(row['ass_invest_income']),
                'forex_gain': decimal.Decimal(row['forex_gain']),
                'total_cogs': decimal.Decimal(row['total_cogs']),
                'oper_cost': decimal.Decimal(row['oper_cost']),
                'int_exp': decimal.Decimal(row['int_exp']),
                'comm_exp': decimal.Decimal(row['comm_exp']),
                'biz_tax_surchg': decimal.Decimal(row['biz_tax_surchg']),
                'sell_exp': decimal.Decimal(row['sell_exp']),
                'admin_exp': decimal.Decimal(row['admin_exp']),
                'fin_exp': decimal.Decimal(row['fin_exp']),
                'assets_impair_loss': decimal.Decimal(row['assets_impair_loss']),
                'prem_refund': decimal.Decimal(row['prem_refund']),
                'compens_payout': decimal.Decimal(row['compens_payout']),
                'reser_insur_liab': decimal.Decimal(row['reser_insur_liab']),
                'div_payt': decimal.Decimal(row['div_payt']),
                'reins_exp': decimal.Decimal(row['reins_exp']),
                'oper_exp': decimal.Decimal(row['oper_exp']),
                'compens_payout_refu': decimal.Decimal(row['compens_payout_refu']),
                'insur_reser_refu': decimal.Decimal(row['insur_reser_refu']),
                'reins_cost_refund': decimal.Decimal(row['reins_cost_refund']),
                'other_bus_cost': decimal.Decimal(row['other_bus_cost']),
                'operate_profit': decimal.Decimal(row['operate_profit']),
                'non_oper_income': decimal.Decimal(row['non_oper_income']),
                'non_oper_exp': decimal.Decimal(row['non_oper_exp']),
                'nca_disploss': decimal.Decimal(row['nca_disploss']),
                'total_profit': decimal.Decimal(row['total_profit']),
                'income_tax': decimal.Decimal(row['income_tax']),
                'n_income': decimal.Decimal(row['n_income']),
                'n_income_attr_p': decimal.Decimal(row['n_income_attr_p']),
                'minority_gain': decimal.Decimal(row['minority_gain']),
                'oth_compr_income': decimal.Decimal(row['oth_compr_income']),
                't_compr_income': decimal.Decimal(row['t_compr_income']),
                'compr_inc_attr_p': decimal.Decimal(row['compr_inc_attr_p']),
                'compr_inc_attr_m_s': decimal.Decimal(row['compr_inc_attr_m_s']),
                'ebit': decimal.Decimal(row['ebit']),
                'ebitda': decimal.Decimal(row['ebitda']),
                'insurance_exp': decimal.Decimal(row['insurance_exp']),
                'undist_profit': decimal.Decimal(row['undist_profit']),
                'distable_profit': decimal.Decimal(row['distable_profit']),
                'update_flag': row['update_flag'],
            }
        )
    
    return json_data

'''
code	str	N	交易所代码
ts_code	str	N	TS股票代码
trade_date	str	N	交易日期
start_date	str	N	开始日期
end_date	str	N	结束日期
exchange	str	N	类型：SH沪股通（北向）SZ深股通（北向）HK港股通（南向持股）
'''
def hk_hold(ex_code:str='', code:str='', trade_date:str='', start_date:str='', end_date:str='', exchange:str=''):
    pro = TushareAPI(token)
    df = pro.query(
        'hk_hold',
        parse_float='str',
        code=ex_code,
        ts_code=code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        exchange=exchange
        )

    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'ex_code': row['code'],
                'trade_date': row['trade_date'],
                'code': row['ts_code'],
                'name': row['name'],
                'vol':  None if row['vol'] is None else decimal.Decimal(row['vol']),
                'ratio': None if row['ratio'] is None else decimal.Decimal(row['ratio']),
                'exchange': row['exchange'],
            }
        )
    return json_data

