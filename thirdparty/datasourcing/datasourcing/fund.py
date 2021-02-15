

'''
基金数据源
'''
import json
import time
import decimal
import datetime
import random
import tempfile
from typing import Optional, List, Dict

import tqdm
import requests
import pandas as pd
import pymongo
from bson.decimal128 import Decimal128
from bson.codec_options import TypeCodec
from bson.codec_options import TypeRegistry
from bson.codec_options import CodecOptions

from .data import TushareAPI
from quantmew.datasourcing import token

class DecimalCodec(TypeCodec):
    python_type = decimal.Decimal    # the Python type acted upon by this type codec
    bson_type = Decimal128   # the BSON type acted upon by this type codec
    def transform_python(self, value):
        """Function that transforms a custom type value into a type
        that BSON can encode."""
        return Decimal128(value)
    def transform_bson(self, value):
        """Function that transforms a vanilla BSON type value into our
        custom type."""
        return value.to_decimal()

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])

'''
接口：fund_basic
描述：获取公募基金数据列表，包括场内和场外基金
积分：用户需要1500积分才可以调取，具体请参阅积分获取办法

输入参数：
名称	类型	必选	描述
market	str	    N	    交易市场: E场内 O场外（默认E）
status	str	    N	    存续状态 D摘牌 I发行 L上市中
输出参数：
名称	        类型	默认显示	描述
ts_code	        str	    Y           基金代码
name	        str	    Y           简称
management	    str	    Y           管理人
custodian	    str	    Y           托管人
fund_type	    str	    Y           投资类型
found_date	    str	    Y           成立日期
due_date	    str	    Y           到期日期
list_date	    str	    Y           上市时间
issue_date	    str	    Y           发行日期
delist_date	    str	    Y           退市日期
issue_amount	float	Y	        发行份额(亿)
m_fee	        float	Y	        管理费
c_fee	        float	Y	        托管费
duration_year	float	Y	        存续期
p_value	        float	Y	        面值
min_amount	    float	Y	        起点金额(万元)
exp_return	    float	Y	        预期收益率
benchmark	    str	    Y	        业绩比较基准
status	        str	    Y	        存续状态D摘牌 I发行 L已上市
invest_type	    str	    Y	        投资风格
type	        str	    Y	        基金类型
trustee	        str	    Y	        受托人
purc_startdate	str	    Y	        日常申购起始日
redm_startdate	str	    Y	        日常赎回起始日
market	        str	    Y	        E场内O场外
'''
def fund_basic(market:str='',status:str=''):
    pro = TushareAPI(token)
    df = pro.query(
        'fund_basic',
        parse_float='str',
        market=market,
        status=status)

    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'name': row['name'],
                'management': row['management'],
                'custodian': row['custodian'],
                'fund_type': row['fund_type'],
                'found_date': row['found_date'],
                'due_date': row['due_date'],
                'list_date': row['list_date'],
                'issue_date': row['issue_date'],
                'delist_date': row['delist_date'],
                'issue_amount': decimal.Decimal(row['issue_amount']) if row['issue_amount'] is not None else None,
                'm_fee': decimal.Decimal(row['m_fee']),
                'c_fee': decimal.Decimal(row['c_fee']),
                'duration_year': decimal.Decimal(row['duration_year']) if row['duration_year'] is not None else None,
                'p_value': decimal.Decimal(row['p_value']),
                'min_amount': decimal.Decimal(row['min_amount']) if row['min_amount'] is not None else None,
                'exp_return': decimal.Decimal(row['exp_return']) if row['exp_return'] is not None else None,
                'benchmark': row['benchmark'],
                'status': row['status'],
                'invest_type': row['invest_type'],
                'type': row['type'],
                'trustee': row['trustee'],
                'purc_startdate': row['purc_startdate'],
                'redm_startdate': row['redm_startdate'],
                'market': row['market'],
            }
        )
    return json_data

'''
接口：fund_company
描述：获取公募基金管理人列表
积分：用户需要1500积分才可以调取，一次可以提取全部数据。具体请参阅积分获取办法
输出：
name	    str	Y	基金公司名称
shortname	str	Y	简称
short_enname	str	N	英文缩写
province	str	Y	省份
city	    str	Y	城市
address	    str	Y	注册地址
phone	    str	Y	电话
office	    str	Y	办公地址
website	    str	Y	公司网址
chairman	str	Y	法人代表
manager	    str	Y	总经理
reg_capital	float	Y	注册资本
setup_date	str	Y	成立日期
end_date	str	Y	公司终止日期
employees	float	Y	员工总数
main_business	str	Y	主要产品及业务
org_code	str	Y	组织机构代码
credit_code	str	Y	统一社会信用代码
'''

def fund_company():
    pro = TushareAPI(token)
    df = pro.query(
        'fund_company',
        parse_float='str',
        fields='name,shortname,short_enname,province,city,address,phone,office,website,\
            chairman,manager,reg_capital,setup_date,end_date,employees,main_business,\
            org_code,credit_code'
        )

    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'name': row['name'],
                'shortname': row['shortname'],
                'short_enname': row['short_enname'],
                'province': row['province'],
                'city': row['city'],
                'address': row['address'],
                'phone': row['phone'],
                'office': row['office'],
                'website': row['website'],
                'chairman': row['chairman'],
                'manager': row['manager'],
                'reg_capital': decimal.Decimal(row['reg_capital']) if row['reg_capital'] is not None else None,
                'setup_date': row['setup_date'],
                'end_date': row['end_date'],
                'employees': decimal.Decimal(row['employees']) if row['employees'] is not None else None,
                'main_business': row['main_business'],
                'org_code': row['org_code'],
                'credit_code': row['credit_code'],
            }
        )
    return json_data

'''
接口：fund_manager
描述：获取公募基金经理数据，包括基金经理简历等数据
限量：单次最大5000，支持分页提取数据
积分：用户有500积分可获取数据，2000积分以上可以提高访问频次
输入参数：
名称	类型	必选	描述
ts_code	str	N	基金代码，支持多只基金，逗号分隔
ann_date	str	N	公告日期，格式：YYYYMMDD
name	str	N	基金经理姓名
offset	int	N	开始行数
limit	int	N	每页行数
输出参数
ts_code	    str	Y	基金代码
ann_date	str	Y	公告日期
name	    str	Y	基金经理姓名
gender	    str	Y	性别
birth_year	str	Y	出生年份
edu	        str	Y	学历
nationality	str	Y	国籍
begin_date	str	Y	任职日期
end_date	str	Y	离任日期
resume	    str	Y	简历
'''
def fund_manager(code:Optional[str]=None, ann_date:Optional[str]=None, name:Optional[str]=None, offset:Optional[int]=None, limit:Optional[int]=None):
    params = {}
    if code is not None:
        params['ts_code'] = code
    if ann_date is not None:
        params['ann_date'] = ann_date
    if name is not None:
        params['name'] = name
    if offset is not None:
        params['offset'] = offset
    if limit is not None:
        params['limit'] = limit

    pro = TushareAPI(token)
    df = pro.query(
        'fund_manager',
        parse_float='str',
        **params,
        fields='ts_code,ann_date,name,gender,birth_year,edu,nationality,begin_date,end_date,resume'
        )

    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'ann_date': row['ann_date'],
                'name': row['name'],
                'gender': row['gender'],
                'birth_year': row['birth_year'],
                'edu': row['edu'],
                'nationality': row['nationality'],
                'begin_date': row['begin_date'],
                'end_date': row['end_date'],
                'resume': row['resume'],
            }
        )
    return json_data

'''
接口：fund_share
描述：获取基金规模数据，包含上海和深圳ETF基金
限量：单次最大提取2000行数据
积分：用户需要至少2000积分可以调取，5000积分以上正常调取无频次限制，具体请参阅积分获取办法
输入参数：
名称	类型	必选	描述
ts_code	    str	N	TS基金代码
trade_date	str	N	交易日期
start_date	str	N	开始日期
end_date	str	N	结束日期
fund_type	str	N	基金类型，见下表
market	    str	N	市场：SH/SZ
输出参数：
名称	    类型	默认显示	描述
ts_code	    str	    Y	        基金代码，支持多只基金同时提取，用逗号分隔
trade_date	str	    Y	        交易（变动）日期，格式YYYYMMDD
fd_share	float	Y	        基金份额（万）
'''
def fund_share(code:str=None, trade_date:str=None, start_date:str=None, end_date:str=None, fund_type:str=None, market:str=None):
    params = {}
    if code is not None:
        params['ts_code'] = code
    if trade_date is not None:
        params['trade_date'] = trade_date
    if start_date is not None:
        params['start_date'] = start_date
    if end_date is not None:
        params['end_date'] = end_date
    if fund_type is not None:
        params['fund_type'] = fund_type
    if market is not None:
        params['market'] = market

    pro = TushareAPI(token)
    df = pro.query(
        'fund_share',
        parse_float='str',
        **params,
        fields='ts_code,trade_date,fd_share'
        )

    json_data = []
    for _index, row in df.iterrows():
        json_data.append(
            {
                'code': row['ts_code'],
                'trade_date': row['trade_date'],
                'fd_share': decimal.Decimal(row['fd_share']),
            }
        )
    return json_data

def init_fund_basic():
    client = pymongo.MongoClient(config['MongoDB'])
    db = client[config['DatabaseName']]
    codec_options = CodecOptions(type_registry=type_registry)
    collection = db.get_collection('fund_basic', codec_options=codec_options)

    data:list = []
    data.extend(fund_basic(market='E'))
    data.extend(fund_basic(market='O'))

    for each_data in tqdm.tqdm(iterable=data, ascii=True):
        collection.find_one_and_update(
            { 'code' : each_data['code'] },
            { '$set': {
                'name': each_data['name'],
                'management': each_data['management'],
                'custodian': each_data['custodian'],
                'fund_type': each_data['fund_type'],
                'found_date': each_data['found_date'],
                'due_date': each_data['due_date'],
                'list_date': each_data['list_date'],
                'issue_date': each_data['issue_date'],
                'delist_date': each_data['delist_date'],
                'issue_amount': each_data['issue_amount'],
                'm_fee': each_data['m_fee'],
                'c_fee': each_data['c_fee'],
                'duration_year': each_data['duration_year'],
                'p_value': each_data['p_value'],
                'min_amount': each_data['min_amount'],
                'exp_return': each_data['exp_return'],
                'benchmark': each_data['benchmark'],
                'status': each_data['status'],
                'invest_type': each_data['invest_type'],
                'type': each_data['type'],
                'trustee': each_data['trustee'],
                'purc_startdate': each_data['purc_startdate'],
                'redm_startdate': each_data['redm_startdate'],
                'market': each_data['market'],
            }
            },
            upsert=True,
            return_document=pymongo.ReturnDocument.BEFORE
        )


def init_fund_company():
    client = pymongo.MongoClient(config['MongoDB'])
    db = client[config['DatabaseName']]
    codec_options = CodecOptions(type_registry=type_registry)
    collection = db.get_collection('fund_company', codec_options=codec_options)

    data:list = []
    data.extend(fund_company())

    for each_data in tqdm.tqdm(iterable=data, ascii=True):
        collection.find_one_and_update(
            { 'name': each_data['name'] },
            { '$set': {
                'shortname': each_data['shortname'],
                'short_enname': each_data['short_enname'],
                'province': each_data['province'],
                'city': each_data['city'],
                'address': each_data['address'],
                'phone': each_data['phone'],
                'office': each_data['office'],
                'website': each_data['website'],
                'chairman': each_data['chairman'],
                'manager': each_data['manager'],
                'reg_capital': each_data['reg_capital'],
                'setup_date': each_data['setup_date'],
                'end_date': each_data['end_date'],
                'employees': each_data['employees'],
                'main_business': each_data['main_business'],
                'org_code': each_data['org_code'],
                'credit_code': each_data['credit_code'],
            }
            },
            upsert=True,
            return_document=pymongo.ReturnDocument.BEFORE
        )


def init_fund_manager():
    client = pymongo.MongoClient(config['MongoDB'])
    db = client[config['DatabaseName']]
    codec_options = CodecOptions(type_registry=type_registry)
    fund_basic_collection = db.get_collection('fund_basic', codec_options=codec_options)
    collection = db.get_collection('fund_manager', codec_options=codec_options)

    fund_count = fund_basic_collection.count()
    fund_iter = fund_basic_collection.find()
    for each_fund in tqdm.tqdm(iterable=fund_iter, total=fund_count, ascii=True):
        data:list = []
        
        for _ in range(10):
            try:
                data = fund_manager(code=each_fund['code'])
                break
            except Exception as e:
                print(e)
                time.sleep(30)
            except requests.exceptions.ConnectionError as e:
                print(e)
                time.sleep(10)

        for each_data in data:
            collection.find_one_and_update(
                {
                    'code': each_data['code'],
                    'ann_date': each_data['ann_date'],
                    'name': each_data['name']
                },
                { '$set': {
                    'gender': each_data['gender'],
                    'birth_year': each_data['birth_year'],
                    'edu': each_data['edu'],
                    'nationality': each_data['nationality'],
                    'begin_date': each_data['begin_date'],
                    'end_date': each_data['end_date'],
                    'resume': each_data['resume'],
                }
                },
                upsert=True,
                return_document=pymongo.ReturnDocument.BEFORE
            )

def init_fund_share():
    client = pymongo.MongoClient(config['MongoDB'])
    db = client[config['DatabaseName']]
    codec_options = CodecOptions(type_registry=type_registry)
    fund_basic_collection = db.get_collection('fund_basic', codec_options=codec_options)
    collection = db.get_collection('fund_share', codec_options=codec_options)

    fund_count = fund_basic_collection.count()
    fund_iter = fund_basic_collection.find()
    for each_fund in tqdm.tqdm(iterable=fund_iter, total=fund_count, ascii=True):
        data:list = []
        
        for _ in range(10):
            try:
                data = fund_share(code=each_fund['code'])
                break
            except Exception as e:
                print(e)
                time.sleep(30)
            except requests.exceptions.ConnectionError as e:
                print(e)
                time.sleep(10)

        for each_data in data:
            collection.find_one_and_update(
                {
                    'code': each_data['code'],
                    'trade_date': each_data['trade_date'],
                },
                { '$set': {
                    'fd_share': each_data['fd_share'],
                }
                },
                upsert=True,
                return_document=pymongo.ReturnDocument.BEFORE
            )