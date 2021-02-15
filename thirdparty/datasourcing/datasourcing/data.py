import json
import decimal
from functools import partial
import requests
import pandas as pd

# import tushare
# tushare.pro_bar()

class TushareAPI(object):
    __token = ''
    __http_url = 'http://api.tushare.pro'

    def __init__(self, token, timeout=50):
        self.__token = token
        self.__timeout = timeout

    def query(self, api_name, fields='', parse_float:str=None, **kwargs):
        req_params = {
            'api_name': api_name,
            'token': self.__token,
            'params': kwargs,
            'fields': fields
        }

        res = requests.post(self.__http_url, json=req_params, timeout=self.__timeout)
        if parse_float == None or parse_float == '':
            result = json.loads(res.text)
        elif parse_float == 'str':
            result = json.loads(res.text, parse_float = str)
        elif parse_float == 'decimal':
            result = json.loads(res.text, parse_float = decimal.Decimal)
        else:
            result = json.loads(res.text)

        if result['code'] != 0:
            raise Exception(result['msg'])
        data = result['data']
        columns = data['fields']
        items = data['items']

        return pd.DataFrame(items, columns=columns)

    def __getattr__(self, name):
        return partial(self.query, name)