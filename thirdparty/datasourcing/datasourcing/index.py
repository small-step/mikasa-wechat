
from typing import Union

from .data import TushareAPI
from quantmew.application import app
config = app.config['app_config']
token = config['tushare_token']

'''
指数数据源
'''

def index_basic(
    code:Union[str,None]=None,
    name:Union[str,None]=None,
    market:Union[str,None]=None,
    publisher:Union[str,None]=None,
    category:Union[str,None]=None):
    pass
