from selenium import webdriver
from lxml import etree

class EasyMoneyAPI(object):
    def __init__(self):
        self.browser = None
        self.browser = webdriver.Chrome()

    def __del__(self):
        if self.browser is not None:
            self.browser.close()
    '''
    接口：stock_easymoney_categories
    描述：获取东方财富该股票分类
    '''
    
    def stock_easymoney_categories(self, code:str):
        c, m = code.split('.')
        code = m + c
        url = f'http://quote.eastmoney.com/{code}.html'
        if self.browser is not None:
            self.browser.get(url)
        text = self.browser.page_source
        html = etree.HTML(text)
        result = html.xpath('//tbody[@id="zjlxbk"]/tr/td[1]/a/@title')
        return result