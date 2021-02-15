from selenium import webdriver
from lxml import etree
import time

class XuangubaoAPI(object):
    def __init__(self):
        self.browser = None
        self.browser = webdriver.Chrome()

    def __del__(self):
        if self.browser is not None:
            self.browser.close()
    '''
    接口：stock_concepts
    描述：获取选股宝该股票分类
    '''
    
    def stock_concepts(self, code:str):
        url = f'https://xuangubao.cn/stock/{code}'
        if self.browser is not None:
            self.browser.get(url)
        time.sleep(3)
        text = self.browser.page_source
        html = etree.HTML(text)
        result = html.xpath('//div[@class="related-subject-item-name"]/text()')
        return result