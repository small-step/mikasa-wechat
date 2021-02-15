import tqdm

from .model import *
from . import datasourcing

def sync_easymoney_concepts():
    em_api = datasourcing.easymoney.EasyMoneyAPI()
    concepts_cache = set()
    for each_stock in tqdm.tqdm(iterable=StockBasic.select(), total=StockBasic.select().count()):
        concepts = em_api.stock_easymoney_categories(each_stock.code)
        for each_concept in concepts:
            if each_concept not in concepts_cache:
                record, created = EasyMoneyConcept.get_or_create(
                    name=each_concept
                )
                concepts_cache.add(each_concept)
            record, created = StockEasyMoneyConcept.get_or_create(
                code=each_stock.code,
                concept=each_concept
            )