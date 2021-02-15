import tqdm

from quantmew.model import *
from quantmew import datasourcing

def sync_xuangubao_concepts():
    api = datasourcing.xuangubao.XuangubaoAPI()
    concepts_cache = set()
    total_num = StockBasic.select().count()
    for each_stock in tqdm.tqdm(iterable=StockBasic.select(), total=total_num):
        if StockXuangubaoConcept.get_or_none(StockXuangubaoConcept.code==each_stock.code) is not None:
            continue
        concepts = api.stock_concepts(each_stock.code)
        print(each_stock.name, concepts)
        for each_concept in concepts:
            if each_concept not in concepts_cache:
                record, created = XuangubaoConcept.get_or_create(
                    name=each_concept
                )
                concepts_cache.add(each_concept)
            record, created = StockXuangubaoConcept.get_or_create(
                code=each_stock.code,
                concept=each_concept
            )