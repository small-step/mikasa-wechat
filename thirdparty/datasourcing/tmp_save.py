import tqdm
import os
import json
import datetime
import time
import csv
from typing import Union, List, Dict

import datasourcing


if __name__ == "__main__":
    data = datasourcing.stock.stock_daily(code='600030.SH')
    with open('tmp.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(
            ['code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
            )
        for each_data in data:
            csvwriter.writerow([
                each_data['code'],
                each_data['trade_date'],
                each_data['open'],
                each_data['high'],
                each_data['low'],
                each_data['close'],
                each_data['pre_close'],
                each_data['change'],
                each_data['pct_chg'],
                each_data['vol'],
                each_data['amount'],
                ])
