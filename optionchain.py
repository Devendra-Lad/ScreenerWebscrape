import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
from functools import lru_cache

pd.options.mode.chained_assignment = None

pd.set_option('display.max_columns', 500)

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:76.0) Gecko/20100101 Firefox/76.0',
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

def fetchdata(url, headers):
    return requests.get(url=url, headers=headers).text

# Symbol Options
# 1. BANKNIFTY
# 2. NIFTY
@lru_cache(maxsize=500)
def fetchindexchaindata(symbol):
    url = 'https://www.nseindia.com/api/option-chain-indices?symbol=' + symbol
    return json.loads(fetchdata(url, headers))

@lru_cache(maxsize=500)
def fetchequitychaindata(symbol):
    url = 'https://www.nseindia.com/api/option-chain-equities?symbol=' + symbol
    return json.loads(fetchdata(url, headers))

# Symbol Options
# 1. top20_contracts
# 2. stock_fut
# 3. stock_opt
# 4. top20_spread_contracts
# 5. nse50_fut
# 6. se50_opt
# 7. nifty_bank_fut
# 8. nifty_bank_opt
@lru_cache(maxsize=500)
def fetchderivativedata(symbol):
    url = 'https://www.nseindia.com/api/liveEquity-derivatives?index=' + symbol
    return json.loads(fetchdata(url, headers))
