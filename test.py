import json
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import yfinance as yf
from functools import lru_cache

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:76.0) Gecko/20100101 Firefox/76.0',
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

def fetchdata(url, headers):
    return requests.get(url=url, headers=headers).text

def fetchstockcorpdata(symbol):
    url = 'https://www.nseindia.com/api/quote-equity?symbol=' + symbol + '&section=corp_info'
    return json.loads(fetchdata(url, headers))

def fetchpromoterholding(symbol):
    latestDate = datetime.fromtimestamp(1)
    latestshareholding = 0
    for shareholding in fetchstockcorpdata(symbol)['corporate']['shareholdingPatterns']['data']:
        if shareholding['name'] == 'Promoter & Promoter Group':
            for (key, value) in shareholding.items():
                if key != 'name':
                    date = datetime.strptime(key, '%d-%b-%Y')
                    if date > latestDate:
                        latestDate = date
                        latestshareholding = value
    return latestshareholding


def mutualfundselling(symbol):
    sixmonthbackdate = datetime.now() - timedelta(days=180)
    for sastregulation in fetchstockcorpdata(symbol)['corporate']['sastRegulations_29']:
        regulationdate = datetime.strptime(sastregulation['timestamp'], '%d-%b-%Y %H:%M')
        if regulationdate > sixmonthbackdate:
            if sastregulation['noOfShareSale'] and int(sastregulation['noOfShareSale']) > 0:
                return True
    return False

def mutualfundselling(symbol):
    sixmonthbackdate = datetime.now() - timedelta(days=180)
    for sastregulation in fetchstockcorpdata(symbol)['corporate']['sastRegulations_29']:
        regulationdate = datetime.strptime(sastregulation['timestamp'], '%d-%b-%Y %H:%M')
        if regulationdate > sixmonthbackdate:
            if sastregulation['noOfShareSale'] and int(sastregulation['noOfShareSale']) > 0:
                return True
    return False

def ispledged(symbol):

    for pledges in fetchstockcorpdata(symbol)['corporate']['pledgedetails']:
        if float(pledges['per3']) > 0:
            return True
    return False

def positiveearning(symbol):
    latestDate = datetime.fromtimestamp(1)
    latesteps = -1
    for fresults in fetchstockcorpdata(symbol)['corporate']['financialResults']:
        publishdate = datetime.strptime(fresults['re_broadcast_timestamp'], '%d-%b-%Y %H:%M')
        if(publishdate > latestDate):
            latestDate = publishdate
            if fresults['reDilEPS']:
                latesteps = float(fresults['reDilEPS'])
    if latesteps > 0:
        return True
    else:
        return False

@lru_cache(maxsize=500)
def fetchstockinsiderdata(days):
    todayTime = datetime.now(pytz.timezone('Asia/Kolkata'))
    threeMonthsBack = todayTime - timedelta(days=days)
    todayTimeStr = todayTime.strftime("%d-%m-%Y")
    threeMonthsBackStr = threeMonthsBack.strftime("%d-%m-%Y")
    insiderTradingDataUrl = 'https://www.nseindia.com/api/corporates-pit?index=equities&from_date=' + threeMonthsBackStr + '&to_date=' + todayTimeStr
    return json.loads(fetchdata(insiderTradingDataUrl, headers))


insiderTradingData = fetchstockinsiderdata(93)
promoterTradingFrame = pd.json_normalize(insiderTradingData['data'])

filenamedate = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%d-%m-%Y')
promoterTradingFrame.to_csv(filenamedate, index=False)