import csv
import json
import os
from datetime import datetime, timedelta
from functools import lru_cache
from urllib.parse import quote

import pandas as pd
import pytz
import requests
import xlrd
from pandas.tseries.offsets import BDay
from xlrd import Book


def clean_blank_values(value):
    value = value.strip()
    if value == '-':
        return 0.0
    else:
        return float(value)


def clean_up_eq_bhav_copy(bhav_copy):
    bhav_copy = bhav_copy.set_index('SYMBOL')
    bhav_copy.columns = bhav_copy.columns.str.replace(' ', '')
    bhav_copy['SERIES'] = bhav_copy.loc[:, 'SERIES'].apply(lambda x: x.strip())
    bhav_copy['DATE1'] = bhav_copy.loc[:, 'DATE1'].apply(lambda x: x.strip())
    bhav_copy['DELIV_PER'] = bhav_copy['DELIV_PER'].apply(lambda x: clean_blank_values(x))
    bhav_copy['DELIV_QTY'] = bhav_copy['DELIV_QTY'].apply(lambda x: clean_blank_values(x))
    return bhav_copy


def process_category_turnover_data(workbook: Book):
    sheet = workbook.sheet_by_index(0)
    return sheet.cell_value(3, 2) - sheet.cell_value(3, 3), \
           sheet.cell_value(4, 2) - sheet.cell_value(4, 3), \
           sheet.cell_value(5, 2) - sheet.cell_value(5, 3), \
           sheet.cell_value(6, 2) - sheet.cell_value(6, 3)

class NSE:
    nse_session = requests.session()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:76.0) Gecko/20100101 Firefox/76.0',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    def __init__(self):
        self.nse_session.get('http://www.nseindia.com', headers=self.headers)

    def fetch_data(self, url):
        # time.sleep(2)
        print('External Call')
        print(url)
        return self.nse_session.get(url=url, headers=self.headers).text

    def fetch_files(self, url):
        # time.sleep(2)
        print('External Call')
        print(url)
        return self.nse_session.get(url=url, headers=self.headers).content

    def fetch_index_stock_data(self, index):
        url = 'https://www.nseindia.com/api/equity-stockIndices?index=' + index
        stocks = []
        try:
           return json.loads(self.fetch_data(url))
        except:
            print('Retrying Index Data ')
            print(index)
        return self.fetch_index_stock_data(index)

    def fetch_index_chain_data(self, symbol):
        encode = quote(symbol)
        url = 'https://www.nseindia.com/api/option-chain-indices?symbol=' + encode
        try:
            return json.loads(self.fetch_data(url))
        except:
            print('Retrying Index Chain Data ')
            print(symbol)
            return self.fetch_index_chain_data(symbol)

    def fetch_equity_chain_data(self, symbol):
        encode = quote(symbol)
        url = 'https://www.nseindia.com/api/option-chain-equities?symbol=' + encode
        try:
            return json.loads(self.fetch_data(url))
        except:
            print('Retrying Index Chain Data ')
            print(symbol)
            return self.fetch_equity_chain_data(symbol)

    @lru_cache
    def fetch_stock_quotes(self, symbol):
        encode = quote(symbol)
        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + encode
        try:
            return json.loads(self.fetch_data(url))
        except:
            print('Retrying Stock Quotes Data')
            print(symbol)
            return self.fetch_stock_quotes(symbol)

    @lru_cache
    def fetch_stock_corp_data(self, symbol):
        encode = quote(symbol)
        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + encode + '&section=corp_info'
        try:
            return json.loads(self.fetch_data(url))
        except:
            print('Retrying Corp Data ')
            print(symbol)
            return self.fetch_stock_corp_data(symbol)

    def fetch_close_price(self, symbol):
        try:
            return self.fetch_stock_quotes(symbol)['priceInfo']['lastPrice']
        except:
            print('Retrying : Fetch Close Price')
            print(symbol)
            return self.fetch_close_price(symbol)

    def fetch_stock_insider_data(self, days):
        today_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        three_months_back = today_time - timedelta(days=days)
        today_time_str = today_time.strftime("%d-%m-%Y")
        three_months_back_str = three_months_back.strftime("%d-%m-%Y")
        insider_trading_data_url = 'https://www.nseindia.com/api/corporates-pit?index=equities&from_date=' + \
                                   three_months_back_str + '&to_date=' + today_time_str
        try:
            return json.loads(self.fetch_data(insider_trading_data_url))
        except:
            print('Retrying Stock Insider Data')
            return self.fetch_stock_insider_data(days)

    def fetch_promoter_holding(self, symbol):
        latest_date = datetime.fromtimestamp(1)
        latest_share_holding = 0
        try:
            for shareholding in self.fetch_stock_corp_data(symbol)['corporate']['shareholdingPatterns']['data']:
                if shareholding['name'] == 'Promoter & Promoter Group':
                    for (key, value) in shareholding.items():
                        if key != 'name':
                            date = datetime.strptime(key, '%d-%b-%Y')
                            if date > latest_date:
                                latest_date = date
                                latest_share_holding = value
            return latest_share_holding
        except:
            print('Retrying Symbol fetch_promoter_holding')
            print(symbol)
            return self.fetch_promoter_holding(symbol)

    def mutual_fund_selling(self, symbol):
        three_month_back_date = datetime.now() - timedelta(days=90)
        try:
            total_sell = 0.0
            for sast_regulation in self.fetch_stock_corp_data(symbol)['corporate']['sastRegulations_29']:
                regulation_date = datetime.strptime(sast_regulation['timestamp'], '%d-%b-%Y %H:%M')
                if regulation_date > three_month_back_date:
                    if sast_regulation['noOfShareSale']:
                        total_sell = total_sell + float(sast_regulation['noOfShareSale'])
            return total_sell
        except:
            print('Retrying Symbol mutual_fund_selling')
            print(symbol)
            return self.mutual_fund_selling(symbol)

    def get_pledged(self, symbol):
        try:
            total_pledge = 0
            for pledges in self.fetch_stock_corp_data(symbol)['corporate']['pledgedetails']:
                total_pledge = total_pledge + float(pledges['per3'])
            return total_pledge
        except:
            print('Retrying Symbol is_pledged')
            print(symbol)
            return self.get_pledged(symbol)

    def earnings(self, symbol):
        try:
            latest_date = datetime.fromtimestamp(1)
            latest_eps = -1
            for financial_results in self.fetch_stock_corp_data(symbol)['corporate']['financialResults']:
                publish_date = datetime.strptime(financial_results['re_broadcast_timestamp'], '%d-%b-%Y %H:%M')
                if publish_date > latest_date:
                    latest_date = publish_date
                    if financial_results['reDilEPS']:
                        latest_eps = float(financial_results['reDilEPS'])
            return latest_eps
        except:
            print('Retrying Symbol Positive Earning')
            print(symbol)
            return self.earnings(symbol)

    # specify how many days back bhav-copy you want
    def fetch_eq_bhav_copy(self, days):
        now = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
        date = now - timedelta(max(1, (now.weekday() + 6) % 7 - 3)) - BDay(days)

        bhav_copy_date_str = date.strftime('%d%m%Y')
        print(bhav_copy_date_str)
        url = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_' + bhav_copy_date_str + '.csv'
        try:
            file_local_url = '../data/eq-bhavcopy/' + bhav_copy_date_str + '.csv'
            if os.path.exists(file_local_url):
                return clean_up_eq_bhav_copy(pd.read_csv(file_local_url))
            else:
                content = self.fetch_files(url)
                csv_file = open(file_local_url, 'wb')
                csv_file.write(content)
                csv_file.close()
                return clean_up_eq_bhav_copy(pd.read_csv(file_local_url))
        except:
            print('Bhav copy not available')
            print(url)
            return self.fetch_eq_bhav_copy(days)

    def fetch_category_turnover_data(self, offset):
        today = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
        category_date = today - timedelta(max(1, (today.weekday() + 6) % 7 - 3)) - BDay(1 + offset)
        category_turnover_date = category_date.strftime('%d%m%y')
        url = 'https://archives.nseindia.com/archives/equities/cat/cat_turnover_' + category_turnover_date + '.xls'
        file_local_url = '../data/turnover_data/cash/' + category_turnover_date + '.xls'
        if os.path.exists(file_local_url):
            bank, dfi, prop, retail = process_category_turnover_data(xlrd.open_workbook(file_local_url))
        else:
            files = self.fetch_files(url)
            xls = open(file_local_url, 'wb')
            xls.write(files)
            xls.close()
            bank, dfi, prop, retail = process_category_turnover_data(xlrd.open_workbook(file_local_url))
        return category_date, bank, dfi, prop, retail

    def fetch_index_stock_list(self, index):
        stocks_symbols = []
        index_stock_data = self.fetch_index_stock_data(index)
        stock_list = index_stock_data['data']
        for stock in stock_list:
            if stock['symbol'] != index:
                stocks_symbols.append(stock['symbol'])
        return stocks_symbols


    def fetch_fno_oi_data_csv(self, offset):
        today = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
        category_date = today - timedelta(max(1, (today.weekday() + 6) % 7 - 3)) - BDay(offset)
        category_turnover_date = category_date.strftime('%d%m%Y')
        url = 'https://archives.nseindia.com/content/nsccl/fao_participant_oi_' + category_turnover_date + '.csv'
        file_local_url = '../data/turnover_data/fno/' + category_turnover_date + '.csv'
        if os.path.exists(file_local_url):
            with open(file_local_url, newline='') as csvfile:
                return category_date, [row for row in csv.reader(csvfile, delimiter=',', quotechar='|')]
        else:
            content = self.fetch_files(url)
            csv_file = open(file_local_url, 'wb')
            csv_file.write(content)
            csv_file.close()
            with open(file_local_url, newline='') as csvfile:
                return category_date, [row for row in csv.reader(csvfile, delimiter=',', quotechar='|')]
