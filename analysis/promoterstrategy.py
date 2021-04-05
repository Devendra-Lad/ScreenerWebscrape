from datetime import datetime

import pandas as pd
import pytz
import sqlite3

from external.nse import NSE

# cursor.execute('''
# DROP TABLE IF EXISTS PROMOTER
# ''')
# cursor.execute('''
# CREATE TABLE PROMOTER (
# date TEXT NOT NULL,
# symbol REAL NOT NULL,
# promoter_price REAL,
# curr_price REAL,
# premium REAL,
# max_entry_price REAL,
# stop_loss REAL,
# promoter_holding REAL,
# mf_sell REAL,
# pledged REAL,
# eps REAL,
# promoter_sold BOOL,
# secVal REAL,
# secAcq REAL,
# PRIMARY KEY(date, symbol))
# ''')
DATE = 'date'
STOP_LOSS = 'stop_loss'
PREMIUM = 'premium'
MAX_ENTRY_PRICE = 'max_entry_price'
PROMOTER_SOLD = 'promoter_sold'
EPS = 'eps'
MF_SELL = 'mf_sell'
PLEDGED = 'pledged'
PROMOTER_HOLDING = 'promoter_holding'
CURR_PRICE = 'curr_price'
MARKET_PURCHASE = 'Market Purchase'
MARKET_SALE = 'Market Sale'
PROMOTER_GROUP = 'Promoter Group'
PROMOTERS = 'Promoters'
PERSON_CATEGORY = 'personCategory'
SEC_ACQ = 'secAcq'
ACQ_MODE = 'acqMode'
SEC_VAL = 'secVal'
SYMBOL = 'symbol'
PROMOTER_PRICE = 'promoter_price'

nse = NSE()

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 500)


def promoter_selling(symbol, sold_stocks):
    if symbol in sold_stocks:
        return True
    else:
        return False


today = datetime.now(pytz.timezone('Asia/Kolkata')).date()
insider_trading_data = nse.fetch_stock_insider_data(100)
promoter_trading_frame = pd.json_normalize(insider_trading_data['data'])
promoter_trading_frame = promoter_trading_frame[[SYMBOL, SEC_VAL, ACQ_MODE, SEC_ACQ, PERSON_CATEGORY, DATE]]
promoter_trading_frame = promoter_trading_frame.loc[promoter_trading_frame.personCategory.isin([PROMOTERS, PROMOTER_GROUP])]

date_sorted_promoter_trading_frame = promoter_trading_frame.loc[promoter_trading_frame.acqMode == MARKET_PURCHASE].sort_values(by=[DATE], ascending=True).groupby(SYMBOL).tail(1)
date_sorted_promoter_trading_frame.set_index(SYMBOL, drop=True, inplace=True)
symbol_to_latest_promoter_purchase_mapping = date_sorted_promoter_trading_frame.to_dict(orient='index')

seller_trading_frame = promoter_trading_frame.loc[promoter_trading_frame.acqMode == MARKET_SALE]
promoter_sold_stocks = seller_trading_frame.symbol.unique()

promoter_trading_frame = promoter_trading_frame.loc[promoter_trading_frame.acqMode == MARKET_PURCHASE]
promoter_trading_frame = promoter_trading_frame[[SYMBOL, SEC_VAL, SEC_ACQ]]
promoter_trading_frame = promoter_trading_frame.astype({SEC_VAL: 'int', SEC_ACQ: 'int'})
promoter_trading_frame = promoter_trading_frame.groupby(SYMBOL, as_index=False).agg({SEC_VAL: 'sum', SEC_ACQ: 'sum'})
promoter_trading_frame = promoter_trading_frame.loc[promoter_trading_frame.secVal >= 10000000]
promoter_trading_frame[PROMOTER_PRICE] = promoter_trading_frame[SEC_VAL] / promoter_trading_frame[SEC_ACQ]
promoter_trading_frame[CURR_PRICE] = promoter_trading_frame[SYMBOL].apply(lambda x: nse.fetch_close_price(x))
promoter_trading_frame[PROMOTER_HOLDING] = promoter_trading_frame[SYMBOL].apply(lambda x: nse.fetch_promoter_holding(x))
promoter_trading_frame[MF_SELL] = promoter_trading_frame[SYMBOL].apply(lambda x: nse.mutual_fund_selling(x))
promoter_trading_frame[PLEDGED] = promoter_trading_frame[SYMBOL].apply(lambda x: nse.get_pledged(x))
promoter_trading_frame[EPS] = promoter_trading_frame[SYMBOL].apply(lambda x: nse.earnings(x))
promoter_trading_frame[PROMOTER_SOLD] = promoter_trading_frame[SYMBOL].apply(lambda x: promoter_selling(x, promoter_sold_stocks))
promoter_trading_frame[MAX_ENTRY_PRICE] = promoter_trading_frame[PROMOTER_PRICE] + 0.03 * promoter_trading_frame[PROMOTER_PRICE]
promoter_trading_frame[PREMIUM] = (promoter_trading_frame[CURR_PRICE] - promoter_trading_frame[PROMOTER_PRICE]) * 100 / promoter_trading_frame[
    CURR_PRICE]
promoter_trading_frame[STOP_LOSS] = promoter_trading_frame[PROMOTER_PRICE] - 0.05 * promoter_trading_frame[PROMOTER_PRICE]
promoter_trading_frame = promoter_trading_frame.sort_values(by=PREMIUM, ascending=False, na_position='first')

connect = sqlite3.connect("../data/moneyflow.db")
cursor = connect.cursor()

for i, row in promoter_trading_frame.iterrows():
    premium = round(row[PREMIUM], 2)
    pledged = round(row[PLEDGED], 2)
    eps = round(row[EPS], 2)
    mf_sell = round(row[MF_SELL], 2)
    curr_price = round(row[CURR_PRICE], 2)
    promoter_price = round(row[PROMOTER_PRICE], 2)
    secVal = round(row[SEC_VAL], 2)
    secAcq = round(row[SEC_ACQ], 2)
    promoter_sold = row[PROMOTER_SOLD]
    promoter_holding = round(float(row[PROMOTER_HOLDING]), 2)
    stop_loss = round(row[STOP_LOSS], 2)
    max_entry_price = round(row[MAX_ENTRY_PRICE], 2)
    lpd = datetime.strptime(symbol_to_latest_promoter_purchase_mapping[row[SYMBOL]][DATE], "%d-%b-%Y %H:%M")\
        .strftime("%Y-%m-%d")
    cursor.execute('''
    INSERT INTO PROMOTER 
    (date, symbol, pledged, eps, mf_sell, curr_price, promoter_price, secVal, secAcq, promoter_sold, promoter_holding, premium, stop_loss, max_entry_price, lpd)  
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date, symbol) DO UPDATE 
    SET pledged=?, eps=?, mf_sell=?, curr_price=?, promoter_price=?, secVal=?, secAcq=?, promoter_sold=?, promoter_holding=?, premium=?, stop_loss=?, max_entry_price=?, lpd=?
    WHERE date=? AND symbol=?''',
                   (today, row[SYMBOL],
    pledged, eps, mf_sell, curr_price, promoter_price, secVal, secAcq, promoter_sold, promoter_holding, premium, stop_loss, max_entry_price, lpd,
    pledged, eps, mf_sell, curr_price, promoter_price, secVal, secAcq, promoter_sold, promoter_holding, premium, stop_loss, max_entry_price, lpd,
    today, row[SYMBOL]))

connect.commit()
connect.close()

# Create Support for comparing with last update of data like new stocks and removed stocks
# Create support for scoring stocks instead
