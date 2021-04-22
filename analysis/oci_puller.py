from external.nse import NSE
from datetime import datetime
import sqlite3


nse = NSE()

connect = sqlite3.connect("../data/database.db")
records = nse.fetch_index_chain_data('NIFTY')['records']
option_chain_date = datetime.strptime(records['timestamp'], '%d-%b-%Y %H:%M:%S').date()
expiry_dates = records['expiryDates']
option_chain = records['data']
underlying_value = records['underlyingValue']

cursor = connect.cursor()

cursor.execute('''DROP TABLE IF EXISTS OPTION_CHAIN''')
cursor.execute('''CREATE TABLE OPTION_CHAIN (
date TEXT,
expiry_date TEXT,
option_type TEXT  NOT NULL,
strike_price REAL NOT NULL,
oi REAL NOT NULL,
coi REAL NOT NULL,
volume REAL NOT NULL,
iv REAL NOT NULL,
ltp REAL NOT NULL,
change REAL NOT NULL,
TM TEXT NOT NULL,
sentiment TEXT NOT NULL,
coi_by_v REAL NOT NULL,
underlying_value REAL NOT NULL,
PRIMARY KEY(date, expiry_date, option_type, strike_price))''')


def sentiment_analysis(coi, change):
    sentiment = 'unknown'
    # OI Increases - Premium Increases - Fresh Long Positions - Long Build Up
    if coi > 0 and change > 0:
        sentiment = 'long_build_up'
    # OI Decreases - Premium Decreases - Closing Short Positions - Long Unwinding
    if coi < 0 and change < 0:
        sentiment = 'long_unwinding'
    # OI Increase - Premium Decrease - Fresh Short Position - Short Build Up
    if coi > 0 and change < 0:
        sentiment = 'short_build_up'
    # OI Decreases - Premium Increases - Closing Short Positions - Short Covering
    if coi < 0 and change > 0:
        sentiment = 'short_covering'
    return sentiment

for record in option_chain:
    try:
        if 'CE' in record:
            option_chain_record_ce = record['CE']
            strikePrice = option_chain_record_ce['strikePrice']
            open_interest = option_chain_record_ce['openInterest']
            coi = option_chain_record_ce['changeinOpenInterest']
            volume = option_chain_record_ce['totalTradedVolume']
            expiry_date = option_chain_record_ce['expiryDate']
            iv = option_chain_record_ce['impliedVolatility']
            ltp = option_chain_record_ce['lastPrice']
            change = option_chain_record_ce['change']
            TM = 'ITM' if (strikePrice < underlying_value) else 'OTM'
            TM = 'ATM' if (abs(strikePrice - underlying_value) < 100) else TM
            TM = 'FOTM' if (strikePrice - underlying_value >= 250) else TM
            coi_by_v = abs(coi) * 75 / volume if(volume != 0) else 0
            sentiment = sentiment_analysis(coi, change)
            cursor.execute("INSERT INTO OPTION_CHAIN(date, expiry_date, option_type, strike_price, coi, oi, volume, ltp, change, iv, TM, sentiment, coi_by_v, underlying_value) "
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (option_chain_date, expiry_date, 'CE', strikePrice, coi, open_interest,
                                                                                 volume, ltp, change, iv, TM, sentiment, coi_by_v, underlying_value))
        if 'PE' in record:
            option_chain_record_pe = record['PE']
            strikePrice = option_chain_record_pe['strikePrice']
            open_interest = option_chain_record_pe['openInterest']
            coi = option_chain_record_pe['changeinOpenInterest']
            volume = option_chain_record_pe['totalTradedVolume']
            expiry_date = option_chain_record_pe['expiryDate']
            iv = option_chain_record_pe['impliedVolatility']
            ltp = option_chain_record_pe['lastPrice']
            change = option_chain_record_pe['change']
            TM = 'OTM' if (strikePrice < underlying_value) else 'ITM'
            TM = 'ATM' if (abs(strikePrice - underlying_value) < 100) else TM
            TM = 'FOTM' if (underlying_value - strikePrice >= 250) else TM
            coi_by_v = abs(coi) * 75 / volume if(volume != 0) else 0
            sentiment = sentiment_analysis(coi, change)
            cursor.execute("INSERT INTO OPTION_CHAIN(date, expiry_date, option_type, strike_price, coi, oi, volume, ltp, change, iv, TM, sentiment, coi_by_v, underlying_value) "
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (option_chain_date, expiry_date, 'PE', strikePrice, coi, open_interest,
                                                                                 volume, ltp, change, iv, TM, sentiment, coi_by_v, underlying_value))
    except Exception as e:
        print(e)


lot_sizes = {
    'NIFTY': 75,
    'BANKNIFTY': 25
}

connect.commit()
connect.close()