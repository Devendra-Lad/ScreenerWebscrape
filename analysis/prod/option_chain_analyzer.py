import math
import os
import sqlite3
from datetime import datetime, timedelta
import sys

import mibian
import pandas as pd
import pytz
from scipy.stats import norm

from external.nse import NSE

OUT_DATE_FORMAT = '%d-%b-%Y'
ASIA_KOLKATA = 'Asia/Kolkata'

# Temporary variables TODO Classification pending

UNKNOWN = 'unknown'
SENTIMENT = 'sentiment'
SHORT_COVERING = 'short_covering'
SHORT_BUILD_UP = 'short_build_up'
LONG_UNWINDING = 'long_unwinding'
LONG_BUILD_UP = 'long_build_up'
CHANGE = 'change'
VIEW = 'view'
PE_OI = 'pe_oi'
CE_OI = 'ce_oi'
RATIO = 'pe_ce_ratio'
NORMAL = 'Normal'
BULL = 'BULL'
BEAR = 'BEAR'
PE = 'PE'
OPTION_TYPE = 'optiontype'
CE = 'CE'

# Data columns
CHANGE_IN_OPEN_INTEREST = 'changeinOpenInterest'
EXPIRY_DATE = 'expiryDate'
IMPLIED_VOLATILITY = 'impliedVolatility'
OPEN_INTEREST = 'openInterest'
STRIKE_PRICE = 'strikePrice'
TOTAL_TRADED_VOLUME = 'totalTradedVolume'

# Index Funds
indices = ['NIFTY', 'BANKNIFTY', 'FINNIFTY']

# User Defined Columns
PRICE = 'price'
EXPIRY = 'expiry'
UPPER_BOUND = 'upper_bound'
CE_IV = 'ce_iv'
LOWER_BOUND = 'lower_bound'
PE_IV = 'pe_iv'
CE_WIN_PROBABILITY = 'ce_win_probability'
PE_WIN_PROBABILITY = 'pe_win_probability'
MARKET_SENTIMENT = 'market_sentiment'

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 500)


def create_hd_option_chain_df(data):
    option_chain_df = pd.DataFrame()
    for record in data:
        try:
            if CE in record:
                option_chain_record_ce = record[CE]
                option_chain_record_ce[OPTION_TYPE] = CE
                option_chain_df = option_chain_df.append(option_chain_record_ce, ignore_index=True)
            if PE in record:
                option_chain_record_pe = record[PE]
                option_chain_record_pe[OPTION_TYPE] = PE
                option_chain_df = option_chain_df.append(option_chain_record_pe, ignore_index=True)
        except:
            print(record)
    return option_chain_df.sort_values(by=OPEN_INTEREST, ascending=False)


def get_total_oi(option_chain_df):
    subset_option_chain_df = option_chain_df[[EXPIRY_DATE, OPTION_TYPE, OPEN_INTEREST, TOTAL_TRADED_VOLUME]]
    return subset_option_chain_df.groupby([EXPIRY_DATE, OPTION_TYPE], as_index=False).agg(
        {OPEN_INTEREST: 'sum', TOTAL_TRADED_VOLUME: 'sum'})


def pcr_analysis(symbol, option_chain_df):
    today = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%Y-%m-%d')
    total_oi_data = get_total_oi(option_chain_df)
    expiries = total_oi_data.expiryDate.unique()
    connect = sqlite3.connect("data/database.db")
    cursor = connect.cursor()
    for expiry in expiries:
        expiry_date = datetime.strftime(datetime.strptime(expiry, '%d-%b-%Y'), '%Y-%m-%d')
        data_for_expiry = total_oi_data.loc[total_oi_data.expiryDate == expiry]
        pe = data_for_expiry.loc[total_oi_data.optiontype == PE]
        ce = data_for_expiry.loc[total_oi_data.optiontype == CE]
        if not ce.empty and not pe.empty:
            total_pe = pe.iloc[0][OPEN_INTEREST]
            total_ce = ce.iloc[0][OPEN_INTEREST]
            pe_ce_ratio = 1
            if total_ce > 0 and total_pe > 0:
                pe_ce_ratio = pe.iloc[0][OPEN_INTEREST] / ce.iloc[0][OPEN_INTEREST]
            view = NORMAL
            if pe_ce_ratio < 0.75:
                view = BEAR
            if pe_ce_ratio > 1.25:
                view = BULL
            cursor.execute('''
                INSERT INTO OPTION_ANALYSIS 
                (date, symbol, expiry, pcr, pcr_view)  
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol, expiry) DO UPDATE 
                SET pcr=?, pcr_view=?
                WHERE date=? AND symbol=? AND expiry=?''',
                           (today, symbol, expiry_date,
                            pe_ce_ratio, view,
                            pe_ce_ratio, view,
                            today, symbol, expiry_date))
    connect.commit()
    connect.close()


def analyze_sentiment(option_chain_single_record):
    sentiment = 'unknown'
    change_open_interest = option_chain_single_record[CHANGE_IN_OPEN_INTEREST]
    price_change = option_chain_single_record[CHANGE]

    # OI Increases - Premium Increases - Fresh Long Positions - Long Build Up
    if change_open_interest > 0 and price_change > 0:
        sentiment = LONG_BUILD_UP

    # OI Decreases - Premium Decreases - Closing Short Positions - Long Unwinding
    if change_open_interest < 0 and price_change < 0:
        sentiment = LONG_UNWINDING

    # OI Increase - Premium Decrease - Fresh Short Position - Short Build Up
    if change_open_interest > 0 > price_change:
        sentiment = SHORT_BUILD_UP

    # OI Decreases - Premium Increases - Closing Short Positions - Short Covering
    if change_open_interest < 0 < price_change:
        sentiment = SHORT_COVERING
    return sentiment


def expiry_sentiment_analysis(expiry, option_chain_df):
    expiry_df = option_chain_df.loc[option_chain_df.expiryDate == expiry]
    expiry_df[SENTIMENT] = expiry_df.apply(analyze_sentiment, axis=1)
    return expiry_df.loc[expiry_df.sentiment != UNKNOWN][
        [EXPIRY_DATE, STRIKE_PRICE, OPTION_TYPE, CHANGE_IN_OPEN_INTEREST, CHANGE, SENTIMENT]] \
        .sort_values([OPTION_TYPE, STRIKE_PRICE], ascending=[False, False])


def calculate_z(current_price, strike_price, iv, number_of_days):
    return math.log(strike_price / current_price) / (iv * 0.01 * math.sqrt(number_of_days / 365))


def iv_analysis(symbol, stock_price, option_chain_df):
    today = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%Y-%m-%d')
    connect = sqlite3.connect("data/database.db")
    cursor = connect.cursor()
    expiries = option_chain_df.expiryDate.unique()
    iv_expiry_wise_analysis_df = pd.DataFrame()
    for expiry in expiries:
        expiry_date = datetime.strftime(datetime.strptime(expiry, '%d-%b-%Y'), '%Y-%m-%d')
        analyze_df = option_chain_df.copy()
        analyze_df = analyze_df.loc[analyze_df.expiryDate == expiry]
        analyze_df = analyze_df.sort_values(STRIKE_PRICE, ascending=True)

        temp_df = analyze_df.loc[analyze_df.strikePrice > stock_price]
        if temp_df.empty:
            continue
        optimal_iv_strike_price = temp_df.loc[temp_df.strikePrice == temp_df.strikePrice.min()].iloc[0][STRIKE_PRICE]

        ce_ = analyze_df.loc[(analyze_df.strikePrice == optimal_iv_strike_price) & (analyze_df.optiontype == CE)]
        if ce_.empty:
            continue
        ce_optimal_iv = ce_.iloc[0][IMPLIED_VOLATILITY]
        pe_ = analyze_df.loc[(analyze_df.strikePrice == optimal_iv_strike_price) & (analyze_df.optiontype == PE)]
        if pe_.empty:
            continue
        pe_optimal_iv = pe_.iloc[0][IMPLIED_VOLATILITY]

        temp_df = analyze_df.loc[(analyze_df.strikePrice > optimal_iv_strike_price) & (analyze_df.optiontype == CE)]
        ce_max_ = temp_df.loc[temp_df.openInterest == temp_df.openInterest.max()]
        if ce_max_.empty:
            continue
        max_oi_ce_strike = ce_max_.iloc[0][STRIKE_PRICE]
        temp_df = analyze_df.loc[(analyze_df.strikePrice < optimal_iv_strike_price) & (analyze_df.optiontype == PE)]
        pe_max_ = temp_df.loc[temp_df.openInterest == temp_df.openInterest.max()]
        if pe_max_.empty:
            continue
        max_oi_pe_strike = pe_max_.iloc[0][STRIKE_PRICE]

        now = datetime.now(pytz.timezone(ASIA_KOLKATA))
        strike_time = pytz.timezone(ASIA_KOLKATA).localize(datetime.strptime(expiry, OUT_DATE_FORMAT)) + timedelta(
            hours=15,
            minutes=30)

        time_until_expiry = (strike_time - now).days + (strike_time - now).seconds / 86400

        # Data for expiry might be published after expiry. To Remove that use case.
        if time_until_expiry < 0:
            continue

        if pe_optimal_iv != 0 and ce_optimal_iv != 0 and time_until_expiry != 0:
            ce_win_prob_cdf = (1 - norm.cdf(
                calculate_z(stock_price, max_oi_ce_strike, pe_optimal_iv, time_until_expiry))) * 100
            pe_win_prob_cdf = norm.cdf(
                calculate_z(stock_price, max_oi_pe_strike, ce_optimal_iv, time_until_expiry)) * 100
            if ce_win_prob_cdf > pe_win_prob_cdf:
                market_sentiment = BULL
            else:
                market_sentiment = BEAR

            cursor.execute('''
                INSERT INTO OPTION_ANALYSIS 
                (date, symbol, expiry, spot, iv_view, ub, lb, cep, pep, ceiv, peiv)  
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol, expiry) DO UPDATE 
                SET spot=?, iv_view=?, ub=?, lb=?, cep=?, pep=?, ceiv=?, peiv=? 
                WHERE date=? AND symbol=? AND expiry=?''',
                           (today, symbol, expiry_date,
                            stock_price, market_sentiment, max_oi_ce_strike, max_oi_pe_strike, ce_win_prob_cdf,
                            pe_win_prob_cdf, ce_optimal_iv, pe_optimal_iv,
                            stock_price, market_sentiment, max_oi_ce_strike, max_oi_pe_strike, ce_win_prob_cdf,
                            pe_win_prob_cdf, ce_optimal_iv, pe_optimal_iv,
                            today, symbol, expiry_date))
    connect.commit()
    connect.close()


def delta_calculation(option_chain_row, underlyingValue, interest_rate, time_till_expiry):
    if option_chain_row.optiontype == PE:
        volatility = mibian.BS(
            [underlyingValue, option_chain_row.strikePrice, interest_rate, time_till_expiry],
            putPrice=option_chain_row.lastPrice).impliedVolatility
        return mibian.BS(
            [underlyingValue, option_chain_row.strikePrice, interest_rate, time_till_expiry],
            volatility).putDelta
    else:
        volatility = mibian.BS(
            [underlyingValue, option_chain_row.strikePrice, interest_rate, time_till_expiry],
            callPrice=option_chain_row.lastPrice).impliedVolatility
        return mibian.BS(
            [underlyingValue, option_chain_row.strikePrice, interest_rate, time_till_expiry],
            volatility).callDelta


def delta_strategy(symbol, hd_option_chain):
    today_datetime = datetime.now(pytz.timezone('Asia/Kolkata'))
    today = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%Y-%m-%d')
    interest_rate = 10
    expiry_date_uniques = hd_option_chain.expiryDate.unique()
    connect = sqlite3.connect("data/database.db")
    cursor = connect.cursor()
    for expiry in expiry_date_uniques:
        expiry_date = datetime.strftime(datetime.strptime(expiry, '%d-%b-%Y'), '%Y-%m-%d')
        end_expiry_date = today_datetime + timedelta(days=45)
        expiry_datetime = datetime.strptime(expiry + '-15-30-+0530', '%d-%b-%Y-%H-%M-%z')
        underlying_value = hd_option_chain.to_dict('records')[0]['underlyingValue']
        if expiry_datetime <= end_expiry_date:
            time_till_expiry = (expiry_datetime - today_datetime).total_seconds() / (60 * 60 * 24)
            expiry_option_chain = hd_option_chain.loc[hd_option_chain.expiryDate == expiry]
            expiry_option_chain['delta'] = expiry_option_chain.apply(
                lambda row: delta_calculation(row, underlying_value, interest_rate, time_till_expiry), axis=1)

            atm_min_diff = sys.maxsize
            atm_strike = underlying_value
            atm_last_price = 0

            pe_25_min_dff = sys.maxsize
            pe_25_strike = underlying_value
            pe_25_last_price = 0

            ce_25_min_diff = sys.maxsize
            ce_25_strike = underlying_value
            ce_25_last_price = 0

            for index, row in expiry_option_chain.iterrows():
                if row.delta is not None:
                    delta_diff = abs(abs(row.delta) * 100 - 50.0)
                    if delta_diff < atm_min_diff:
                        atm_min_diff = delta_diff
                        atm_strike = row.strikePrice
                        atm_last_price = row.lastPrice
                if row.delta is not None :
                    if row.optiontype == PE:
                        delta_diff = abs(abs(row.delta) * 100 - 25.0)
                        if delta_diff < pe_25_min_dff:
                            pe_25_min_dff = delta_diff
                            pe_25_strike = row.strikePrice
                            pe_25_last_price = row.lastPrice
                    if row.optiontype == CE:
                        delta_diff = abs(abs(row.delta) * 100 - 25.0)
                        if delta_diff < ce_25_min_diff:
                            ce_25_min_diff = delta_diff
                            ce_25_strike = row.strikePrice
                            ce_25_last_price = row.lastPrice

            pe_strike_diff = abs(atm_strike - pe_25_strike)
            pe_price_diff = abs(atm_last_price - pe_25_last_price)
            ds_pe_profit = 0
            if pe_price_diff != 0 and pe_strike_diff != 0:
                ds_pe_profit = pe_price_diff/pe_strike_diff * 100

            ce_strike_diff = abs(ce_25_strike - atm_strike)
            ce_price_diff = abs(atm_last_price - ce_25_last_price)
            ds_ce_profit = 0
            if ce_price_diff != 0 and ce_strike_diff != 0:
                ds_ce_profit = ce_price_diff/ce_strike_diff * 100

            cursor.execute('''
                INSERT INTO OPTION_ANALYSIS 
                (date, symbol, expiry, ds_pe_profit, ds_ce_profit, ds_atm, ds_ce, ds_pe)  
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol, expiry) DO UPDATE 
                SET ds_pe_profit=?, ds_ce_profit=?, ds_atm=?, ds_ce=?, ds_pe=?
                WHERE date=? AND symbol=? AND expiry=?''',
                           (today, symbol, expiry_date,
                            ds_pe_profit, ds_ce_profit, atm_strike, ce_25_strike, pe_25_strike,
                            ds_pe_profit, ds_ce_profit, atm_strike, ce_25_strike, pe_25_strike,
                            today, symbol, expiry_date))
    connect.commit()
    connect.close()


# Main Code Starts Here
# Buy at OTM
# Shorts at ATM
def analyze_option_chain(symbol):
    today = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%d-%m-%Y-%H')
    path = 'data/optionchain/' + today + '/' + symbol
    if not os.path.exists('data/optionchain/' + today):
        os.mkdir('data/optionchain/' + today)
    if not os.path.exists('data/optionchain/' + today + '/' + symbol):
        os.mkdir('data/optionchain/' + today + '/' + symbol)
    if not os.path.exists('data/optionchain/' + today + '/' + symbol + '/sentiment'):
        os.mkdir('data/optionchain/' + today + '/' + symbol + '/sentiment')
    try:
        nse = NSE()
        if symbol in indices:
            option_chain = nse.fetch_index_chain_data(symbol)
        else:
            option_chain = nse.fetch_equity_chain_data(symbol)

        stock_price = option_chain['records']['underlyingValue']
        hd_option_chain_df = create_hd_option_chain_df(option_chain['records']['data'])
        iv_analysis(symbol, stock_price, hd_option_chain_df)
        pcr_analysis(symbol, hd_option_chain_df)
        delta_strategy(symbol, hd_option_chain_df)
    except Exception as e:
        print("Re Processing " + symbol)
        print(e)
        analyze_option_chain(symbol)


derivative_equities = nse.list_of_derivatives()
for symbol in derivative_equities:
    print("processing " + symbol)
    analyze_option_chain(symbol)

