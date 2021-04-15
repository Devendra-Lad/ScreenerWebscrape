import math
import os
from datetime import datetime, timedelta

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
indices = ['NIFTY', 'BANKNIFTY']

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

nse = NSE()


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


def create_expiry_wise_view(option_chain_df):
    total_oi_data = get_total_oi(option_chain_df)
    expiries = total_oi_data.expiryDate.unique()
    analyze_data_frame = pd.DataFrame()
    for expiry in expiries:
        expiry_date = datetime.strptime(expiry, '%d-%b-%Y').timestamp()
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
            analyze_data_frame = analyze_data_frame.append(
                {EXPIRY_DATE: expiry, PE_OI: total_pe, CE_OI: total_ce, RATIO: pe_ce_ratio, VIEW: view,
                 'expiry_date': expiry_date},
                ignore_index=True)
    analyze_data_frame = analyze_data_frame.sort_values(by='expiry_date', ascending=True)
    return analyze_data_frame[[EXPIRY_DATE, RATIO, VIEW]]


def max_oi_buildup(option_chain_df):
    return option_chain_df.groupby([STRIKE_PRICE, OPTION_TYPE], as_index=False) \
        .agg({OPEN_INTEREST: 'sum', TOTAL_TRADED_VOLUME: 'sum'})[
        [STRIKE_PRICE, OPTION_TYPE, OPEN_INTEREST, TOTAL_TRADED_VOLUME]] \
        .sort_values(by=OPEN_INTEREST, ascending=False) \
        .head(4)


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


def calculate_z(current_price, strikep_rice, iv, number_of_days):
    return math.log(strikep_rice / current_price) / (iv * 0.01 * math.sqrt(number_of_days / 365))


def iv_analysis(stock_price, option_chain_df):
    expiries = option_chain_df.expiryDate.unique()
    iv_expiry_wise_analysis_df = pd.DataFrame()
    for expiry in expiries:
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
        if pe_optimal_iv != 0 and ce_optimal_iv != 0 and time_until_expiry != 0:
            ce_win_prob_cdf = (1 - norm.cdf(
                calculate_z(stock_price, max_oi_ce_strike, pe_optimal_iv, time_until_expiry))) * 100
            pe_win_prob_cdf = norm.cdf(
                calculate_z(stock_price, max_oi_pe_strike, ce_optimal_iv, time_until_expiry)) * 100
            if ce_win_prob_cdf > pe_win_prob_cdf:
                market_sentiment = BULL
            else:
                market_sentiment = BEAR
            iv_analysis_record = {
                EXPIRY: expiry,
                PRICE: stock_price,
                UPPER_BOUND: max_oi_ce_strike,
                CE_IV: ce_optimal_iv,
                LOWER_BOUND: max_oi_pe_strike,
                PE_IV: pe_optimal_iv,
                CE_WIN_PROBABILITY: ce_win_prob_cdf,
                PE_WIN_PROBABILITY: pe_win_prob_cdf,
                MARKET_SENTIMENT: market_sentiment
            }
            iv_expiry_wise_analysis_df = iv_expiry_wise_analysis_df.append(iv_analysis_record, ignore_index=True)
    return iv_expiry_wise_analysis_df


# Main Code Starts Here
# Buy at OTM
# Shorts at ATM
# TODO code is not working in current revision
# TODO move data to SQL or Google Sheets.
def analyze_option_chain(symbol):
    today = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%d-%m-%Y-%H')
    path = '../data/optionchain/' + today + '/' + symbol
    if not os.path.exists('../data/optionchain/' + today):
        os.mkdir('../data/optionchain/' + today)
    if not os.path.exists('../data/optionchain/' + today + '/' + symbol):
        os.mkdir('../data/optionchain/' + today + '/' + symbol)
    if not os.path.exists('../data/optionchain/' + today + '/' + symbol + '/sentiment'):
        os.mkdir('../data/optionchain/' + today + '/' + symbol + '/sentiment')
    if symbol in indices:
        option_chain = nse.fetch_index_chain_data(symbol)
    else:
        option_chain = nse.fetch_equity_chain_data(symbol)
    hd_option_chain_df = create_hd_option_chain_df(option_chain['records']['data'])
    create_expiry_wise_view(hd_option_chain_df).to_csv(path + '/total_oi_expiry_wise_view.csv', index=False)
    max_oi_buildup(hd_option_chain_df).to_csv(path + '/max_oi_build.csv', index=False)
    expiries = hd_option_chain_df.expiryDate.unique()
    for expiry in expiries :
        expiry_sentiment_analysis(expiry, hd_option_chain_df).to_csv(path + '/sentiment/' + expiry + '.csv', index=False)
    stock_price = option_chain['records']['underlyingValue']
    iv_analysis(stock_price, hd_option_chain_df).to_csv(path + '/iv_analysis.csv', index=False)


analyze_option_chain('NIFTY')
analyze_option_chain('BANKNIFTY')
