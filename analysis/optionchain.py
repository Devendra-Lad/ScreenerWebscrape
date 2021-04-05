import math
import os
from datetime import datetime, timedelta

import pandas as pd
import pytz
from scipy.stats import norm

from external.nse import NSE

pd.options.mode.chained_assignment = None

pd.set_option('display.max_columns', 500)

nse = NSE()


def create_hd_option_chain_df(data):
    option_chain_df = pd.DataFrame()
    for record in data:
        try:
            if 'CE' in record:
                option_chain_record_ce = record['CE']
                option_chain_record_ce['optiontype'] = 'CE'
                option_chain_df = option_chain_df.append(option_chain_record_ce, ignore_index=True)
            if 'PE' in record:
                option_chain_record_pe = record['PE']
                option_chain_record_pe['optiontype'] = 'PE'
                option_chain_df = option_chain_df.append(option_chain_record_pe, ignore_index=True)
        except:
            print(record)
    return option_chain_df.sort_values(by='openInterest', ascending=False)


def get_total_oi(option_chain_df):
    subset_option_chain_df = option_chain_df[['expiryDate', 'optiontype', 'openInterest', 'totalTradedVolume']]
    return subset_option_chain_df.groupby(['expiryDate', 'optiontype'], as_index=False).agg(
        {'openInterest': 'sum', 'totalTradedVolume': 'sum'})


def create_expiry_wise_view(option_chain_df):
    total_oi_data = get_total_oi(option_chain_df)
    expiries = total_oi_data.expiryDate.unique()
    analyze_data_frame = pd.DataFrame()
    for expiry in expiries:
        expiry_date = datetime.strptime(expiry, '%d-%b-%Y').timestamp()
        data_for_expiry = total_oi_data.loc[total_oi_data.expiryDate == expiry]
        pe = data_for_expiry.loc[total_oi_data.optiontype == 'PE']
        ce = data_for_expiry.loc[total_oi_data.optiontype == 'CE']
        if not ce.empty and not pe.empty:
            total_pe = pe.iloc[0]['openInterest']
            total_ce = ce.iloc[0]['openInterest']
            pe_ce_ratio = 1
            if total_ce > 0 and total_pe > 0:
                pe_ce_ratio = pe.iloc[0]['openInterest'] / ce.iloc[0]['openInterest']
            view = 'Normal'
            if pe_ce_ratio < 0.75:
                view = 'BEAR'
            if pe_ce_ratio > 1.25:
                view = 'BULL'
            analyze_data_frame = analyze_data_frame.append(
                {'expiryDate': expiry, 'pe_oi': total_pe, 'ce_oi': total_ce, 'pe_ce_ratio': pe_ce_ratio, 'view': view,
                 'expiry_date': expiry_date},
                ignore_index=True)
    analyze_data_frame = analyze_data_frame.sort_values(by='expiry_date', ascending=True)
    return analyze_data_frame[['expiryDate', 'pe_ce_ratio', 'view']]


def max_oi_buildup(option_chain_df):
    return option_chain_df.groupby(['strikePrice', 'optiontype'], as_index=False) \
        .agg({'openInterest': 'sum', 'totalTradedVolume': 'sum'})[
        ['strikePrice', 'optiontype', 'openInterest', 'totalTradedVolume']] \
        .sort_values(by='openInterest', ascending=False) \
        .head(4)


def analyze_sentiment(option_chain_single_record):
    sentiment = 'unknown'
    change_open_interest = option_chain_single_record['changeinOpenInterest']
    price_change = option_chain_single_record['change']

    # OI Increases - Premium Increases - Fresh Long Positions - Long Build Up
    if change_open_interest > 0 and price_change > 0:
        sentiment = 'long_build_up'

    # OI Decreases - Premium Decreases - Closing Short Positions - Long Unwinding
    if change_open_interest < 0 and price_change < 0:
        sentiment = 'long_unwinding'

    # OI Increase - Premium Decrease - Fresh Short Position - Short Build Up
    if change_open_interest > 0 and price_change < 0:
        sentiment = 'short_build_up'

    # OI Decreases - Premium Increases - Closing Short Positions - Short Covering
    if change_open_interest < 0 and price_change > 0:
        sentiment = 'short_covering'
    return sentiment


def expiry_sentiment_analysis(expiry, option_chain_df):
    expiry_df = option_chain_df.loc[option_chain_df.expiryDate == expiry]
    expiry_df['sentiment'] = expiry_df.apply(analyze_sentiment, axis=1)
    return expiry_df.loc[expiry_df.sentiment != 'unknown'][
        ['expiryDate', 'strikePrice', 'optiontype', 'changeinOpenInterest', 'change', 'sentiment']] \
        .sort_values(['optiontype', 'strikePrice'], ascending=[False, False])


def calculate_z(current_price, strikep_rice, iv, number_of_days):
    return math.log(strikep_rice / current_price) / (iv * 0.01 * math.sqrt(number_of_days / 365))


def iv_analysis(stock_price, option_chain_df):
    expiries = option_chain_df.expiryDate.unique()
    iv_expiry_wise_analysis_df = pd.DataFrame()
    for expiry in expiries:
        analyze_df = option_chain_df.copy()
        analyze_df = analyze_df.loc[analyze_df.expiryDate == expiry]
        analyze_df = analyze_df.sort_values('strikePrice', ascending=True)

        temp_df = analyze_df.loc[analyze_df.strikePrice > stock_price]
        if temp_df.empty:
            continue
        optimal_iv_strike_price = temp_df.loc[temp_df.strikePrice == temp_df.strikePrice.min()].iloc[0]['strikePrice']

        ce_ = analyze_df.loc[(analyze_df.strikePrice == optimal_iv_strike_price) & (analyze_df.optiontype == 'CE')]
        if ce_.empty:
            continue
        ce_optimal_iv = ce_.iloc[0]['impliedVolatility']
        pe_ = analyze_df.loc[(analyze_df.strikePrice == optimal_iv_strike_price) & (analyze_df.optiontype == 'PE')]
        if pe_.empty:
            continue
        pe_optimal_iv = pe_.iloc[0]['impliedVolatility']

        temp_df = analyze_df.loc[(analyze_df.strikePrice > optimal_iv_strike_price) & (analyze_df.optiontype == 'CE')]
        ce_max_ = temp_df.loc[temp_df.openInterest == temp_df.openInterest.max()]
        if ce_max_.empty:
            continue
        max_oi_ce_strike = ce_max_.iloc[0]['strikePrice']
        temp_df = analyze_df.loc[(analyze_df.strikePrice < optimal_iv_strike_price) & (analyze_df.optiontype == 'PE')]
        pe_max_ = temp_df.loc[temp_df.openInterest == temp_df.openInterest.max()]
        if pe_max_.empty:
            continue
        max_oi_pe_strike = pe_max_.iloc[0]['strikePrice']

        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        strike_time = pytz.timezone('Asia/Kolkata').localize(datetime.strptime(expiry, '%d-%b-%Y')) + timedelta(
            hours=15,
            minutes=30)

        time_until_expiry = (strike_time - now).days + (strike_time - now).seconds / 86400
        if pe_optimal_iv != 0 and ce_optimal_iv != 0 and time_until_expiry != 0:
            ce_win_prob_cdf = (1 - norm.cdf(
                calculate_z(stock_price, max_oi_ce_strike, pe_optimal_iv, time_until_expiry))) * 100
            pe_win_prob_cdf = norm.cdf(
                calculate_z(stock_price, max_oi_pe_strike, ce_optimal_iv, time_until_expiry)) * 100
            if ce_win_prob_cdf > pe_win_prob_cdf:
                market_sentiment = "BULL"
            else:
                market_sentiment = "BEAR"
            iv_analysis_record = {
                'expiry': expiry,
                'price': stock_price,
                'upper_bound': max_oi_ce_strike,
                'ce_iv': ce_optimal_iv,
                'lower_bound': max_oi_pe_strike,
                'pe_iv': pe_optimal_iv,
                'ce_win_probability': ce_win_prob_cdf,
                'pe_win_probability': pe_win_prob_cdf,
                'market_sentiment': market_sentiment
            }
            iv_expiry_wise_analysis_df = iv_expiry_wise_analysis_df.append(iv_analysis_record, ignore_index=True)
    return iv_expiry_wise_analysis_df


# Main Code Starts Here
# Buy at OTM
# Shorts at ATM
def analyze_option_chain(symbol):
    today = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%d-%m-%Y-%H')
    path = '../data/optionchain/' + today + '/' + symbol
    if not os.path.exists('../data/optionchain/' + today):
        os.mkdir('../data/optionchain/' + today)
    if not os.path.exists('../data/optionchain/' + today + '/' + symbol):
        os.mkdir('../data/optionchain/' + today + '/' + symbol)
    if symbol in indices:
        option_chain = nse.fetch_index_chain_data(symbol)
    else:
        option_chain = nse.fetch_equity_chain_data(symbol)
    hd_option_chain_df = create_hd_option_chain_df(option_chain['records']['data'])
    # Not saving HD Option Chain Anymore
    # hd_option_chain_df.to_csv(path + '/hd_option_chain.csv', index=False)
    create_expiry_wise_view(hd_option_chain_df).to_csv(path + '/total_oi_expiry_wise_view.csv', index=False)
    max_oi_buildup(hd_option_chain_df).to_csv(path + '/max_oi_build.csv', index=False)
    expiry_sentiment_analysis('31'
                              '-Dec-2020', hd_option_chain_df).to_csv(path + '/sentiment.csv', index=False)
    stock_price = option_chain['records']['underlyingValue']
    iv_analysis(stock_price, hd_option_chain_df).to_csv(path + '/iv_analysis.csv', index=False)


indices = ['NIFTY', 'BANKNIFTY']
analyze_option_chain('NIFTY')
analyze_option_chain('BANKNIFTY')