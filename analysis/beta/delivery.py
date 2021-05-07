import pandas as pd
import os
from datetime import datetime
import pytz

from external.nse import NSE

pd.options.mode.chained_assignment = None

pd.set_option('display.max_columns', 500)

# Not working anymore
nse = NSE()

range_start = 0
range_end = 3

days = [0, 1, 2, 3]

bv1 = nse.fetch_eq_bhav_copy(0)
bv2 = nse.fetch_eq_bhav_copy(1)
bv3 = nse.fetch_eq_bhav_copy(2)
bv4 = nse.fetch_eq_bhav_copy(3)

final_copy = pd.concat([bv1, bv2, bv3, bv4], keys=days, names=['DAY', 'SYMBOL'])

print(final_copy)
final_copy = final_copy.swaplevel(0, 1).sort_index(axis=0)

print(final_copy)

def rising_delivery_percentage(data):
    is_delivery_rising = True
    try:
        for i in range(range_start + 1, range_end):
            is_delivery_rising = is_delivery_rising and data.loc[i - 1, 'DELIV_PER'].iloc[0] >= \
                                 data.loc[i, 'DELIV_PER'].iloc[0]
        return is_delivery_rising
    except:
        # print(data)
        return False


def rising_qty_rising(data):
    is_delivery_rising = True
    try:
        for i in range(range_start + 1, range_end):
            is_delivery_rising = is_delivery_rising and data.loc[data.DAY == i - 1, 'DELIV_QTY'].iloc[0] >= \
                                 data.loc[data.DAY == i, 'DELIV_QTY'].iloc[0]
        return is_delivery_rising
    except:
        # print(data)
        return False

def rising_price(data):
    is_price_rising = True
    try:
        for i in range(range_start + 1, range_end):
            is_price_rising = is_price_rising and data.loc[data.DAY == i - 1, 'CLOSE_PRICE'].iloc[0] >= \
                              data.loc[data.DAY == i, 'CLOSE_PRICE'].iloc[0]
        return is_price_rising
    except:
        # print(data)
        return False


final_copy.reset_index(level=0, inplace=True)

SYMBOLS = final_copy.SYMBOL.unique()

delivery_analysis = pd.DataFrame()

today = datetime.strftime(datetime.now(pytz.timezone('Asia/Kolkata')), '%d-%m-%Y')

if not os.path.exists('../data/eq-delivery/' + today):
    os.mkdir('../data/eq-delivery/' + today)

for symbol in SYMBOLS:
    symbol_final_copy = final_copy.loc[final_copy.SYMBOL == symbol, :]
    # print(symbol_final_copy)
    if len(symbol_final_copy.index) == range_end - range_start + 1:
        delivery = rising_delivery_percentage(symbol_final_copy)
        price = rising_price(symbol_final_copy)
        qty = rising_qty_rising(symbol_final_copy)
        # close_price = symbol_final_copy.loc[symbol_final_copy.DAY == 0, 'CLOSE_PRICE'].iloc[0]
        delivery_analysis = delivery_analysis.append(
            {'SYMBOL': symbol,
             'delivery': delivery,
             'price': price,
             'close_price': 0,
             'qty': qty},
            ignore_index=True)
delivery_analysis.to_csv('../data/eq-delivery/' + today + '/delivery.csv', index=False)
#
price_ = delivery_analysis.loc[(delivery_analysis.delivery == 1) & (delivery_analysis.price == 1) & (delivery_analysis.qty == 1), :]
price_ = price_.sort_values(by='close_price', ascending=False)
price_.to_csv('../data/eq-delivery/' + today + '/price.csv', index=False)

# covert into sql