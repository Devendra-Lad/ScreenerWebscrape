import sqlite3
from datetime import datetime, timedelta

import pytz
from pandas.tseries.offsets import BDay

connect = sqlite3.connect("../data/database.db")

cursor = connect.cursor()

today = datetime.now(tz=pytz.timezone('Asia/Kolkata'))

latest_option_chain_data_date = (today - timedelta(max(1, (today.weekday() + 6) % 7 - 3)) - BDay(0)).strftime("%Y-%m-%d")


cursor.execute('''
SELECT DISTINCT(expiry_date) FROM option_chain as oc
WHERE oc.date = ?
ORDER BY oc.expiry_date ASC
''', (latest_option_chain_data_date,))
expiries = cursor.fetchall()



for expiry in expiries:
    cursor.execute('''
    SELECT expiry_date, 
           strike_price,
           tm,
           option_type, 
           coi,
           oi,
           coi_by_v
    FROM option_chain as oc
    WHERE oc.date = ? 
    AND oc.option_type = 'CE'
    AND oc.expiry_date = ?
    ORDER BY oc.coi DESC
    LIMIT 3
    ''', (latest_option_chain_data_date, expiry[0]))
    top_options_by_coi = cursor.fetchall()
    for row in top_options_by_coi:
        if row[5] > 9:
            print(row)
    cursor.execute('''
    SELECT expiry_date, 
           strike_price, 
           tm,
           option_type, 
           coi,
           oi,
           coi_by_v
    FROM option_chain as oc
    WHERE oc.date = ? 
    AND oc.option_type = 'PE'
    AND oc.expiry_date = ?
    ORDER BY oc.coi DESC
    LIMIT 3 
    ''', (latest_option_chain_data_date, expiry[0]))
    top_options_by_coi = cursor.fetchall()
    for row in top_options_by_coi:
        if row[5] > 9:
            print(row)
