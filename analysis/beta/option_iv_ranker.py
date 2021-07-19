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
from analysis.utils.number_utils import NumberUtils
from analysis.utils.timeutils import TimeUtils

timeUtils = TimeUtils()

connect = sqlite3.connect("data/database.db")
cursor = connect.cursor()


today_str = timeUtils.getUnifiedFormatedDateStr(timeUtils.getToday())
optimal_expiry_str = timeUtils.getUnifiedFormatedDateStr(timeUtils.getOptimalMonthlyExpiry())
cursor.execute('''SELECT count(symbol) FROM OPTION_ANALYSIS where date=? and expiry=? Order By iv DESC''', (today_str, optimal_expiry_str))
result = cursor.fetchone()
top_20_record_number = math.floor(result[0] * 20 / 100)


cursor.execute('''SELECT symbol, iv FROM OPTION_ANALYSIS where date=? and expiry=? Order By iv DESC''', (today_str, optimal_expiry_str))
result = cursor.fetchall();

print("Top 20% IV")
print(result[top_20_record_number][1])

cursor.execute('''SELECT symbol, volume FROM OPTION_ANALYSIS where date=? and expiry=? Order By volume DESC''', (today_str, optimal_expiry_str))
result = cursor.fetchall();
print("Top 20% Volume")
print(result[top_20_record_number][1])

cursor.execute('''SELECT symbol, oi FROM OPTION_ANALYSIS where date=? and expiry=? Order By oi DESC''', (today_str, optimal_expiry_str))
result = cursor.fetchall();
print("Top 20% OI")
print(result[top_20_record_number][1])

cursor.execute('''SELECT symbol, ds_pe_profit FROM OPTION_ANALYSIS where date=? and expiry=? Order By ds_pe_profit DESC''', (today_str, optimal_expiry_str))
result = cursor.fetchall();
print("Top 20% DS PE Profit")
print(result[top_20_record_number][1])

cursor.execute('''SELECT symbol, spread FROM OPTION_ANALYSIS where date=? and expiry=? Order By spread ASC''', (today_str, optimal_expiry_str))
result = cursor.fetchall();
print("Top 20% SPREAD")
print(result[top_20_record_number][1])



connect.close()
