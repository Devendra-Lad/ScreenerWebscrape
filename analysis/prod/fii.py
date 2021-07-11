import sqlite3
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import pytz

import pandas as pd

from external.nsdl import NSDL

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', 500)

connect = sqlite3.connect("data/database.db")
fii_date, fii = NSDL().fetch_latest_fii_investment()

c = connect.cursor()
c.execute("INSERT INTO THOUSANDFEET (date, fii) VALUES (?, ?) "
          "ON CONFLICT(date) DO UPDATE SET fii=? WHERE date=?",
          (fii_date.date(), fii, fii, fii_date.date()))
c.close()
connect.commit()

c = connect.cursor()
today = datetime.now(tz=pytz.timezone('Asia/Kolkata')).date()
old_date = (today - timedelta(days=20))
c.execute('SELECT * FROM THOUSANDFEET where date < ? AND date > ?', (today, old_date))
rows = c.fetchall()
c.close()

number_of_fii_days = 0
total_investment = 0
for row in rows:
    number_of_fii_days = number_of_fii_days + 1;
    total_investment = total_investment + float(row[1])

ema = total_investment / number_of_fii_days

c = connect.cursor()
c.execute('''INSERT INTO THOUSANDFEET (date, fii_ema) 
            VALUES (?, ?) 
            ON CONFLICT(date) 
            DO UPDATE SET fii_ema=? 
            WHERE date=?''', (fii_date.date(), format(ema, '.2f'), format(ema, '.2f'), fii_date.date()))
c.close()
connect.commit()

connect.close()
