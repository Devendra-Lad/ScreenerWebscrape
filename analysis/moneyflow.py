import pandas as pd

from external.nse import NSE
from external.nsdl import NSDL
from external.bse import BSE
from external.sebi import SEBI
import sqlite3

pd.options.mode.chained_assignment = None

pd.set_option('display.max_columns', 500)

nse = NSE()
nsdl = NSDL()
bse = BSE()
sebi = SEBI()

fii_date, fii_nsdl = nsdl.fetch_latest_fii_investment()
category_date, bank_nse, dfi_nse, prop_nse, retail_nse = nse.fetch_category_turnover_data(0)
dii_date, retail_bse, nri_bse, prop_bse, dii_bse = bse.fetch_bse_data(0)
mfi_date, mfi_sebi = sebi.fetch_latest_mfi()

connect = sqlite3.connect("../data/moneyflow.db")
c = connect.cursor()
c.execute("INSERT INTO FLOW_BY_DATE (date, fii_nsdl) VALUES (?, ?) "
          "ON CONFLICT(date) DO UPDATE SET fii_nsdl=? WHERE date=?", (fii_date.date(), fii_nsdl, fii_nsdl, fii_date.date(),))
c.execute("INSERT INTO FLOW_BY_DATE (date, bank_nse, dfi_nse, prop_nse, retail_nse) VALUES (?, ?, ?, ?, ?) "
          "ON CONFLICT(date) DO UPDATE SET bank_nse=?, dfi_nse=?, prop_nse=?, retail_nse=? WHERE date=?",
          (category_date.date(), bank_nse, dfi_nse, prop_nse, retail_nse, bank_nse, dfi_nse, prop_nse, retail_nse,
           category_date.date()))
c.execute("INSERT INTO FLOW_BY_DATE (date, retail_bse, nri_bse, prop_bse, dii_bse) VALUES (?, ?, ?, ?, ?) "
          "ON CONFLICT(date) DO UPDATE SET retail_bse=?, nri_bse=?, prop_bse=?, dii_bse=? WHERE date=?",
          (dii_date.date(), retail_bse, nri_bse, prop_bse, dii_bse, retail_bse, nri_bse, prop_bse, dii_bse,
           dii_date.date()))
c.execute("INSERT INTO FLOW_BY_DATE (date, mfi_sebi) VALUES (?, ?) "
          "ON CONFLICT(date) DO UPDATE SET mfi_sebi=? WHERE date=?", (mfi_date.date(), mfi_sebi, mfi_sebi, mfi_date.date(),))
connect.commit()
connect.close()

# https://www.youtube.com/watch?v=qywJqPY2Swo Completed
# https://www.youtube.com/watch?v=p1G4z_q6Beg&list=UUqvVj1LkOpA8tjb7RadTvOg Completed
