import sqlite3
import os

# Create data folder
if not os.path.exists('data'):
    os.mkdir('data')

# Run this file to create all necessary folders and resources
# Resource 1 : Creating Database

connect = sqlite3.connect("data/database.db")
cursor = connect.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS PROMOTER (
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    promoter_price REAL,
    curr_price REAL,
    premium REAL,
    max_entry_price REAL,
    stop_loss REAL,
    promoter_holding REAL,
    mf_sell REAL,
    pledged REAL,
    eps REAL,
    promoter_sold BOOL,
    secVal REAL,
    secAcq REAL,
    lpd TEXT,
    PRIMARY KEY(date, symbol))
''')
connect.commit()
connect.close()


connect = sqlite3.connect("data/database.db")
cursor = connect.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS OPTION_ANALYSIS (
    date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    expiry TEXT NOT NULL,
    spot REAL,
    pcr_view TEXT,
    iv_view TEXT,
    pcr TEXT,
    ub REAL,
    lb REAL,
    cep REAL,
    pep REAL,
    ceiv REAL,
    peiv REAL,
    PRIMARY KEY(date, symbol, expiry))
''')
connect.commit()
connect.close()

connect = sqlite3.connect("data/database.db")
cursor = connect.cursor()
cursor.execute('''DROP TABLE IF EXISTS FNO_CONSOLIDATED''')
cursor.execute('''CREATE TABLE FNO_CONSOLIDATED (
date TEXT,
fii_fi_long REAL NOT NULL,
fii_fi_short REAL NOT NULL,
fii_fi_r REAL NOT NULL,
fii_fs_long REAL NOT NULL,
fii_fs_short REAL NOT NULL,
fii_io_call_long REAL NOT NULL,
fii_io_put_long REAL NOT NULL,
fii_io_call_short REAL NOT NULL,
fii_io_put_short REAL NOT NULL,
fii_so_call_long REAL NOT NULL,
fii_so_put_long REAL NOT NULL,
fii_so_call_short REAL NOT NULL,
fii_so_put_short REAL NOT NULL,
dii_fi_long REAL NOT NULL,
dii_fi_short REAL NOT NULL,
dii_fs_long REAL NOT NULL,
dii_fs_short REAL NOT NULL,
dii_io_call_long REAL NOT NULL,
dii_io_put_long REAL NOT NULL,
dii_io_call_short REAL NOT NULL,
dii_io_put_short REAL NOT NULL,
dii_so_call_long REAL NOT NULL,
dii_so_put_long REAL NOT NULL,
dii_so_call_short REAL NOT NULL,
dii_so_put_short REAL NOT NULL,
client_fi_long REAL NOT NULL,
client_fi_short REAL NOT NULL,
client_fs_long REAL NOT NULL,
client_fs_short REAL NOT NULL,
client_io_call_long REAL NOT NULL,
client_io_put_long REAL NOT NULL,
client_io_call_short REAL NOT NULL,
client_io_put_short REAL NOT NULL,
client_so_call_long REAL NOT NULL,
client_so_put_long REAL NOT NULL,
client_so_call_short REAL NOT NULL,
client_so_put_short REAL NOT NULL,
pro_fi_long REAL NOT NULL,
pro_fi_short REAL NOT NULL,
pro_fs_long REAL NOT NULL,
pro_fs_short REAL NOT NULL,
pro_io_call_long REAL NOT NULL,
pro_io_put_long REAL NOT NULL,
pro_io_call_short REAL NOT NULL,
pro_io_put_short REAL NOT NULL,
pro_so_call_long REAL NOT NULL,
pro_so_put_long REAL NOT NULL,
pro_so_call_short REAL NOT NULL,
pro_so_put_short REAL NOT NULL,
PRIMARY KEY(date))''')
connect.commit()
connect.close()
