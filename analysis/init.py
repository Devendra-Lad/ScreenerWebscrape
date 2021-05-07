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