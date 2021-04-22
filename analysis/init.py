import sqlite3
import os

# Create data folder
if not os.path.exists('../data'):
    os.mkdir('../data')


# Run this f
# ile to create all necessary folders and resources
# Resource 1 : Creating Database

connect = sqlite3.connect("../data/database.db")
cursor = connect.cursor()

cursor.execute('''
    CREATE TABLE PROMOTER (
    date TEXT NOT NULL,
    symbol REAL NOT NULL,
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
