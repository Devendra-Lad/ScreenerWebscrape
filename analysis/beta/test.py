import time

from external.nse import NSE

nse = NSE()

list = nse.list_of_derivatives()

for symbol in list:
    time.sleep(1)
    nse.fetch_equity_chain_data(symbol)

