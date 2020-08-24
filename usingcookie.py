import requests


insiderTradingData = 'https://www.nseindia.com/api/corporates-pit?index=equities&from_date=21-07-2020&to_date=21-08-2020'

cookie_dict = {'bm_sv': '0E81D40DF0F52E64D2A3362A7105F3DA~JdTrFdcxPqLFz6sq0GA9FqGgBEs3VbZAxQjfjHjG4h/FL7CcohRUYNPNB+DM0MzMODidU9z3+CB/xO1zzH9b9gXJBbXgLR8kWR8IdJwmHMEoAMMuXzNvUn2mjrOoG9znUnZNbuyK3eOapesGDNuquj/2Wg7J1FLEjLCCFKtJVq0'}
session = requests.session()
for cookie in cookie_dict:
    session.cookies.set(cookie, cookie_dict[cookie])



headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:76.0) Gecko/20100101 Firefox/76.0',
     "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

x = session.get(url=insiderTradingData, headers=headers)

print(x.text)