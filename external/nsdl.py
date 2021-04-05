import requests
from bs4 import BeautifulSoup
from datetime import datetime

class NSDL:
    nsdl_session = requests.session()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:76.0) Gecko/20100101 Firefox/76.0',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    def __init__(self):
        self.nsdl_session.get('https://www.nsdl.co.in/', headers=self.headers)

    def fetch_latest_fii_investment(self):
        url = 'https://www.fpi.nsdl.co.in/web/Reports/Latest.aspx'
        content = self.nsdl_session.get(url=url, headers=self.headers).content
        soup = BeautifulSoup(content, 'html.parser')
        tr_2 = soup.find('table', class_='tbls01').find_all('tr')[2]
        date = tr_2.find_all('td')[0].text
        return datetime.strptime(date, '%d-%b-%Y'), float(tr_2.find_all('td')[3].text) - \
            float(tr_2.find_all('td')[4].text)