import requests
from bs4 import BeautifulSoup
from datetime import datetime


class SEBI:
    sebi_session = requests.session()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:76.0) Gecko/20100101 Firefox/89.0',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    def __init__(self):
        self.sebi_session.get('https://www.sebi.gov.in', headers=self.headers, verify=False)

    def fetch_latest_mfi(self):
        url = 'https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doMfd=yes&type=1'
        response = self.sebi_session.get(url, headers=self.headers, verify=False).text
        souped = BeautifulSoup(response, 'html.parser')
        data = souped.find('div', {'class': 'org-table-1'}).find('table').find_all('tr')[1].find_all('td')

        return datetime.strptime(data[0].text, '%d %b, %Y'), float(data[2].text) - float(data[3].text)

