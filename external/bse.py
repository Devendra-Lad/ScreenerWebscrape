import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import pytz

class BSE:
    bse_session = requests.session()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:76.0) Gecko/20100101 Firefox/76.0',
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    def __init__(self):
        self.bse_session.get('https://www.bseindia.com', headers=self.headers)

    def fetch_bse_data(self, offset):
        url = 'https://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?flag=1'
        initialization_response = self.bse_session.get(url=url, headers=self.headers).text
        soup = BeautifulSoup(initialization_response, 'html.parser')
        event_target = soup.find('input', {'name': '__EVENTTARGET'}).get('value')
        event_argument = soup.find('input', {'name': '__EVENTARGUMENT'}).get('value')
        event_state = soup.find('input', {'name': '__VIEWSTATE'}).get('value')
        view_sate_generator = soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value')
        if soup.find('input', {'name': '__VIEWSTATEENCRYPTED'}) is not None:
            view_state_encrypted = soup.find('input', {'name': '__VIEWSTATEENCRYPTED'}).get('value')
        else:
            view_state_encrypted = ''
        event_validation = soup.find('input', {'name': '__EVENTVALIDATION'}).get('value')
        today = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
        last_working_day = today - timedelta(max(1, (today.weekday() + 6) % 7 - 3)) - BDay(offset)
        last_working_day_string = last_working_day.strftime('%d/%m/%Y')
        last_working_day_string_hidden = last_working_day.strftime('%m/%d/%Y')
        request_params = {
            "__EVENTTARGET": event_target,
            "__EVENTARGUMENT": event_argument,
            "__VIEWSTATE": event_state,
            "__VIEWSTATEGENERATOR": view_sate_generator,
            # If VIEW STATE is none then it is better to pass empty
            "__VIEWSTATEENCRYPTED": view_state_encrypted,
            "__EVENTVALIDATION": event_validation,
            "ctl00$ContentPlaceHolder1$hidFromDate": last_working_day_string_hidden,
            "ctl00$ContentPlaceHolder1$hidToDate": last_working_day_string_hidden,
            "ctl00$ContentPlaceHolder1$hidDMY": "D",
            "ctl00$ContentPlaceHolder1$hdflag": "1",
            "ctl00$ContentPlaceHolder1$DMY": "rdbDaily",
            "ctl00$ContentPlaceHolder1$txtFromDate": last_working_day_string,
            "ctl00$ContentPlaceHolder1$txtToDate": last_working_day_string,
            "ctl00$ContentPlaceHolder1$btnSubmit": "Submit"
        }

        response = self.bse_session.post(url=url, data=request_params, headers=self.headers).text
        result = BeautifulSoup(response, 'html.parser')
        data = result.find('div', {'id': 'ContentPlaceHolder1_divRecord'}).find('table').find_all('tr')[3].find_all('td')
        return last_working_day, float(data[3].text.replace(',', '')), float(data[4].text.replace(',', '')), \
            float(data[9].text.replace(',', '')), float(data[12].text.replace(',', ''))
