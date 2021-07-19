from datetime import datetime, timedelta
import pytz
from pandas.tseries.offsets import BDay


class TimeUtils:
    # 0 = Monday, 1=Tuesday, 2=Wednesday...
    def next_weekday(self, date, weekday):
        days_ahead = weekday - date.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        return date + timedelta(days_ahead)

    def getLastWorkingDay(self):
        now = datetime.now(tz=pytz.timezone('Asia/Kolkata'))
        return now - timedelta(max(1, (now.weekday() + 6) % 7 - 3)) - BDay(0)

    def getOptimalWeeklyExpiry(self):
        today = datetime.now(tz=pytz.timezone('Asia/Kolkata')).date()
        return self.next_weekday(today, 3) + timedelta(weeks=1)

    def getUnifiedFormatedDateStr(self, date):
        return datetime.strftime(date, '%Y-%m-%d')

    def getOptimalMonthlyExpiry(self):
        today = datetime.now(pytz.timezone('Asia/Kolkata'))
        day_itr = today
        current_month = today.month
        monthly_expiry_day = today
        crossed_current_month_expiry = 1
        while day_itr.month == current_month:
            if day_itr.weekday() == 3:
                crossed_current_month_expiry = 0
                monthly_expiry_day = day_itr
            day_itr += timedelta(days=1)

        if crossed_current_month_expiry == 1:
            day_itr += timedelta(weeks=2)
            current_month = day_itr.month
            while day_itr.month == current_month:
                if day_itr.weekday() == 3:
                    monthly_expiry_day = day_itr
                day_itr += timedelta(days=1)
        else:
            diff = monthly_expiry_day - today
            if diff.days < 5:
                day_itr += timedelta(weeks=3)
                current_month = day_itr.month
                while day_itr.month == current_month:
                    if day_itr.weekday() == 3:
                        monthly_expiry_day = day_itr
                    day_itr += timedelta(days=1)
        return monthly_expiry_day.date()

    def getToday(self):
        return datetime.now(pytz.timezone('Asia/Kolkata'))
