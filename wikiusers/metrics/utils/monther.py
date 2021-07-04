from datetime import datetime


today_month_date = None
def get_today_month_date() -> datetime:
    global today_month_date
    if today_month_date is None:
        today_date = datetime.today()
        today_month_date = datetime(today_date.year, today_date.month, 1)
    return today_month_date

def get_month_date_from_key(key: str) -> datetime:
    year_str, month_str = key.split('-')
    return datetime(int(year_str), int(month_str), 1)

def get_diff_in_months(x: datetime, y: datetime) -> int:
   return (x.year - y.year) * 12 + (x.month - y.month)

def get_distance_in_months(x: datetime, y: datetime) -> int:
   return abs(get_diff_in_months(x, y))

def two_digits(n: int) -> str:
    return str(n) if n > 9 else '0' + str(n)

def get_key_from_date(d: datetime) -> str:
    return f'{d.year}-{two_digits(d.month)}'

