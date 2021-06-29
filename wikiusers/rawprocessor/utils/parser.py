from typing import Union
from datetime import datetime
from dateutil import parser as date_parser


def parse_date(date: str) -> Union[datetime, None]:
    return date_parser.parse(date) if date != '' else None


def parse_int(num: str) -> Union[int, None]:
    return int(num) if num != '' else None

def parse_bool(val: str) -> bool:
    return val != ''

def parse_str_array(arr: str) -> list[str]:
    return arr.split(',') if arr != '' else []
