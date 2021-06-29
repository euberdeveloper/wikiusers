from dateutil import parser as date_parser


def parse_date(date: str):
    return date_parser.parse(date) if date != '' else None


def parse_int(num: str):
    return int(num) if num != '' else None


def parse_str_array(arr: str):
    return arr.split(',') if arr != '' else []
