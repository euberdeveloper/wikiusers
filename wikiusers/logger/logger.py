from termcolor import colored
from datetime import datetime
from typing import Optional


def _get_asset_str(month: Optional[str], year: Optional[str]) -> str:
    separator = '/' if month and year else ''
    year = '' if year is None else year
    month = '' if month is None else month
    return f'{month}{separator}{year}'


def _log_scopes(lang: Optional[str] = None, year: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> str:
    lang_str = colored(f'{{{lang}}}', 'yellow') if lang else ''
    asset_str = colored(f'{{{_get_asset_str(month, year)}}}', 'yellow') if month else ''
    scope_str = colored(f'{{{scope}}}', 'cyan') if scope else ''
    return ' '.join([txt for txt in [lang_str, asset_str, scope_str] if txt])


def _log(*args: list[str], tag: str, colour: str, lang: Optional[str] = None, month: Optional[str] = None, year: Optional[str] = None, scope: Optional[str] = None) -> None:
    timestamp = datetime.now().isoformat()
    timestamp_str = colored(f'[{timestamp}]', 'cyan')
    tag = colored(f'[{tag}]', colour, attrs=['bold'])
    text = _log_scopes(lang=lang, month=month, year=year, scope=scope)
    print(f'{timestamp_str} {tag} {text}', *args, flush=True)


def info(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, year: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, year=year, scope=scope, tag='INFO', colour='blue')


def succ(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, year: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, year=year, scope=scope, tag='SUCC', colour='green')


def debug(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, year: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, year=year, scope=scope, tag='DEBUG', colour='grey')


def warn(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, year: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, year=year, scope=scope, tag='WARN', colour='yellow')


def err(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, year: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, year=year, scope=scope, tag='ERROR', colour='red')
