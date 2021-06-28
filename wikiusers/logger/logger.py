from termcolor import colored
from datetime import datetime
from typing import Optional


def _log_scopes(lang: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> str:
    lang_str = colored(f'{{{lang}}}', 'yellow') if lang else ''
    asset_str = colored(f'{{{month}}}', 'yellow') if month else ''
    scope_str = colored(f'{{{scope}}}', 'cyan') if scope else ''
    return ' '.join([txt for txt in [lang_str, asset_str, scope_str] if txt])


def _log(*args: list[str], tag: str, colour: str, lang: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> None:
    timestamp = datetime.now().isoformat()
    timestamp_str = colored(f'[{timestamp}]', 'cyan')
    tag = colored(f'[{tag}]', colour, attrs=['bold'])
    text = _log_scopes(lang=lang, month=month, scope=scope)
    print(f'{timestamp_str} {tag} {text}', *args)


def info(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, scope=scope, tag='INFO', colour='blue')


def succ(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, scope=scope, tag='SUCC', colour='green')


def debug(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, scope=scope, tag='DEBUG', colour='grey')


def warn(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, scope=scope, tag='WARN', colour='yellow')


def err(*args: list[str], lang: Optional[str] = None, month: Optional[str] = None, scope: Optional[str] = None) -> None:
    _log(*args, lang=lang, month=month, scope=scope, tag='ERROR', colour='red')