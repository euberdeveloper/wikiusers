import bz2
from collections import Counter
from datetime import datetime
from pathlib import Path

from wikiusers import logger
from wikiusers.rawprocessor.utils import Uploader,  WhdtKeys,  EVENTS_MAP
from .misc import two_digits, new_month_obj, new_user_insert_obj, new_user_update_obj
from .parser import parse_date, parse_int, parse_bool, parse_str_array


class Analyzer:
    def __init_globals(self) -> None:
        self.user_exists = set()
        self.user_document = {}
        self.user_document_update = {}
        self.user_month_events = {}
        self.user_alter_groups = {}
        self.user_alter_blocks = {}
        self.user_helper_info = {}

    def __update_month_object(self, uid: str, namespace: str, event_type: str, timestamp: datetime, minor_edit: bool, page_id: str, page_seconds_since_previous_revision: str) -> None:
        if uid not in self.user_month_events:
            self.user_month_events[uid] = new_month_obj()
            user_month = self.user_month_events[uid]
            user_month['first_event'] = timestamp
            user_month['activity_days'] = 1
        else:
            user_month = self.user_month_events[uid]

        if 'namespaces' not in user_month:
            user_month['namespaces'] = {}

        user_month_namespaces = user_month['namespaces']

        if namespace not in user_month_namespaces:
            user_month_namespaces[namespace] = {}

        if uid not in self.user_helper_info:
            self.user_helper_info[uid] = {}
        user_helper_info = self.user_helper_info[uid]

        user_month_namespace = user_month_namespaces[namespace]
        if event_type not in user_month_namespace:
            user_month_namespace[event_type] = 1
        else:
            user_month_namespace[event_type] += 1
        if minor_edit:
            if 'minor_edits' not in user_month_namespace:
                user_month_namespace['minor_edits'] = 1
            else:
                user_month_namespace['minor_edits'] += 1

        if 'last_event' in user_month:
            last_timestamp = user_month['last_event']
            day_diff = timestamp.day - last_timestamp.day
            if day_diff == 0:
                user_month['secs_since_same_day_event'] += (timestamp - last_timestamp).total_seconds()
                user_month['secs_since_same_day_event_count'] += 1
            else:
                user_month['activity_days'] += 1
                if day_diff > user_month['max_inact_interval']:
                    user_month['max_inact_interval'] = day_diff

        else:
            user_month['secs_since_same_day_event'] = 0
            user_month['secs_since_same_day_event_count'] = 0

        if page_id != '':
            if 'pages' not in user_helper_info:
                user_helper_info['pages'] = Counter()
            user_helper_info['pages'].update([page_id])

        if page_seconds_since_previous_revision is not None:
            if 'pages_seconds' not in user_month:
                user_month['pages_seconds'] = page_seconds_since_previous_revision
                user_month['pages_seconds_count'] = 1
            else:
                user_month['pages_seconds'] += page_seconds_since_previous_revision
                user_month['pages_seconds_count'] += 1

        user_month['last_event'] = timestamp

    def __check_if_new_month(self, timestamp: datetime, check: bool) -> None:
        month = timestamp.month
        if (not check or month != self.current_month):
            month = two_digits(month)
            year = str(timestamp.year)
            logger.info(f'New month {month}/{year}', lang=self.lang, scope='ANALYZER')
            self.uploader.upload_to_db(
                self.current_year,
                self.current_month,
                self.user_document,
                self.user_document_update,
                self.user_month_events,
                self.user_alter_groups,
                self.user_alter_blocks,
                self.user_helper_info
            )
            self.current_month = month
            self.current_year = year

    def __analyze_user_create(self, parts: list[str], timestamp: datetime) -> None:
        uid = parse_int(parts[WhdtKeys.user_id])

        if uid is not None:
            username = parts[WhdtKeys.user_text]
            creation_timestamp = parse_date(parts[WhdtKeys.user_creation_timestamp])
            registration_timestamp = parse_date(parts[WhdtKeys.user_registration_timestamp])
            is_bot = parse_bool(parts[WhdtKeys.user_is_bot_by])
            groups = parse_str_array(parts[WhdtKeys.user_groups])
            blocks = parse_str_array(parts[WhdtKeys.user_blocks])
            self.user_document[uid] = new_user_insert_obj(uid, username, creation_timestamp,
                                                          registration_timestamp, is_bot, groups, blocks)
            self.user_document_update[uid] = new_user_update_obj(
                uid, username, creation_timestamp, registration_timestamp, is_bot, groups, blocks)

            current_groups = parse_str_array(parts[WhdtKeys.event_user_groups_historical])
            if uid not in self.user_alter_groups:
                self.user_alter_groups[uid] = [{'t': timestamp, 'g': current_groups}]
            else:
                self.user_alter_groups[uid].append({'t': timestamp, 'g': current_groups})

            current_blocks = parse_str_array(parts[WhdtKeys.event_user_blocks_historical])
            if uid not in self.user_alter_blocks:
                self.user_alter_blocks[uid] = [{'t': timestamp, 'g': current_blocks}]
            else:
                self.user_alter_blocks[uid].append({'t': timestamp, 'g': current_blocks})

    def __analyze_user_altergroups(self, parts: list[str], timestamp: datetime) -> None:
        uid = parse_int(parts[WhdtKeys.user_id])
        if uid != '' and uid is not None:
            if uid not in self.user_exists:
                username = parts[WhdtKeys.user_text]
                creation_timestamp = parse_date(parts[WhdtKeys.user_creation_timestamp])
                registration_timestamp = parse_date(parts[WhdtKeys.user_registration_timestamp])
                is_bot = parts[WhdtKeys.user_is_bot_by] != ''
                groups = parse_str_array(parts[WhdtKeys.user_groups])
                blocks = parse_str_array(parts[WhdtKeys.user_blocks])
                self.user_document[uid] = new_user_insert_obj(
                    uid, username, creation_timestamp, registration_timestamp, is_bot, groups, blocks)
                self.user_exists.add(uid)

            current_groups = parse_str_array(parts[WhdtKeys.event_user_groups_historical])
            if uid not in self.user_alter_groups:
                self.user_alter_groups[uid] = [{'t': timestamp, 'g': current_groups}]
            else:
                self.user_alter_groups[uid].append({'t': timestamp, 'g': current_groups})

            current_blocks = parse_str_array(parts[WhdtKeys.event_user_blocks_historical])
            if uid not in self.user_alter_blocks:
                self.user_alter_blocks[uid] = [{'t': timestamp, 'g': current_blocks}]
            else:
                self.user_alter_blocks[uid].append({'t': timestamp, 'g': current_blocks})

    def __analyze_page_or_revision(self, event_type: str, timestamp: datetime, parts: list[str]) -> None:
        uid = parse_int(parts[WhdtKeys.event_user_id])

        if uid != '' and uid is not None:
            if uid not in self.user_exists:
                username = parts[WhdtKeys.event_user_text]
                creation_timestamp = parse_date(parts[WhdtKeys.event_user_creation_timestamp])
                registration_timestamp = parse_date(parts[WhdtKeys.event_user_registration_timestamp])
                is_bot = parts[WhdtKeys.event_user_is_bot_by] != ''
                groups = parse_str_array(parts[WhdtKeys.event_user_groups])
                blocks = parse_str_array(parts[WhdtKeys.event_user_blocks])
                self.user_document[uid] = new_user_insert_obj(
                    uid, username, creation_timestamp, registration_timestamp, is_bot, groups, blocks)
                self.user_exists.add(uid)

            minor_edit = parts[WhdtKeys.revision_minor_edit] == 'true'
            page_id = parts[WhdtKeys.page_id]
            page_seconds_since_previous_revision = parse_int(parts[WhdtKeys.page_seconds_since_previous_revision])
            namespace = parse_int(parts[WhdtKeys.page_namespace])
            namespace = f'n{namespace}' if namespace is not None else 'unknown'
            self.__update_month_object(uid, namespace, EVENTS_MAP[event_type], timestamp,
                                       minor_edit, page_id, page_seconds_since_previous_revision)

    def __init__(
        self,
        path: Path,
        start_month: int,
        start_year: int,
        lang: str,
        database: str
    ):
        self.path = path
        self.lang = lang

        self.current_month = '01' if start_month is None else two_digits(start_month)
        self.current_year = str(start_year)

        self.uploader = Uploader(database, lang)
        self.__init_globals()

    def analyze(self) -> None:
        with bz2.open(self.path, 'rt') as input:
            for line in input:
                parts = line.split('\t')
                timestamp = parse_date(parts[WhdtKeys.event_timestamp])
                self.__check_if_new_month(timestamp, True)

                event_entity = parts[WhdtKeys.event_entity]
                event_type = parts[WhdtKeys.event_type]

                if event_entity == 'revision':
                    self.__analyze_page_or_revision('edit', timestamp, parts)
                elif event_entity == 'page':
                    self.__analyze_page_or_revision(event_type, timestamp, parts)
                elif event_entity == 'user' and event_type == 'create':
                    self.__analyze_user_create(parts, timestamp)
                elif event_entity == 'user' and event_type == 'altergroups':
                    self.__analyze_user_altergroups(parts, timestamp)
        self.__check_if_new_month(timestamp, False)
