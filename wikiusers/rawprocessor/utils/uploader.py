import math
from collections import Counter
from pymongo import MongoClient, UpdateOne, ASCENDING

from wikiusers import logger


class Uploader:
    def __create_index(self) -> None:
        self.collection.create_index([('id', ASCENDING)], name='id_index', unique=True)
        logger.debug('Created id index', lang=self.lang, scope='Uploader')

    def __calc_entropy(self, counter: Counter) -> float:
        values = counter.values()
        sum_of_numbers = sum(value for value in values)
        probs = [value / sum_of_numbers for value in values]
        return -sum([prob * math.log(prob) for prob in probs])

    def __parse_pages_counter(self, pages_counter: Counter) -> dict:
        n = len(pages_counter)
        most_common_5 = dict(pages_counter.most_common(5))
        entropy = self.__calc_entropy(pages_counter)
        return {'n': n, 'most_common_5': most_common_5, 'entropy': entropy}

    def __get_user_inserts(self, user_document: dict) -> list[dict]:
        inserts = []

        for uid in list(user_document.keys()):
            inserts.append(user_document[uid])
            user_document.pop(uid)

        return inserts

    def __get_user_updates(self, user_document_update: dict) -> list[dict]:
        updates = []

        for uid in list(user_document_update.keys()):
            if uid in user_document_update:
                updates.append(UpdateOne({'id': uid}, {'$set': user_document_update[uid]}))
                user_document_update.pop(uid)

        return updates

    def __get_events_updates(self, month: str, year: str, user_month_events: dict, user_helper_info: dict) -> list[dict]:
        updates = []

        for uid in list(user_month_events.keys()):
            month_obj = user_month_events[uid]
            if uid in user_helper_info:
                if 'pages' in user_helper_info[uid]:
                    month_obj['pages'] = self.__parse_pages_counter(user_helper_info[uid]['pages'])
                    user_helper_info[uid].pop('pages')
            updates.append(UpdateOne({'id': uid}, {'$set': {f'events.per_month.{year}.{month}': month_obj}}))
            user_month_events.pop(uid)

        return updates

    def __get_user_alters(self, user_alter_groups: dict, user_alter_blocks: dict) -> list[dict]:
        updates = []

        for uid in list(user_alter_groups.keys()):
            if uid in user_alter_groups:
                updates.append(UpdateOne({'id': uid}, {'$push': {'alter.groups': {'$each': user_alter_groups[uid]}}}))
                user_alter_groups.pop(uid)

        for uid in list(user_alter_blocks.keys()):
            if uid in user_alter_blocks:
                updates.append(UpdateOne({'id': uid}, {'$push': {'alter.blocks': {'$each': user_alter_blocks[uid]}}}))
                user_alter_blocks.pop(uid)

        return updates

    def __get_user_history_usernames(self, user_history_usernames: dict) -> list[dict]:
        updates = []

        for uid in list(user_history_usernames.keys()):
            if uid in user_history_usernames:
                updates.append(
                    UpdateOne({'id': uid}, {'$push': {'history_usernames': {'$each': user_history_usernames[uid]}}}))
                user_history_usernames.pop(uid)

        return updates

    def __init__(self, database: str, lang: str):
        self.lang = lang
        self.connection = MongoClient()
        self.database = self.connection.get_database(database)
        self.collection = self.database.get_collection(f'{lang}wiki_raw')
        self.__create_index()

    def upload_to_db(
        self,
        year: str,
        month: str,
        user_document: dict,
        user_document_update: dict,
        user_month_events: dict,
        user_helper_info: dict,
        user_alter_groups: dict,
        user_alter_blocks: dict,
        user_history_usernames: dict
    ) -> None:
        logger.info(f'Uploading data', year=year, month=month, lang=self.lang, scope='UPLOADER')

        logger.debug(f'Retrieving user inserts', year=year, month=month, lang=self.lang, scope='UPLOADER')
        user_inserts = self.__get_user_inserts(user_document)
        logger.debug(f'Pushing user inserts', year=year, month=month, lang=self.lang, scope='UPLOADER')
        if (user_inserts):
            try:
                self.collection.insert_many(user_inserts, ordered=False)
            except:
                pass
        user_inserts.clear()

        logger.debug(f'Retrieving user updates', year=year, month=month, lang=self.lang, scope='UPLOADER')
        user_updates = self.__get_user_updates(user_document_update)
        logger.debug(f'Pushing user updates', year=year, month=month, lang=self.lang, scope='UPLOADER')
        if (user_updates):
            self.collection.bulk_write(user_updates)
        user_updates.clear()

        logger.debug(f'Retrieving events updates', year=year, month=month, lang=self.lang, scope='UPLOADER')
        events_updates = self.__get_events_updates(month, year, user_month_events, user_helper_info)
        logger.debug(f'Pushing events updates', year=year, month=month, lang=self.lang, scope='UPLOADER')
        if (events_updates):
            self.collection.bulk_write(events_updates)
        events_updates.clear()

        logger.debug(f'Retrieving user alters', year=year, month=month, lang=self.lang, scope='UPLOADER')
        user_alerts = self.__get_user_alters(user_alter_groups, user_alter_blocks)
        logger.debug(f'Pushing user alerts', year=year, month=month, lang=self.lang, scope='UPLOADER')
        if (user_alerts):
            self.collection.bulk_write(user_alerts)
        user_alerts.clear()

        logger.debug(f'Retrieving user history usernames', year=year, month=month, lang=self.lang, scope='UPLOADER')
        user_usernames_updates = self.__get_user_history_usernames(user_history_usernames)
        logger.debug(f'Pushing user history usernames', year=year, month=month, lang=self.lang, scope='UPLOADER')
        if (user_usernames_updates):
            self.collection.bulk_write(user_usernames_updates)
        user_usernames_updates.clear()

        logger.succ(f'Uploading data', year=year, month=month, lang=self.lang, scope='UPLOADER')
