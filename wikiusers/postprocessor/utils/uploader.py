from pymongo import MongoClient

from wikiusers import logger


class Uploader:

    def __check_if_collection_already_exists(self) -> None:
        db_collections = self.database.list_collection_names()
        if self.collection.name in db_collections:
            if self.force:
                logger.warn('Collection already exists: dropping',
                            lang=self.lang, scope='UPLOADER')
                self.collection.drop()
            else:
                logger.err('Collection already exists',
                           lang=self.lang, scope='UPLOADER')
                raise Exception(f'Collection already exists')

    def __init_connection(self) -> None:
        self.connection = MongoClient()
        self.database = self.connection.get_database(self.database)
        self.collection = self.database.get_collection(f'{self.lang}wiki')
        self.__check_if_collection_already_exists()

    def __init__(
        self,
        database: str,
        lang: str,
        force: bool
    ):
        self.database = database
        self.lang = lang
        self.force = force
        self.__init_connection()

    def upload_users(self, user_batch: list[dict]) -> None:
        self.collection.insert_many(user_batch)
