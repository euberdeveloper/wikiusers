from pymongo import ASCENDING, MongoClient
from pymongo.operations import UpdateOne

from wikiusers import logger


class Uploader:

    def __init_connection(self) -> None:
        self.connection = MongoClient()
        self.database = self.connection.get_database(self.database)
        self.collection = self.database.get_collection(f'{self.lang}wiki')

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

    def check_if_collection_already_exists(self) -> None:
        db_collections = self.database.list_collection_names()
        if f'{self.lang}wiki' in db_collections:
            if self.force:
                logger.warn('Collection already exists: dropping',
                            lang=self.lang, scope='UPLOADER')
                self.collection.drop()
            else:
                logger.err('Collection already exists',
                           lang=self.lang, scope='UPLOADER')
                raise Exception(f'Collection already exists')

    def create_index(self) -> None:
        self.collection.create_index([('id', ASCENDING)], name='id_index', unique=True)
        logger.debug('Created id index', lang=self.lang, scope='Uploader')

    def upload_users(self, user_batch: list[dict]) -> None:
        self.collection.insert_many(user_batch)
        user_batch.clear()

    def upload_sex(self, user_updates: list[UpdateOne]) -> None:
        self.collection.bulk_write(user_updates)
        user_updates.clear()

    @staticmethod
    def get_available_langs(dbname: str) -> list[str]:
        connection = MongoClient()
        database = connection.get_database(dbname)
        db_collections: list[str] = database.list_collection_names()
        return [collection_name.split('wiki')[0] for collection_name in db_collections]

    
