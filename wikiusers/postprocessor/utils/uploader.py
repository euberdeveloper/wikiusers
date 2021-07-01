from pymongo import MongoClient


class Uploader:

    def __init_connection(self) -> None:
        self.connection = MongoClient()
        self.database = self.connection.get_database(self.database)
        self.collection = self.database.get_collection(f'{self.lang}wiki')

    def __init__(
        self,
        database: str,
        lang: str
    ):
        self.database = database
        self.lang = lang
        self.__init_connection()

    def upload_users(self, user_batch: list[dict]) -> None:
        self.collection.insert_many(user_batch)