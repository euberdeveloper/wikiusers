from pymongo import MongoClient


class Batcher:

    def __init_connection(self) -> None:
        self.connection = MongoClient()
        self.database = self.connection.get_database(self.database)
        self.collection = self.database.get_collection(f'{self.lang}wiki_raw')

    def __init__(
        self,
        database: str,
        lang: str,
        batch_size: int
    ):
        self.database = database
        self.lang = lang
        self.batch_size = batch_size
        self.__init_connection()
        self.batch_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        data = list(
            self.collection.find({})
                .skip(self.batch_size * self.batch_index)
                .limit(self.batch_size)
        )

        if (data):
            self.batch_index += 1
            return data

        raise StopIteration
