import bz2
from pymongo import UpdateOne
from pathlib import Path

from wikiusers import logger

class SexElaborator:

    def __elaborate_sex(self, line: str) -> UpdateOne:
        parts = line.split('\t')
        id = parts[0]
        str_sex = parts[2]
        try:
            sex = True if str_sex == 'male' else (False if str_sex == 'female' else None)
        except:
            logger.warn('Error in parsing sex', parts, lang=self.lang)
        return UpdateOne({'id': int(id)}, {'$set': {'sex': sex}})

    def __init__(self, datasets_dir: Path, lang: str, batch_size: int):
        self.datasets_dir = Path(datasets_dir)
        self.lang = lang
        self.batch_size = batch_size

        path = self.datasets_dir.joinpath('users_sex').joinpath(f'{self.lang}wiki.tsv.bz2')
        self.file = bz2.open(path, 'rt')

    def __iter__(self):
        return self

    def __next__(self):
        batch = []

        for _ in range(self.batch_size):
            if not self.file:
                break

            line = self.file.readline()
            if not line:
                break
            
            batch.append(self.__elaborate_sex(line))

        if (batch):
            return batch

        raise StopIteration
