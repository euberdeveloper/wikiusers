from typing import Union
from pathlib import Path

from wikiusers import logger
from wikiusers.settings import DEFAULT_LANGUAGE, DEFAULT_DATABASE, DEFAULT_BATCH_SIZE, DEFAULT_FORCE
from wikiusers.postprocessor.utils import Batcher, Uploader, elaborate_users_batch, SexElaborator


class PostProcessor:

    def __get_uploader(self, lang: str) -> Uploader:
        uploader = Uploader(self.database, lang, self.force)
        uploader.check_if_collection_already_exists()
        uploader.create_index()
        return uploader

    def __init__(
        self,
        datasets_dir: Union[Path, str] = DEFAULT_DATABASE,
        database: str = DEFAULT_DATABASE,
        langs: Union[str, list[str]] = DEFAULT_LANGUAGE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        force: bool = DEFAULT_FORCE
    ):
        self.datasets_dir = Path(datasets_dir)
        self.database = database
        self.langs = [langs] if type(langs) == str else langs
        self.batch_size = batch_size
        self.force = force
        self.sex_elaborator: Union[SexElaborator, None] = None

    def process_users(self) -> None:
        for lang in self.langs:
            logger.info('Start postprocessing users', lang=lang, scope='POSTPROCESSOR')
            batcher = Batcher(self.database, lang, self.batch_size)
            uploader = self.__get_uploader(lang)
            for i, user_batch in enumerate(batcher):
                logger.debug(f'Start processing batch {i}', lang=lang, scope='POSTPROCESSOR')
                processed_users = elaborate_users_batch(user_batch)
                logger.debug(f'Start uploading batch {i}', lang=lang, scope='POSTPROCESSOR')
                uploader.upload_users(processed_users)
                logger.debug(f'Finished batch {i}', lang=lang, scope='POSTPROCESSOR')
            logger.succ('Finished postprocessing users', lang=lang, scope='POSTPROCESSOR')

    def process_sex(self) -> None:
        for lang in self.langs:
            logger.info('Start postprocessing sex', lang=lang, scope='POSTPROCESSOR')

            uploader = self.__get_uploader(lang)

            if not self.sex_elaborator:
                self.sex_elaborator = SexElaborator(self.datasets_dir, lang, self.batch_size)

            for i, user_updates in enumerate(self.sex_elaborator):
                logger.debug(f'Start uploading batch {i}', lang=lang, scope='POSTPROCESSOR')
                uploader.upload_sex(user_updates)
                logger.debug(f'Finished batch {i}', lang=lang, scope='POSTPROCESSOR')

            logger.succ('Finished postprocessing sex', lang=lang, scope='POSTPROCESSOR')

    @staticmethod
    def get_available_langs(dbname: str = DEFAULT_DATABASE) -> list[str]:
        return Uploader.get_available_langs(dbname)
