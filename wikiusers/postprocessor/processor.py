from typing import Union
from pathlib import Path

from wikiusers import logger
from wikiusers.settings import DEFAULT_LANGUAGE, DEFAULT_DATABASE, DEFAULT_BATCH_SIZE, DEFAULT_FORCE
from wikiusers.postprocessor.utils import Batcher, Uploader, elaborate_users_batch, SexElaborator, uploader


class PostProcessor:

    def __init__(
        self,
        datasets_dir: Union[Path, str] = DEFAULT_DATABASE,
        database: str = DEFAULT_DATABASE,
        lang: str = DEFAULT_LANGUAGE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        force: bool = DEFAULT_FORCE
    ):
        self.datasets_dir = Path(datasets_dir)
        self.database = database
        self.lang = lang
        self.batch_size = batch_size
        self.force = force
        self.batcher = Batcher(self.database, self.lang, self.batch_size)
        self.uploader = Uploader(self.database, self.lang, self.force)
        self.sex_elaborator: Union[SexElaborator, None] = None

    def process_users(self) -> None:
        logger.info('Start postprocessing users', lang=self.lang, scope='POSTPROCESSOR')
        self.uploader.check_if_collection_already_exists()
        for i, user_batch in enumerate(self.batcher):
            logger.debug(f'Start processing batch {i}', lang=self.lang, scope='POSTPROCESSOR')
            processed_users = elaborate_users_batch(user_batch)
            logger.debug(f'Start uploading batch {i}', lang=self.lang, scope='POSTPROCESSOR')
            self.uploader.upload_users(processed_users)
            logger.debug(f'Finished batch {i}', lang=self.lang, scope='POSTPROCESSOR')
        logger.succ('Finished postprocessing users', lang=self.lang, scope='POSTPROCESSOR')

    def process_sex(self) -> None:
        logger.info('Start postprocessing sex', lang=self.lang, scope='POSTPROCESSOR')

        if not self.sex_elaborator:
            self.sex_elaborator = SexElaborator(self.datasets_dir, self.lang, self.batch_size)

        for i, user_updates in enumerate(self.sex_elaborator):
            logger.debug(f'Start uploading batch {i}', lang=self.lang, scope='POSTPROCESSOR')
            self.uploader.upload_sex(user_updates)
            logger.debug(f'Finished batch {i}', lang=self.lang, scope='POSTPROCESSOR')
        logger.succ('Finished postprocessing sex', lang=self.lang, scope='POSTPROCESSOR')
