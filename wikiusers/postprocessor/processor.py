from wikiusers import logger
from wikiusers.settings import DEFAULT_LANGUAGE, DEFAULT_DATABASE, DEFAULT_BATCH_SIZE, DEFAULT_FORCE
from wikiusers.postprocessor.utils import Batcher, Uploader, elaborate_users_batch, uploader


class PostProcessor:

    def __init__(
        self,
        database: str = DEFAULT_DATABASE,
        lang: str = DEFAULT_LANGUAGE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        force: bool = DEFAULT_FORCE
    ):
        self.database = database
        self.lang = lang
        self.batch_size = batch_size
        self.force = force
        self.batcher = Batcher(self.database, self.lang, self.batch_size)
        self.uploader = Uploader(self.database, self.lang, self.force)

    def process(self) -> None:
        logger.info('Start postprocessing users', lang=self.lang, scope='POSTPROCESSOR')
        for i, user_batch in enumerate(self.batcher):
            logger.debug(f'Start processing batch {i}', lang=self.lang, scope='POSTPROCESSOR')
            processed_users = elaborate_users_batch(user_batch)
            logger.debug(f'Start uploading batch {i}', lang=self.lang, scope='POSTPROCESSOR')
            self.uploader.upload_users(processed_users)
            logger.debug(f'Finished batch {i}', lang=self.lang, scope='POSTPROCESSOR')
        logger.succ('Finished postprocessing users', lang=self.lang, scope='POSTPROCESSOR')
