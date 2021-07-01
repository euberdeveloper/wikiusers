from typing import Optional, Tuple
from joblib import Parallel, delayed

from wikiusers import logger
from wikiusers.settings import DEFAULT_LANGUAGE, DEFAULT_DATABASE, DEFAULT_BATCH_SIZE


class PostProcessor:

    def __init__(
        self,
        database: str = DEFAULT_DATABASE,
        lang: str = DEFAULT_LANGUAGE,
        batch_size: int = DEFAULT_BATCH_SIZE
    ):
        self.database = database
        self.lang = lang
        self.batch_size = batch_size

    def process(self) -> None:
        pass
