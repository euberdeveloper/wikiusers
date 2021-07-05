from json import dumps
from pathlib import Path
from typing import Union

from wikiusers import settings
from wikiusers import logger

from .utils import Batcher, get_key_from_date


class MonthlyTotalPopulation:

    def __process_user(self, user: dict) -> None:
        existence_timestamp = user['creation_timestamp']

        if existence_timestamp is None:
            existence_timestamp = user['registration_timestamp']

        if existence_timestamp is None:
            existence_timestamp = user['activity']['total']['first_event_timestamp']

        if existence_timestamp is not None:
            year_month = get_key_from_date(existence_timestamp)
            try:
                self.result[year_month] += 1
            except:
                self.result[year_month] = 1

    def __get_incremental(self) -> None:
        total = 0
        for key in sorted(list(self.result.keys())):
            total += self.result[key]
            self.result_incremental[key] = total

    def __init__(
        self,
        lang: str = settings.DEFAULT_LANGUAGE,
        database: str = settings.DEFAULT_DATABASE_PREFIX,
        batch_size: str = settings.DEFAULT_BATCH_SIZE,
        metrics_path: Union[str, Path] = settings.DEFAULT_METRICS_DIR,
    ):
        self.lang = lang
        self.database = database
        self.batch_size = batch_size
        self.metrics_path = Path(metrics_path)

        self.batcher = Batcher(self.database, self.lang, self.batch_size)
        self.result = {}
        self.result_incremental = {}

    def compute(self) -> None:
        logger.info(f'Start computing', lang=self.lang, scope='MONTH INC TOT POP')
        for i, users_batch in enumerate(self.batcher):
            logger.debug(f'Computing batch {i}', lang=self.lang, scope='MONTH INC TOT POP')
            for user in users_batch:
                self.__process_user(user)
        self.__get_incremental()
        logger.succ(f'Finished computing', lang=self.lang, scope='MONTH INC TOT POP')

    def save_json(self) -> None:
        logger.info(f'Start saving json', lang=self.lang, scope='MONTH INC TOT POP')
        path = self.metrics_path.joinpath(self.lang).joinpath('monthly_total_population.json')
        path_incremental = self.metrics_path.joinpath(self.lang).joinpath('monthly_total_population_incremental.json')
        path.parent.mkdir(exist_ok=True, parents=True)
        json_text = dumps(self.result, sort_keys=True)
        json_text_incremental = dumps(self.result_incremental, sort_keys=True)
        with open(path, 'w') as out_file:
            out_file.write(json_text)
        with open(path_incremental, 'w') as out_file:
            out_file.write(json_text_incremental)
        logger.succ(f'Finished saving json', lang=self.lang,
                    scope='MONTH INC TOT POP')
