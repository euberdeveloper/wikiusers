from datetime import datetime
from json import dumps
from pathlib import Path
from typing import Union

from wikiusers import settings
from wikiusers import logger

from .utils import Batcher, get_today_month_date, get_month_date_from_key, get_diff_in_months, get_no_ghost, get_key_from_date


class MonthlyDropoff:

    def __process_user(self, user: dict) -> None:
        last_event: datetime = user['activity']['total']['last_event_timestamp']
        year_month = get_key_from_date(last_event)

        try:
            self.result[year_month] += 1
        except:
            self.result[year_month] = 1

    def __filter_for_threshold(self) -> None:
        today_month_date = get_today_month_date()

        for key in list(self.result.keys()):
            key_month_date = get_month_date_from_key(key)
            if get_diff_in_months(today_month_date, key_month_date) <= self.dropoff_month_threshold:
                self.result.pop(key)

    def __init__(
        self,
        lang: str = settings.DEFAULT_LANGUAGE,
        database: str = settings.DEFAULT_DATABASE_PREFIX,
        batch_size: str = settings.DEFAULT_BATCH_SIZE,
        metrics_path: Union[str, Path] = settings.DEFAULT_METRICS_DIR,
        dropoff_month_threshold: int = settings.DEFAULT_DROPOFF_MONTH_THRESHOLD
    ):
        self.lang = lang
        self.database = database
        self.batch_size = batch_size
        self.metrics_path = Path(metrics_path)
        self.dropoff_month_threshold = dropoff_month_threshold

        query_filter = {**get_no_ghost()}
        self.batcher = Batcher(self.database, self.lang, self.batch_size, query_filter)
        self.result = {}

    def compute(self) -> None:
        logger.info(f'Start computing', lang=self.lang, scope=f'MONTHLY DROPOFF THR {self.dropoff_month_threshold}')
        for i, users_batch in enumerate(self.batcher):
            logger.debug(f'Computing batch {i}', lang=self.lang, scope=f'MONTHLY DROPOFF THR {self.dropoff_month_threshold}')
            for user in users_batch:
                self.__process_user(user)
        self.__filter_for_threshold()
        logger.succ(f'Finished computing', lang=self.lang, scope=f'MONTHLY DROPOFF THR {self.dropoff_month_threshold}')

    def save_json(self) -> None:
        logger.info(f'Start saving json', lang=self.lang, scope=f'MONTHLY DROPOFF THR {self.dropoff_month_threshold}')
        path = self.metrics_path.joinpath(self.lang).joinpath(f'monthly_dropoff_with_threshold_{self.dropoff_month_threshold}.json')
        path.parent.mkdir(exist_ok=True, parents=True)
        json_text = dumps(self.result, sort_keys=True)
        with open(path, 'w') as out_file:
            out_file.write(json_text)
        logger.succ(f'Finished saving json', lang=self.lang,
                    scope=f'MONTHLY DROPOFF THR {self.dropoff_month_threshold}')
