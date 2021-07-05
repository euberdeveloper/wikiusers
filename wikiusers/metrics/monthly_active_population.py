from datetime import datetime
from json import dumps
from pathlib import Path
from typing import Union

from wikiusers import settings
from wikiusers import logger

from .utils import Batcher, get_no_ghost


class MonthlyActivePopulation:

    def __process_user(self, user: dict) -> None:
        per_month = user['activity']['per_month']
        for year, year_obj in per_month.items():
            for month, month_obj in year_obj.items():
                if month_obj['events']['total']['total'] >= self.active_per_month_thr:
                    key = f'{year}-{month}'
                    try:
                        self.result[key] += 1
                    except:
                        self.result[key] = 1

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
        active_per_month_thr: int = settings.DEFAULT_ACTIVE_PER_MONTH_THRESHOLD
    ):
        self.lang = lang
        self.database = database
        self.batch_size = batch_size
        self.metrics_path = Path(metrics_path)
        self.active_per_month_thr = active_per_month_thr

        query_filter = {**get_no_ghost()}
        self.batcher = Batcher(self.database, self.lang, self.batch_size, query_filter)
        self.result = {}
        self.result_incremental = {}

    def compute(self) -> None:
        logger.info(f'Start computing', lang=self.lang, scope=f'MONTH INC ACTIVE POP {self.active_per_month_thr}')
        for i, users_batch in enumerate(self.batcher):
            logger.debug(f'Computing batch {i}', lang=self.lang, scope=f'MONTH INC ACTIVE POP {self.active_per_month_thr}')
            for user in users_batch:
                self.__process_user(user)
        self.__get_incremental()
        logger.succ(f'Finished computing', lang=self.lang, scope=f'MONTH INC ACTIVE POP {self.active_per_month_thr}')

    def save_json(self) -> None:
        logger.info(f'Start saving json', lang=self.lang, scope=f'MONTH INC ACTIVE POP {self.active_per_month_thr}')
        path = self.metrics_path.joinpath(self.lang).joinpath(f'monthly_active_population_{self.active_per_month_thr}.json')
        path_incremental = self.metrics_path.joinpath(self.lang).joinpath(f'monthly_active_population_incremental_{self.active_per_month_thr}.json')
        path.parent.mkdir(exist_ok=True, parents=True)
        json_text = dumps(self.result, sort_keys=True)
        json_text_incremental = dumps(self.result_incremental, sort_keys=True)
        with open(path, 'w') as out_file:
            out_file.write(json_text)
        with open(path_incremental, 'w') as out_file:
            out_file.write(json_text_incremental)
        logger.succ(f'Finished saving json', lang=self.lang,
                    scope=f'MONTH INC ACTIVE POP {self.active_per_month_thr}')
