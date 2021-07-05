from json import dumps, loads
from pathlib import Path
from typing import Union
from matplotlib import pyplot as plt

from wikiusers import settings
from wikiusers import logger

from .utils import Batcher, get_month_date_from_key


class AdminsHistory:

    def __process_user(self, user: dict) -> None:
        self.result[user['id']] = {'n0': {}, 'n1': {}, 'n2': {}, 'n3': {}}
        result = self.result[user['id']]

        try:
            first_event = user['activity']['total']['first_event_timestamp']
        except:
            first_event = None

        try:
            last_event = user['activity']['total']['last_event_timestamp']
        except:
            last_event = None

        per_month = user['activity']['per_month']

        for year, year_obj in per_month.items():
            for month, month_obj in year_obj.items():
                for ns in ['n0', 'n1', 'n2', 'n3']:
                    key = f'{year}-{month}'
                    try:
                        result[ns][key] = month_obj['events']['per_namespace'][ns]['total']
                    except:
                        result[ns][key] = 0

        groups_history = user['groups']['history']
        admin_history = [{'from': el['from'].isoformat() if el['from'] else first_event.isoformat(), 'to': el['to'].isoformat()
                          if el['to'] else last_event.isoformat()} for el in groups_history if 'sysop' in el['groups']]
        result['admin_history'] = admin_history

    def __init__(
        self,
        lang: str = settings.DEFAULT_LANGUAGE,
        database: str = settings.DEFAULT_DATABASE_PREFIX,
        batch_size: str = settings.DEFAULT_BATCH_SIZE,
        metrics_path: Union[str, Path] = settings.DEFAULT_METRICS_DIR
    ):
        self.lang = lang
        self.database = database
        self.batch_size = batch_size
        self.metrics_path = Path(metrics_path)

        query_filter = {'groups.ever_had': 'sysop'}
        self.batcher = Batcher(self.database, self.lang, self.batch_size, query_filter)
        self.result = {}

    def compute(self) -> None:
        logger.info(f'Start computing', lang=self.lang, scope=f'DEAD ADMINS HISTORY')
        for i, users_batch in enumerate(self.batcher):
            logger.debug(f'Computing batch {i}', lang=self.lang, scope=f'DEAD ADMINS HISTORY')
            for user in users_batch:
                self.__process_user(user)
        logger.succ(f'Finished computing', lang=self.lang, scope=f'DEAD ADMINS HISTORY')

    def save_json(self) -> None:
        logger.info(f'Start saving json', lang=self.lang, scope=f'DEAD ADMINS HISTORY')
        path_root = self.metrics_path.joinpath(self.lang).joinpath(f'admins_history')
        path_root.mkdir(exist_ok=True, parents=True)

        for id, user_data in self.result.items():
            json_text = dumps(user_data, sort_keys=True)
            path = path_root.joinpath(f'{id}.json')
            with open(path, 'w') as out_file:
                out_file.write(json_text)

        logger.succ(f'Finished saving json', lang=self.lang,
                    scope=f'DEAD ADMINS HISTORY')

    @staticmethod
    def show_graphs(
        lang: str = settings.DEFAULT_LANGUAGE,
        metrics_path: Union[str, Path] = settings.DEFAULT_METRICS_DIR
    ) -> None:
        metrics_path = Path(metrics_path)
        path_root = metrics_path.joinpath(lang).joinpath(f'admins_history')
        dest_root = metrics_path.joinpath(lang).joinpath(f'admins_history').joinpath('graphs')
        dest_root.mkdir(exist_ok=True)
        files = [file for file in path_root.iterdir() if file.is_file()]

        for file in files:
            name = file.stem
            with open(file) as input_file:
                raw_data = loads(input_file.read())

                plt.ylabel('Events count')
                for ns in ['n0', 'n1', 'n2', 'n3']:
                    ns_vals = raw_data[ns]
                    x_values = [get_month_date_from_key(key) for key in ns_vals.keys()]
                    y_values = ns_vals.values()
                    plt.plot(x_values, y_values, label=ns)
                    plt.title(f'Administrator {name}')

                for el in raw_data['admin_history']:
                    plt.axvspan(el['from'], el['to'], facecolor='#83ccef9c')

                plt.legend()
                plt.savefig(dest_root.joinpath(f'{name}.png'), bbox_inches='tight')
                plt.clf()
