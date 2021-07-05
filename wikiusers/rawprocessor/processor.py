from pathlib import Path
from typing import Optional, Tuple, Union
from joblib import Parallel, delayed
from pymongo import MongoClient

from wikiusers import logger
from wikiusers import settings
from wikiusers.dataloader import WhdtLoader
from wikiusers.rawprocessor.utils import Analyzer


class RawProcessor:

    def __init_loader(self, lang: str) -> None:
        loader = WhdtLoader(self.datasets_dir, lang)
        if self.sync_data:
            loader.sync_wikies()
        return loader

    def __check_if_collection_already_exists(self, lang: str) -> bool:
        connection = MongoClient()
        database = connection.get_database(self.database)
        collection = database.get_collection(f'{lang}wiki_raw')

        db_collections = database.list_collection_names()
        if collection.name in db_collections:
            if self.force:
                logger.warn('Collection already exists: dropping',
                            lang=lang, scope='RAWPROCESSOR')
                collection.drop()
            elif self.skip:
                logger.warn('Collection already exists, skipping',
                           lang=lang, scope='RAWPROCESSOR')

                return True
            else:
                logger.warn('Collection already exists, it does not matter',
                           lang=lang, scope='RAWPROCESSOR')
        return False

    def __get_tsv_month_and_year(self, tsv_file_name: str) -> Tuple[Optional[int], Optional[int]]:
        time_str = tsv_file_name.split('.')[-3]
        parts = time_str.split('-')

        if len(parts) > 1:
            [year, month] = parts
        else:
            year = parts[0]

        try:
            month = int(month)
        except:
            month = None

        try:
            year = int(year)
        except:
            year = None

        return month, year

    def __init__(
        self,
        sync_data: bool = settings.DEFAULT_SYNC_DATA,
        datasets_dir: Union[Path, str] = settings.DEFAULT_DATASETS_DIR,
        langs: Union[str, list[str]] = settings.DEFAULT_LANGUAGE,
        parallelize: bool = settings.DEFAULT_PARALLELIZE,
        n_processes: int = settings.DEFAULT_N_PROCESSES,
        database: str = settings.DEFAULT_DATABASE_PREFIX,
        force: bool = settings.DEFAULT_FORCE,
        skip: bool = settings.DEFAULT_SKIP,
        erase_datasets: bool = settings.DEFAULT_ERASE_DATASETS
    ):
        self.sync_data = sync_data
        self.datasets_dir = Path(datasets_dir)
        self.langs = [langs] if type(langs) == str else langs
        self.parallelize = parallelize
        self.n_processes = n_processes
        self.database = f'{database}_raw'
        self.force = force
        self.skip = skip
        self.erase_datasets = erase_datasets

    def _process_file(self, lang: str, path: Path) -> None:
        logger.info(f'Starting processing {path}', lang=lang, scope='RAWPROCESSOR')
        month, year = self.__get_tsv_month_and_year(path.name)
        analyzer = Analyzer(path, month, year, lang, self.database)
        analyzer.analyze()
        logger.succ(f'Finished processing {path}', lang=lang, scope='RAWPROCESSOR')
        if self.erase_datasets:
            path.unlink()
            logger.info(f'Deleted {path}', lang=lang, scope='RAWPROCESSOR')

    def process(self) -> None:
        for lang in self.langs:
            if self.__check_if_collection_already_exists(lang):
                continue

            loader = self.__init_loader(lang)
            datasets_paths = loader.get_tsv_files()

            if self.parallelize:
                Parallel(n_jobs=self.n_processes)(
                    delayed(self._process_file)(lang, path)
                    for path in datasets_paths
                )
            else:
                for path in datasets_paths:
                    self._process_file(lang, path)
