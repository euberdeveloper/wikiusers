from pathlib import Path
from wikiusers.settings.settings import DEFAULT_DATABASE
from wikiusers.settings import DEFAULT_DATASETS_DIR, DEFAULT_N_PROCESSES, DEFAULT_LANGUAGE, DEFAULT_PARALLELIZE, DEFAULT_SYNC_DATA


class RawProcessor:
    def __init__(
        self,
        sync_data: bool = DEFAULT_SYNC_DATA,
        datasets_dir: Path = DEFAULT_DATASETS_DIR,
        lang: str = DEFAULT_LANGUAGE,
        parallelize: bool = DEFAULT_PARALLELIZE,
        n_processes: int = DEFAULT_N_PROCESSES,
        database: str = DEFAULT_DATABASE
    ):
        self.sync_data = sync_data
        self.datasets_dir = datasets_dir
        self.lang = lang
        self.parallelize = parallelize
        self.n_processes = n_processes
        self.database = database
