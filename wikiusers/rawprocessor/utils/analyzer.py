from pathlib import Path
from wikiusers.settings.settings import DEFAULT_DATABASE
from wikiusers.settings import DEFAULT_DATASETS_DIR, DEFAULT_N_PROCESSES, DEFAULT_LANGUAGE, DEFAULT_PARALLELIZE, DEFAULT_SYNC_DATA


class Analyzer:
    def __init_globals(self) -> None:
        self.user_exists = set()
        self.user_document = {}
        self.user_document_update = {}
        self.user_monh_events = {}
        self.user_alter_groups = {}
        self.user_alter_blocks = {}
        self.user_helper_info = {}

    def __init__(
        self,
        path: Path,
        lang: str,
        database
    ):
        self.path = path
        self.lang = lang
        self.database = database

        self.__init_globals()

    def analyze(self) -> None:
        pass
