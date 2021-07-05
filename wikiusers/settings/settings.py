from pathlib import Path
import multiprocessing

DEFAULT_DATASETS_DIR = Path.cwd().joinpath('datasets') if __name__ != '__main__' else Path(__file__).parent.parent.parent.joinpath('datasets')
DEFAULT_DATABASE_PREFIX = 'wikiusers'
DEFAULT_LANGUAGE = 'it'
DEFAULT_SYNC_DATA = True
DEFAULT_PARALLELIZE = True
DEFAULT_N_PROCESSES = multiprocessing.cpu_count()
DEFAULT_BATCH_SIZE = int(2e4)
DEFAULT_FORCE = False
DEFAULT_SKIP = False
DEFAULT_ERASE_DATASETS = False
DEFAULT_DROPOFF_MONTH_THRESHOLD = 12
DEFAULT_METRICS_DIR = Path.cwd().joinpath('metrics') if __name__ != '__main__' else Path(__file__).parent.parent.parent.joinpath('metrics')
DEFAULT_ACTIVE_PER_MONTH_THRESHOLD = 5