from pathlib import Path
import multiprocessing

DEFAULT_DATASETS_DIR = Path.cwd().joinpath('datasets') if __name__ != '__main__' else Path(__file__).parent.parent.parent.joinpath('datasets')
DEFAULT_DATABASE = 'wikiusers'
DEFAULT_LANGUAGE = 'it'
DEFAULT_SYNC_DATA = True
DEFAULT_PARALLELIZE = True
DEFAULT_N_PROCESSES = multiprocessing.cpu_count()
DEFAULT_BATCH_SIZE = int(1e5)
DEFAULT_FORCE = False
DEFAULT_SKIP = False
DEFAULT_ERASE_DATASETS = False