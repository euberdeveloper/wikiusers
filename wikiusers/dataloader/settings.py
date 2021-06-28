from pathlib import Path

DEFAULT_ASSETS_DIR = Path.cwd().joinpath('datasets') if __name__ != '__main__' else Path(__file__).parent.parent.parent.joinpath('datasets')
