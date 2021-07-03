import requests
from typing import Union
from pathlib import Path
from whdtscraper import fetch_wikies, fetch_dumps, fetch_latest_version, WIKI_URL

from wikiusers import logger


class WhdtLoader:

    def __init__(self, datasets_dir: Union[str, Path], lang: str):
        datasets_dir = Path(datasets_dir)
        self.wiki_dir = datasets_dir.joinpath('whdt')
        self.lang = lang

    def __download(self, file_path: str, file_path_obj: Path, tsv_url: str) -> None:
        file_path_obj.parent.mkdir(exist_ok=True, parents=True)
        response = requests.get(tsv_url, allow_redirects=True)
        open(file_path_obj, 'wb').write(response.content)
        logger.debug(f'Downloaded {file_path}', lang=self.lang, scope='scraper')

    def __download_if_needed(self, file_path: str, size_in_bytes: int, tsv_url: str) -> None:
        logger.debug(f'Syncing {file_path}', lang=self.lang, scope='scraper')
        file_path_obj = self.wiki_dir.joinpath(file_path)

        if not file_path_obj.is_file():
            self.__download(file_path, file_path_obj, tsv_url)
        elif abs(file_path_obj.stat().st_size - size_in_bytes) > 50e6:
            logger.warn(f'{file_path} already exists but with unexpected size, redownloading',
                        lang=self.lang, scope='scraper')
            file_path_obj.unlink()
            self.__download(file_path, file_path_obj, tsv_url)
        else:
            logger.debug(f'{file_path} already exists', lang=self.lang, scope='scraper')

    def sync_wikies(self, /, version: str = 'latest'):
        logger.info('Syncing tsv files', lang=self.lang, scope='scraper')

        if (version == 'latest'):
            last_version_obj = fetch_latest_version()
            version = last_version_obj['version']

        wiki = f'{self.lang}wiki'
        dumps = fetch_dumps(version, wiki)

        for dump in dumps:
            url: str = dump['url']
            size_in_bytes: int = int(dump['bytes'])
            self.__download_if_needed(url.replace(f'{WIKI_URL}/', ''), size_in_bytes, url)

        logger.succ(f'Synced tsv files', lang=self.lang, scope='scraper')

    def get_tsv_files(self, /, version: str = 'latest') -> list[Path]:
        if version == 'latest':
            version_path = list(self.wiki_dir.glob('*'))[-1]
        else:
            version_path = self.wiki_dir.joinpath(version)

        wiki = f'{self.lang}wiki'
        wiki_path = version_path.joinpath(wiki)
        files = [file for file in wiki_path.iterdir() if file.is_file()]
        return sorted(files, key=lambda f: f.stem)

    def get_available_langs(self) -> list[str]:
        unfiltered_langs = sorted([wiki['wiki'].replace('wiki', '') for wiki in fetch_wikies('latest', wikitype='wiki')])
        return [lang for lang in unfiltered_langs if len(lang) == 2]

    def get_local_langs(self) -> list[str]:
        versions = sorted([dir.name for dir in self.wiki_dir.iterdir() if dir.is_dir()], reverse=True)
        if not versions:
            return []

        path = self.wiki_dir.joinpath(versions[0])
        return sorted([dir.name.replace('wiki', '') for dir in path.iterdir() if dir.is_dir()])
