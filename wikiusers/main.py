#!/usr/bin/env python
import click

from wikiusers import settings
from wikiusers.dataloader import WhdtLoader
from wikiusers.rawprocessor import RawProcessor, processor
from wikiusers.postprocessor import PostProcessor

@click.group(help="Tool to analyze the wikimedia history dump tsv and obtain per-user information")
def cli():
    pass

@cli.command(help='Downloads the assets if needed and saves per-user raw information on mongodb')
@click.option('-s', '--sync-data', type=click.BOOL, default=settings.DEFAULT_SYNC_DATA, show_default=True, help='If the dataset will be synced before the processing (by downloading missing datasets or newest version)')
@click.option('-i', '--datasets-dir', type=click.STRING, default=settings.DEFAULT_DATASETS_DIR, show_default=True, help='The path to the datasets folder')
@click.option('-l', '--lang', type=click.STRING, default=settings.DEFAULT_LANGUAGE, show_default=True, help='The language that you want to process')
@click.option('-p', '--parallelize/--no-parallelize', is_flag=True, default=settings.DEFAULT_PARALLELIZE, show_default=True, help='If processing the various datasets files in parallel')
@click.option('-n', '--n-processes', type=click.INT, default=settings.DEFAULT_N_PROCESSES, show_default=True, help='If parallelize is active, specifies the number of parallel processes. Default is the number of cores of the CPU')
@click.option('-d', '--dbname', type=click.STRING, default=settings.DEFAULT_DATABASE, show_default=True, help='The name of the MongoDB database where the result will be saved')
@click.option('-f', '--force/--no-force', is_flag=True, default=settings.DEFAULT_FORCE, show_default=True, help='If already populated collections will be dropped and overriden')
def rawprocess(*, sync_data: bool, datasets_dir: str, lang: str, parallelize: bool, n_processes: int, dbname: str, force: bool):
    rawprocessor = RawProcessor(sync_data, datasets_dir, lang, parallelize, n_processes, dbname, force)
    rawprocessor.process()

@cli.group(help="Given an already populated raw collection, postprocesses it, creating/updating a new collection")
def postprocess():
    pass

@postprocess.command(help='Postprocesses the users of the raw collection')
@click.option('-d', '--dbname', type=click.STRING, default=settings.DEFAULT_DATABASE, show_default=True, help='The name of the MongoDB database where the result will be saved')
@click.option('-l', '--lang', type=click.STRING, default=settings.DEFAULT_LANGUAGE, show_default=True, help='The language that you want to process')
@click.option('-b', '--batch-size', type=click.INT, default=settings.DEFAULT_BATCH_SIZE, show_default=True, help='Users from mongodb are taken and updated in batches. This option specifies the batch size')
@click.option('-f', '--force/--no-force', is_flag=True, default=settings.DEFAULT_FORCE, show_default=True, help='If already populated collections will be dropped and overriden')
def users(*, dbname: str, lang: str, batch_size: int, force: bool):
    postprocessor = PostProcessor(settings.DEFAULT_DATASETS_DIR, dbname, lang, batch_size, force)
    postprocessor.process_users()

@postprocess.command(help='Postprocesses the users of the raw collection')
@click.option('-i', '--datasets-dir', type=click.STRING, default=settings.DEFAULT_DATASETS_DIR, show_default=True, help='The path to the datasets folder')
@click.option('-d', '--dbname', type=click.STRING, default=settings.DEFAULT_DATABASE, show_default=True, help='The name of the MongoDB database where the result will be saved')
@click.option('-l', '--lang', type=click.STRING, default=settings.DEFAULT_LANGUAGE, show_default=True, help='The language that you want to process')
@click.option('-b', '--batch-size', type=click.INT, default=settings.DEFAULT_BATCH_SIZE, show_default=True, help='Users from mongodb are taken and updated in batches. This option specifies the batch size')
def sex(*, datasets_dir: str, dbname: str, lang: str, batch_size: int):
    postprocessor = PostProcessor(datasets_dir, dbname, lang, batch_size, settings.DEFAULT_FORCE)
    postprocessor.process_sex()


@cli.group(help="Handles the datasets")
def datasets():
    pass


@datasets.command(help='Synces the datasets, by downloading the missing datasets')
@click.option('-i', '--datasets-dir', type=click.STRING, default=settings.DEFAULT_DATASETS_DIR, show_default=True, help='The path to the datasets folder')
@click.option('-l', '--lang', type=click.STRING, default=settings.DEFAULT_LANGUAGE, show_default=True, help='The language that you want to process')
def sync(*, datasets_dir: str, lang: str):
    loader = WhdtLoader(datasets_dir, lang)
    loader.sync_wikies()

@datasets.command(help='Show available langs for the whdt datasets')
@click.option('-i', '--datasets-dir', type=click.STRING, default=settings.DEFAULT_DATASETS_DIR, show_default=True, help='The path to the datasets folder')
def list(*, datasets_dir: str):
    loader = WhdtLoader(datasets_dir, settings.DEFAULT_LANGUAGE)
    langs = loader.get_available_langs()
    langs_list = "\n".join(langs)
    click.echo(click.style('Downloadable datasets are available in:', fg='yellow', bold=True))
    click.echo(click.style(f'{langs_list}', fg='blue', bold=True))

@datasets.command(help='Show downloaded langs for the whdt datasets')
@click.option('-i', '--datasets-dir', type=click.STRING, default=settings.DEFAULT_DATASETS_DIR, show_default=True, help='The path to the datasets folder')
def local(*, datasets_dir: str):
    loader = WhdtLoader(datasets_dir, settings.DEFAULT_LANGUAGE)
    langs = loader.get_local_langs()
    langs_list = "\n".join(langs)
    click.echo(click.style('Downloaded datasets are available in:', fg='yellow', bold=True))
    click.echo(click.style(f'{langs_list}', fg='blue', bold=True))


def run():
    cli()

if __name__ == '__main__':
    run()
