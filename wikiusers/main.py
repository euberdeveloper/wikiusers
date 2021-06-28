from wikiusers.dataloader.whdtloader import WhdtLoader


def run():
    loader = WhdtLoader(lang='ca')
    loader.sync_wikies()
