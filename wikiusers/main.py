from wikiusers.dataloader import WhdtLoader

def run():
    loader = WhdtLoader(lang='ca')
    loader.sync_wikies()
