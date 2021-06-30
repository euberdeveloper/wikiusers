from wikiusers.rawprocessor import RawProcessor


def run():
    rawprocessor = RawProcessor(lang='it')
    rawprocessor.process()