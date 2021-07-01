# from wikiusers.rawprocessor import RawProcessor


# def run():
#     rawprocessor = RawProcessor(lang='it')
#     rawprocessor.process()

from wikiusers.postprocessor import PostProcessor

def run():
    postprocessor = PostProcessor(lang='ca', force=True)
    postprocessor.process()
