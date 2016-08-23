import logging


def preview_compile(nodes):
    pass


class PuppetDB:
    def __init__(self, config):
        pass

    def get_nodes_by_files(self, filenames, message_id):
        logger = logging.getLogger(__name__)
        logger.debug('Querying PuppetDB for nodes affected by PR', extra={'MESSAGE_ID': message_id})

    def _filter_filenames(self, filenames):
        pass

    def _generalize_filename(self, filename):
        pass
