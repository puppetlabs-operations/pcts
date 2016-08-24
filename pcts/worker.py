import pcts.github
import pcts.puppet

import asyncio
import configparser
import logging
import traceback


handlers = dict()


def handler(event_type):
    def dummy_decorator(func):
        handlers[event_type] = func
        return func
    return dummy_decorator


@handler('pull_request')
@asyncio.coroutine
def handle_pull_request(payload, id, config):
    logger = logging.getLogger('{}.worker'.format(__name__))
    logger.debug('Handling message {}'.format(id),
                 extra={'MESSAGE_ID': id})
    pr = pcts.github.PullRequest(payload=payload, auth_token=config['github']['auth_token'])
    pdb = pcts.puppet.PuppetDB(pdb_config=config['puppetdb'])

    pr.update_status(state='pending',
                     target_url='https://puppet.com/',
                     description='Testing of catalog compilation in progress',
                     message_id=id)
    affected_nodes = yield from pdb.get_nodes_by_files(filenames=pr.get_files(), message_id=id)
    report = pcts.puppet.preview_compile(nodes=affected_nodes)
    pr.update_status(state='success',
                     target_url='https://puppet.com/',
                     description='All catalogs compiled successfully',
                     message_id=id)


@asyncio.coroutine
def worker(queue: asyncio.JoinableQueue, config: configparser.ConfigParser):
    logger = logging.getLogger('{}.worker'.format(__name__))
    logger.info('Starting worker')
    while True:
        message = yield from queue.get()
        logger.info('Processing message {0} of event type "{1}" from queue'.format(message['id'], message['event']),
                    extra={'MESSAGE_ID': message['id']})
        # logger.debug('Message body: {}'.format(message['body']),
        #              extra={'MESSAGE_ID': message['id']})
        handler_f = handlers.get(message['event'])
        if handler_f:
            try:
                yield from handler_f(payload=message['body'], id=message['id'], config=config)
            except:
                logger.error('Error received when processing message {0}: {1}'.format(message['id'],
                                                                                      traceback.format_exc()),
                             extra={'MESSAGE_ID': message['id']})
        else:
            logger.info('No action to take on event type "{}"'.format(message['event']),
                        extra={'MESSAGE_ID': message['id']})
        queue.task_done()
