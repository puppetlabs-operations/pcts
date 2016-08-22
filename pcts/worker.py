import asyncio
import configparser
import logging


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


@asyncio.coroutine
def worker(queue: asyncio.Queue, config: configparser.ConfigParser):
    logger = logging.getLogger('{}.worker'.format(__name__))
    logger.info('Starting worker')
    while True:
        message = yield from queue.get()
        logger.info('Processing message for event type "{}" from queue'.format(message['event']),
                    extra={'MESSAGE_ID': message['id']})
        logger.debug('Message body: {}'.format(message['body']),
                     extra={'MESSAGE_ID': message['id']})
        handler_f = handlers.get(message['event'])
        if handler_f:
            yield from handler_f(payload=message['body'], id=message['id'], config=config)
        else:
            logger.info('No action to take on event type "{}"'.format(message['event']),
                        extra={'MESSAGE_ID': message['id']})
