import asyncio


handlers = dict()


def handler(event_type):
    def dummy_decorator(func):
        handlers[event_type] = func
        return func
    return dummy_decorator


@handler('pull_request')
@asyncio.coroutine
def handle_pull_request(payload):
    print('Handling message {}'.format(payload['id']))


@asyncio.coroutine
def worker(queue: asyncio.Queue):
    print('starting worker')
    while True:
        message = yield from queue.get()
        print('Found message {}'.format(message['id']))
        handler_f = handlers.get(message['event'])
        if handler_f:
            yield from handler_f(message['body'])
