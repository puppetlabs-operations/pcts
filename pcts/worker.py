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
    pass


@asyncio.coroutine
def worker(queue: asyncio.Queue):
    while True:
        message = yield from queue.get()
        handler_f = handlers.get(message['event'])
        if handler_f:
            yield from handler_f(message['body'])
