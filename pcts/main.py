import pcts.http
import pcts.worker

import asyncio


def main():
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()

    pcts.http.start_server(loop, queue)
    asyncio.async(pcts.worker.worker(queue))

    loop.run_forever()

