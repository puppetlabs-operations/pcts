import pcts.http
import pcts.worker

import asyncio


def main():
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()

    srv = pcts.http.start_server(loop, queue)
    worker = asyncio.async(pcts.worker.worker(queue))

    loop.run_until_complete(pcts.http.stop_server(server=srv, queue=queue, worker=worker))
