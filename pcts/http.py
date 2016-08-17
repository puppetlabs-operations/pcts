import asyncio
import json
import http.server
import socket

import aiohttp
import aiohttp.protocol
import aiohttp.server
import aiohttp.streams
import aiohttp.web


def get_systemd_socket() -> socket.socket:
    """Shows how to get the socket"""
    print('Finding systemd socket')
    try:
        from systemd.daemon import listen_fds;
        fds = listen_fds()
    except ImportError:
        fds = [3]

    SYSTEMD_FIRST_SOCKET_FD = fds[0]
    print('Using socket id {}'.format(SYSTEMD_FIRST_SOCKET_FD))
    socket_type = http.server.HTTPServer.socket_type
    address_family = http.server.HTTPServer.address_family
    return socket.fromfd(SYSTEMD_FIRST_SOCKET_FD, address_family, socket_type)


def handle_github_request(work_queue: asyncio.Queue):
    @asyncio.coroutine
    def request_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
        print('Handling request {}'.format(request.headers.get('X-GitHub-Delivery')))
        try:
            body = yield from request.text()
            queue_message = {
                'event': request.headers.get('X-GitHub-Event'),
                'id': request.headers.get('X-GitHub-Delivery'),
                'body': json.loads(body),
            }
            yield from work_queue.put(queue_message)
            response = aiohttp.web.Response(status=200, text='ok')
        except ValueError as e:
            response = aiohttp.web.Response(status=400, text=str(e))
        except BaseException as e:
            response = aiohttp.web.Response(status=500, text='Server error: {}'.format(str(e)))
        return response

    return request_handler


def start_server(event_loop: asyncio.BaseEventLoop, work_queue: asyncio.Queue) -> asyncio.base_events.Server:
    print('Attempting to start server')
    socket = get_systemd_socket()
    app = aiohttp.web.Application(loop=event_loop)
    app.router.add_route('*', '/{tail:.*}', handle_github_request(work_queue=work_queue))
    f = event_loop.create_server(app.make_handler(), sock=socket)
    return event_loop.run_until_complete(f)

@asyncio.coroutine
def stop_server(server: asyncio.base_events.Server, queue: asyncio.Queue, worker: asyncio.Task):
    while True:
        yield from asyncio.sleep(5)
        if queue.empty():
            print('queue is empty, checking again soon')
            yield from asyncio.sleep(5)
            if queue.empty():
                print('queue is still empty. time to shut down.')
                break
    print('going to stop server')
    server.close()
    print('server stopped')
    print('going to stop worker')
    worker.cancel()
    print('worker stopped')
