import asyncio
import json
import http.server
import socket

import aiohttp
import aiohttp.protocol
import aiohttp.server
import aiohttp.streams
import aiohttp.web


def get_systemd_socket():
    """Shows how to get the socket"""
    try:
        from systemd.daemon import listen_fds;
        fds = listen_fds()
    except ImportError:
        fds = [3]

    SYSTEMD_FIRST_SOCKET_FD = fds[0]
    socket_type = http.server.HTTPServer.socket_type
    address_family = http.server.HTTPServer.address_family
    return socket.fromfd(SYSTEMD_FIRST_SOCKET_FD, address_family, socket_type)


class HttpRequestHandler(aiohttp.server.ServerHttpProtocol):
    work_queue = None

    @asyncio.coroutine
    def handle_request(self, message: aiohttp.protocol.HttpRequestParser,
                             payload: aiohttp.streams.FlowControlStreamReader):
        try:
            queue_message = {
                'event': message.headers.get('X-GitHub-Event'),
                'id': message.headers.get('X-GitHub-Delivery'),
                'body': json.loads(payload.read()),
            }
            self.work_queue.put(queue_message)
            response = aiohttp.web.Response(status=200, text='ok')
        except ValueError as e:
            response = aiohttp.web.Response(status=400, text=str(e))
        except:
            response = aiohttp.web.Response(status=500, text='Server error')
        yield from response.write_eof()


def start_server(event_loop: asyncio.BaseEventLoop, work_queue: asyncio.Queue):
    def request_handler():
        handler = HttpRequestHandler(allowed_messages=['POST'], keep_alive_on=False)
        handler.work_queue = work_queue
        return handler
    f = event_loop.create_server(request_handler, sock=get_systemd_socket())
    srv = event_loop.run_until_complete(f)

