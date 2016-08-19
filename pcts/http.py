import asyncio
import json
import http.server
import logging
import socket
import uuid

import aiohttp
import aiohttp.protocol
import aiohttp.server
import aiohttp.streams
import aiohttp.web

import systemd.daemon


def get_systemd_socket() -> socket.socket:
    """Shows how to get the socket"""
    logger = logging.getLogger(__name__)

    logger.debug('Attempting to discover systemd-provided socket')

    SYSTEMD_FIRST_SOCKET_FD = systemd.daemon.listen_fds()[0]

    logger.debug('Using socket id {}'.format(SYSTEMD_FIRST_SOCKET_FD))
    socket_type = http.server.HTTPServer.socket_type
    address_family = http.server.HTTPServer.address_family
    return socket.fromfd(SYSTEMD_FIRST_SOCKET_FD, address_family, socket_type)


def handle_github_request(work_queue: asyncio.Queue):
    @asyncio.coroutine
    def request_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
        logger = logging.getLogger(__name__)

        try:
            raw_message_id = request.headers.get('X-GitHub-Event')

            try:
                message_id = uuid.UUID(raw_message_id)
            except ValueError:
                if raw_message_id:
                    message_id = raw_message_id
                else:
                    message_id = uuid.uuid4()

            event_type = request.headers.get('X-GitHub-Event')

            logger.info('Received webhook for GitHub "{0}" event with id "{1}"'.format(event_type, message_id),
                         extra={'MESSAGE_ID': message_id})
            raw_body = yield from request.text()
            logger.debug('Webhook body: {}'.format(raw_body),
                        extra={'MESSAGE_ID': message_id})
            queue_message = {
                'event': event_type,
                'id': message_id,
                'body': json.loads(raw_body),
            }
            yield from work_queue.put(queue_message)
            response = aiohttp.web.Response(status=200, text='ok')
            logger.debug('Sending response code 200')
        except ValueError as e:
            logger.error('Invalid JSON in request, caught error: {}'.format(e), extra={'MESSAGE_ID': message_id})
            response = aiohttp.web.Response(status=400, text=str(e))
        except BaseException as e:
            response = aiohttp.web.Response(status=500, text='Server error: {}'.format(e))
            logger.error('Caught unexpected error: {}'.format(e), extra={'MESSAGE_ID': message_id})
        return response

    return request_handler


def start_server(event_loop: asyncio.BaseEventLoop, work_queue: asyncio.Queue) -> asyncio.base_events.Server:
    logger = logging.getLogger(__name__)

    socket = get_systemd_socket()

    logger.info('Starting HTTP server')

    app = aiohttp.web.Application(loop=event_loop)
    app.router.add_route('*', '/{tail:.*}', handle_github_request(work_queue=work_queue))
    f = event_loop.create_server(app.make_handler(), sock=socket)
    return event_loop.run_until_complete(f)

@asyncio.coroutine
def stop_server(server: asyncio.base_events.Server, queue: asyncio.Queue, worker: asyncio.Task):
    logger = logging.getLogger(__name__)

    while True:
        yield from asyncio.sleep(5)
        logger.debug('Attempting to shut down. Checking if work queue is empty.')
        if queue.empty():
            logger.debug('Work queue is empty. Checking again in 5 seconds.')
            yield from asyncio.sleep(5)
            if queue.empty():
                logger.debug('Work queue is still empty.')
                logger.info('Preparing to shut down.')
                break
    logger.info('Attempting to stop HTTP server.')
    server.close()
    logger.info('Stopped HTTP server.')
    logger.info('Attempting to stop worker.')
    worker.cancel()
    logger.info('Stopped worker.')
