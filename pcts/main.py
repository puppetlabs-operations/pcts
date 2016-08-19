import pcts.http
import pcts.worker

import argparse
import logging

import asyncio

import systemd.journal


def parse_args():
    loglevels = ['debug', 'info', 'warning', 'error', 'critical']
    loglevels = loglevels + [x.upper() for x in loglevels]
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--loglevel', type=str, choices=loglevels, default='WARN',
                        help='The minimum severity of log messages to output for the main process')
    parser.add_argument('--internal-loglevel', type=str, choices=loglevels, default='WARN',
                        help='The minimum severity of log messages to output for internal components')
    return parser.parse_args()


def configure_logging(args):
    logging.getLogger().addHandler(systemd.journal.JournalHandler(SYSLOG_IDENTIFIER='pcts'))

    print('log level main: {}'.format(args.loglevel.upper()))
    print('log level aiohttp: {}'.format(args.internal_loglevel.upper()))
    logging.getLogger(__name__.split('.')[0]).setLevel(getattr(logging, args.loglevel.upper()))
    logging.getLogger('aiohttp').setLevel(getattr(logging, args.internal_loglevel.upper()))


def main():
    args = parse_args()

    configure_logging(args)

    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()

    srv = pcts.http.start_server(loop, queue)
    worker = asyncio.async(pcts.worker.worker(queue))

    loop.run_until_complete(pcts.http.stop_server(server=srv, queue=queue, worker=worker))
