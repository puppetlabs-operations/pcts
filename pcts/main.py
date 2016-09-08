import pcts.http
import pcts.worker

import argparse
import configparser
import logging
import subprocess

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
    parser.add_argument('-c', '--config', type=str, default='/etc/pcts.conf',
                        help='The configuration file to load from')
    parser.add_argument('-p', '--puppet', type=str, default='/opt/puppetlabs/bin/puppet',
                        help='The full path to the `puppet` executable')
    return parser.parse_args()


def get_config(filename, puppet):
    """Configuration

    {
      'puppetdb': {
        'base_uri': '', # first `server_urls` value from puppetdb.conf
        'ssl': {
          'host_key': `puppet config print hostprivkey`,
          'host_cert': `puppet config print hostcert`,
          'ca_cert': `puppet config print localcacert`,
        },
      },
      'elasticsearch': {
        'host': 'localhost',
        'port': 9200,
        'index': 'pcts',
      },
      'github': {
        'auth_token': '',
      },
      'preview': {
        'excludes_file': '',
      }
    }

    """
    logger = logging.getLogger(__name__)
    puppet_confdir = subprocess.check_output([puppet, 'config', 'print', 'confdir'], universal_newlines=True).rstrip()
    puppetdb_conf_file = '{}/puppetdb.conf'.format(puppet_confdir)
    logger.debug('Loading default PuppetDB config from {}'.format(puppetdb_conf_file))
    with open(puppetdb_conf_file) as f:
        puppetdb_conf = configparser.ConfigParser()
        puppetdb_conf.read_file(f)
        puppetdb_default_uri = puppetdb_conf['main']['server_urls'].split(',')[0]
    default_dict = {
        'puppetdb': {
            'base_uri': puppetdb_default_uri,
            'ssl_host_key': subprocess.check_output([puppet, 'config', 'print', 'hostprivkey'], universal_newlines=True).rstrip(),
            'ssl_host_cert': subprocess.check_output([puppet, 'config', 'print', 'hostcert'], universal_newlines=True).rstrip(),
            'ssl_ca_cert': subprocess.check_output([puppet, 'config', 'print', 'localcacert'], universal_newlines=True).rstrip(),
        },
        'elasticsearch': {
            'host': 'localhost',
            'port': 9200,
            'index': 'pcts',
        },
        'github': {
            'auth_token': '',
        },
        'executables': {
            'puppet': 'puppet',
            'armature': '/opt/puppetlabs/puppet/bin/armature',
        },
        'preview': {
            'excludes_file': '',
        }
    }
    config = configparser.ConfigParser()
    config.read_dict(default_dict)
    logger.info('Loading configuration settings from {}'.format(filename))
    config.read(filename)
    return config


def configure_logging(args):
    logging.getLogger().addHandler(systemd.journal.JournalHandler(SYSLOG_IDENTIFIER='pcts'))

    print('log level main: {}'.format(args.loglevel.upper()))
    print('log level aiohttp: {}'.format(args.internal_loglevel.upper()))
    logging.getLogger(__name__.split('.')[0]).setLevel(getattr(logging, args.loglevel.upper()))
    logging.getLogger('aiohttp').setLevel(getattr(logging, args.internal_loglevel.upper()))


def main():
    args = parse_args()

    configure_logging(args)

    logger = logging.getLogger(__name__)

    config = get_config(filename=args.config, puppet=args.puppet)

    loop = asyncio.get_event_loop()
    queue = asyncio.JoinableQueue()

    srv = pcts.http.start_server(loop, queue)
    worker = asyncio.async(pcts.worker.worker(queue=queue, config=config))

    loop.run_until_complete(pcts.http.stop_server(server=srv, queue=queue, worker=worker))
    logger.info('Service shut down due to no activity.')
