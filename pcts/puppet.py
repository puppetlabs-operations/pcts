import asyncio
import json
import logging
import re
import ssl

import aiohttp


def preview_compile(nodes):
    pass


class PuppetDB:
    def __init__(self, pdb_config):
        logger = logging.getLogger(__name__)
        self.query_uri = '{}/pdb/query/v4'.format(pdb_config['base_uri'])
        logger.debug('Querying against PuppetDB URI {}'.format(self.query_uri))
        self.ssl = {
            'host_key': pdb_config['ssl_host_key'],
            'host_cert': pdb_config['ssl_host_cert'],
            'ca_cert': pdb_config['ssl_ca_cert'],
        }
        logger.debug('Using host_key file {} for PuppetDB querying'.format(self.ssl['host_key']))
        logger.debug('Using host_cert file {} for PuppetDB querying'.format(self.ssl['host_cert']))
        logger.debug('Using ca_cert file {} for PuppetDB querying'.format(self.ssl['ca_cert']))

    @asyncio.coroutine
    def get_nodes_by_files(self, filenames, message_id):
        logger = logging.getLogger(__name__)
        logger.info('Querying PuppetDB for nodes affected by the pull request', extra={'MESSAGE_ID': message_id})

        files_partial = ' or '.join(
            ['(file ~ "^.*{}$")'.format(filename)
             for filename in filenames
             if re.search('\.pp$', filename)])

        query = ' '.join([
            'nodes [certname] {'
                'resources {',
                    files_partial,
                '}',
                'and deactivated is null',
                'and expired is null',
            '}'])

        logger.debug('Querying PuppetDB with PQL query: {}'.format(query))

        raw_nodes = yield from self.query(query)

        nodes = [node['certname'] for node in raw_nodes]

        logger.debug("\n".join(['Nodes affected by the change:'] + nodes), extra={'MESSAGE_ID': message_id})

        return nodes

    @asyncio.coroutine
    def query(self, query):
        sslcontext = ssl.create_default_context(cafile=self.ssl['ca_cert'])
        sslcontext.load_cert_chain(certfile=self.ssl['host_cert'], keyfile=self.ssl['host_key'])
        conn = aiohttp.TCPConnector(ssl_context=sslcontext, loop=asyncio.get_event_loop())
        session = aiohttp.ClientSession(connector=conn, loop=asyncio.get_event_loop())
        try:
            with aiohttp.Timeout(60):
                response = yield from session.get(self.query_uri, params={'query': query})
                try:
                    results = yield from response.text()
                    output = json.loads(results)
                except:
                    response.close()
                    raise
                finally:
                        yield from response.release()
        finally:
            session.close()
        return output
