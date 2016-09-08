import pcts.github

import asyncio
import json
import logging
import re
import ssl
import subprocess

import aiohttp


@asyncio.coroutine
def preview_compile(nodes, baseline_environment, preview_environment, config, message_id):
    logger = logging.getLogger(__name__)
    command = [config['executables']['puppet'], 'preview',
               '--baseline-environment', baseline_environment,
               '--preview-environment', preview_environment,
               '--view', 'overview-json',
               ]
    if config['preview']['excludes_file']:
        command += ['--excludes', config['preview']['excludes_file']]
    command += nodes

    logger.info('Running puppet preview for message {}'.format(message_id), extra={'MESSAGE_ID': message_id})
    logger.debug('Using preview command: {}'.format(' '.join(command)), extra={'MESSAGE_ID': message_id})

    preview_process = yield from asyncio.create_subprocess_exec(*command,
                                                                stdout=asyncio.subprocess.PIPE,
                                                                stderr=asyncio.subprocess.PIPE,
                                                                stdin=asyncio.subprocess.PIPE)
    stdout, stderr = yield from preview_process.communicate(input="\n".join(nodes).encode('latin-1'))
    return_code = yield from preview_process.wait()

    logger.debug('Execution of puppet preview returned {}'.format(return_code), extra={'MESSAGE_ID': message_id})

    if return_code != 0:
        msg = "\n".join(['Execution of puppet preview failed!', stderr])
        logger.error(msg, extra={'MESSAGE_ID': message_id})
        raise subprocess.CalledProcessError(msg)

    results = json.loads(stdout.decode('utf8'))
    node_results = results['all_nodes']
    success_count = len([node for node in node_results if node['error_count'] == 0])
    failure_count = len([node for node in node_results if node['error_count'] > 0])

    logger.info("\n".join([
        '{} node catalogs compiled successfully'.format(success_count),
        '{} node catalogs failed to compile'.format(failure_count),
    ]), extra={'MESSAGE_ID': message_id})

    # TODO: process results further
    # TODO: send results to elasticsearch

    # TODO: make this better
    report = {
        'success_count': success_count,
        'failure_count': failure_count,
        'raw': results,
    }

    return report


@asyncio.coroutine
def deploy_pr(pr: pcts.github.PullRequest, config, message_id):
    pr_ref = 'refs/pull/{}/merge'.format(pr.number)
    environment_name = 'pr_{}'.format(pr.number)

    yield from armature_deploy(ref=pr_ref,
                               environment=environment_name,
                               repo=pr.repo,
                               executable=config['executables']['armature'],
                               message_id=message_id)
    yield from armature_deploy(ref=pr.base_ref,
                               environment=pr.base_ref,
                               repo=pr.repo,
                               executable=config['executables']['armature'],
                               message_id=message_id)


@asyncio.coroutine
def armature_deploy(ref, environment, repo, executable, message_id):
    logger = logging.getLogger(__name__)
    command = [executable, 'deploy-ref', repo, ref, environment]

    logger.info('Deploying environment {} with armature'.format(environment), extra={'MESSAGE_ID': message_id})
    logger.debug('Using armature command {}'.format(' '.join(command)), extra={'MESSAGE_ID': message_id})

    preview_process = yield from asyncio.create_subprocess_exec(*command,
                                                                stdout=asyncio.subprocess.PIPE,
                                                                stderr=asyncio.subprocess.PIPE)
    stdout, stderr = yield from preview_process.communicate()
    return_code = yield from preview_process.wait()

    logger.debug('Execution of armature returned {}'.format(return_code), extra={'MESSAGE_ID': message_id})

    if return_code != 0:
        msg = "\n".join(['Execution of armature failed!', stderr])
        logger.error(msg, extra={'MESSAGE_ID': message_id})
        raise subprocess.CalledProcessError(msg)

    logger.debug('Successfully deployed environment {} with armature'.format(environment),
                 extra={'MESSAGE_ID': message_id})


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
