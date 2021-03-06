import pcts.elasticsearch
import pcts.github
import pcts.puppet

import asyncio
import configparser
import logging
import traceback


handlers = dict()


def handler(event_type):
    def dummy_decorator(func):
        handlers[event_type] = func
        return func
    return dummy_decorator


@handler('pull_request')
@asyncio.coroutine
def handle_pull_request(payload, id, config):
    logger = logging.getLogger('{}.worker'.format(__name__))
    logger.debug('Handling message {}'.format(id),
                 extra={'MESSAGE_ID': id})

    dashboard_vars = {
        'message_id': id,
        'pr_number': payload['number'],
        'repo_name': payload['repository']['name'],
        'repo_full_name': payload['repository']['full_name'],
    }
    uri = config['elasticsearch']['dashboard'].format(**dashboard_vars)
    logger.debug('Using {} for GitHub status URI'.format(uri))
    try:
        pr = pcts.github.PullRequest(payload=payload, auth_token=config['github']['auth_token'])
        pdb = pcts.puppet.PuppetDB(pdb_config=config['puppetdb'])

        yield from pr.update_status(state='pending',
                         target_url=uri,
                         description='Testing of catalog compilation in progress',
                         message_id=id)

        pdb_f = asyncio.async(pdb.get_nodes_by_files(filenames=pr.get_files(), message_id=id))
        deploy_f = asyncio.async(pcts.puppet.deploy_pr(pr=pr, config=config, message_id=id))
        yield from asyncio.wait([pdb_f, deploy_f])
        affected_nodes = pdb_f.result()

        report = yield from pcts.puppet.preview_compile(nodes=affected_nodes,
                                                        baseline_environment=pr.base_ref,
                                                        preview_environment='pr_{}'.format(pr.number),
                                                        config=config,
                                                        message_id=id)
        yield from pcts.elasticsearch.submit_report(report=report['raw'],
                                                          pr=pr,
                                                          es_config=config['elasticsearch'],
                                                          message_id=id)
        if report['failure_count'] == 0:
            msg = 'All {} catalogs compiled successfully'.format(report['success_count'])
            logger.info(msg, extra={'MESSAGE_ID': id})
            yield from pr.update_status(state='success',
                                        target_url=uri,
                                        description=msg,
                                        message_id=id)
        else:
            msg = 'Compiled {0} catalogs successfully, but failed to compile catalogs for {1} nodes'.format(
                report['success_count'],
                report['failure_count'],
            )
            logger.info(msg, extra={'MESSAGE_ID': id})
            yield from pr.update_status(state='failure',
                                        target_url=uri,
                                        description=msg,
                                        message_id=id)
    except:
        logger.error('Caught exception when trying to test catalog compilation: {}'.format(traceback.format_exc()),
                     extra={'MESSAGE_ID': id})
        yield from pr.update_status(state='error',
                                    target_url=uri,
                                    description='An exception occurred when trying to compile catalogs.',
                                    message_id=id)
        raise


@asyncio.coroutine
def worker(queue: asyncio.JoinableQueue, config: configparser.ConfigParser):
    logger = logging.getLogger('{}.worker'.format(__name__))
    logger.info('Starting worker')
    while True:
        message = yield from queue.get()
        logger.info('Processing message {0} of event type "{1}" from queue'.format(message['id'], message['event']),
                    extra={'MESSAGE_ID': message['id']})
        handler_f = handlers.get(message['event'])
        if handler_f:
            try:
                yield from handler_f(payload=message['body'], id=message['id'], config=config)
            except:
                logger.error('Error received when processing message {0}: {1}'.format(message['id'],
                                                                                      traceback.format_exc()),
                             extra={'MESSAGE_ID': message['id']})
        else:
            logger.info('No action to take on event type "{}"'.format(message['event']),
                        extra={'MESSAGE_ID': message['id']})
        queue.task_done()
