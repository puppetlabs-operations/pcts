import asyncio
import logging
import re

import elasticsearch
import elasticsearch.helpers


def generate_actions(summary, nodes, errors, warnings, resource_changes, edge_changes, pr, index):
    isoyear, isoweek, isoday = pr.updated_time.isocalendar()
    index_vars = {
        'isoday': isoday,
        'isoweek': isoweek,
        'isoyear': isoyear,
        'day': pr.updated_time.day,
        'month': pr.updated_time.month,
        'year': pr.updated_time.year,
    }
    actions = []
    summary.update({'_index': index.format(**index_vars), '_type': 'summary'})
    actions.append(summary)
    for node in nodes:
        node.update({'_index': index.format(**index_vars),  '_type': 'node'})
        actions.append(node)
    for error in errors:
        error.update({'_index': index.format(**index_vars),  '_type': 'error'})
        actions.append(error)
    for warning in warnings:
        warning.update({'_index': index.format(**index_vars),  '_type': 'warning'})
        actions.append(warning)
    for resource_change in resource_changes:
        resource_change.update({'_index': index.format(**index_vars),  '_type': 'resource_change'})
        actions.append(resource_change)
    for edge_change in edge_changes:
        edge_change.update({'_index': index.format(**index_vars),  '_type': 'edge_change'})
        actions.append(edge_change)
    return actions


@asyncio.coroutine
def send_to_es(actions, config, message_id):
    logger = logging.getLogger(__name__)
    es = elasticsearch.Elasticsearch([{'host': config['host'], 'port': config['port']}])
    logger.info('Submitting report to ElasticSearch at {0}:{1}'.format(config['host'], config['port']),
                extra={
                    'MESSAGE_ID': message_id,
                    'ELASTICSEARCH_HOST': config['host'],
                    'ELASTICSEARCH_PORT': config['port'],
                })
    try:
        oks, fails = elasticsearch.helpers.bulk(client=es,
                                                actions=actions,
                                                raise_on_error=False,
                                                raise_on_exception=False)
        logger.info('Submitted report to ElasticSearch',
                    extra={
                        'MESSAGE_ID': message_id,
                        'ELASTICSEARCH_HOST': config['host'],
                        'ELASTICSEARCH_PORT': config['port'],
                    })
        logger.debug('Successfully submitted {} documents to ElasticSearch'.format(oks),
                     extra={
                         'MESSAGE_ID': message_id,
                         'ELASTICSEARCH_HOST': config['host'],
                         'ELASTICSEARCH_PORT': config['port'],
                     })
        if fails:
            logger.error('Failed to index {} documents'.format(len(fails)),
                         extra={
                             'MESSAGE_ID': message_id,
                             'ELASTICSEARCH_HOST': config['host'],
                             'ELASTICSEARCH_PORT': config['port'],
                         })
            for err in fails:
                err = err['create']
                logger.error('Failed to submit data to ElasticSearch',
                             extra={
                                 'MESSAGE_ID': message_id,
                                 'ELASTICSEARCH_HOST': config['host'],
                                 'ELASTICSEARCH_PORT': config['port'],
                                 'ELASTICSEARCH_STATUS_CODE': err.get('status'),
                                 'ELASTICSEARCH_ERROR': err.get('error'),
                                 'ELASTICSEARCH_EXCEPTION': err.get('exception'),
                                 'ELASTICSEARCH_DATA': err.get('data'),
                             })
            raise elasticsearch.helpers.BulkIndexError('Failed to index {} documents'.format(len(fails)), fails)
    except elasticsearch.ElasticsearchException:
        logger.error('Something went wrong while connecting to ElasticSearch',
                     extra={
                         'MESSAGE_ID': message_id,
                         'ELASTICSEARCH_HOST': config['host'],
                         'ELASTICSEARCH_PORT': config['port'],
                     })
        raise

def process_report(report, pr, message_id):
    nodes = report['all_nodes'].copy()
    errors = []
    resource_changes = []
    edge_changes = []
    for node in nodes:
        node['errors'] = []
        node['resource_changes'] = []
        node['edge_changes'] = []

    summary = {
        'message_id': message_id,
        'pull_request': pr.number,
        'base_environment': pr.base_ref,
        'repository': pr.repo,
        'node_count': report['stats']['node_count'],
        'success_count': len([node for node in nodes if node['error_count'] == 0]),
        'failure_count': len([node for node in nodes if node['error_count'] > 0]),
        'equal': {
            'total': report['stats'].get('equal', {}).get('total', 0),
            'percent': report['stats'].get('equal', {}).get('percent', 0),
        },
        'conflicting': {
            'total': report['stats'].get('conflicting', {}).get('total', 0),
            'percent': report['stats'].get('conflicting', {}).get('percent', 0),
        },
        'failures': {
            'total': report['stats'].get('failures', {}).get('total', 0),
            'percent': report['stats'].get('failures', {}).get('percent', 0),
        },
        'preview_failures': {
            'total': report['stats'].get('failures', {}).get('preview', {}).get('total', 0),
            'percent': report['stats'].get('failures', {}).get('preview', {}).get('percent', 0),
        },
    }

    if report['preview'].get('compilation_errors'):
        for manifest_error in report['preview']['compilation_errors']:
            deduped_errors = [ error.copy() for error in manifest_error['errors'] ]
            for node in manifest_error['nodes']:
                for error in deduped_errors:
                    error['message'] = error['message'].replace(node, '<node>')
            deduped_errors = set(deduped_errors)
            for error in deduped_errors:
                error_node = error.copy()
                error_node['manifest'] = manifest_error['manifest']
                for node in nodes:
                    if node['name'] in manifest_error['nodes']:
                        node['errors'].append(error_node)

                error_single = error_node.copy()
                error_single.update({'nodes': manifest_error['nodes']})
                errors.append(error_single)

    if report['preview'].get('warning_count_by_issue_code'):
        for warning in report['preview']['warning_count_by_issue_code']:
            warning['manifests'] = [
                {
                    "file": manifest,
                    "locs": [
                        {
                            "line": int(line_pos.split(':')[0]),
                            "pos": int(line_pos.split(':')[1]),
                        }
                        for line_pos in warning['manifests'][manifest]],
                }
                for manifest in warning['manifests']]
        warnings = report['preview']['warning_count_by_issue_code']

    if report['changes'].get('resource_type_changes'):
        rtc = report['changes']['resource_type_changes']
        for resource_type in rtc:
            if rtc[resource_type].get('conflicting_resources'):
                for resource_title in rtc[resource_type]['conflicting_resources']:
                    for file in rtc[resource_type]['conflicting_resources'][resource_title]:
                        file_name, file_line = file.split(':')
                        resource = {
                            'type': resource_type,
                            'title': resource_title,
                            'file': file_name,
                            'line': file_line,
                        }
                        resource_node_list = rtc[resource_type]['conflicting_resources'][resource_title][file]
                        for node in nodes:
                            if node['name'] in resource_node_list:
                                resource_node = resource.copy()
                                resource_node['attributes'] = []
                                for attribute_name in rtc[resource_type]['attribute_issues']:
                                    attribute = rtc[resource_type]['attribute_issues'][attribute_name]
                                    if (attribute['conflicting_in'].get(resource_title)
                                        and attribute['conflicting_in'][resource_title].get(file)
                                        and node['name'] in attribute['conflicting_in'][resource_title][file]):

                                        resource_node['attributes'].append(attribute_name)
                                node['resource_changes'].append(resource_node)

                        resource_single = resource.copy()
                        resource_single.update({'nodes': resource_node_list})
                        resource_changes.append(resource_single)

    if report['changes'].get('edge_changes'):
        if report['changes']['edge_changes'].get('added_edges'):
            added_edges = report['changes']['edge_changes']['added_edges']
            resource_regex = '([^\[]+)\[([^\]]+)\]'
            for from_resource in added_edges:
                for to_resource in added_edges[from_resource]:
                    from_match = re.search(resource_regex, from_resource)
                    from_type = from_match.group(1)
                    from_title = from_match.group(2)

                    to_match = re.search(resource_regex, to_resource)
                    to_type = to_match.group(1)
                    to_title = to_match.group(2)

                    new_edge = {
                        'edge': 'added',
                        'source_type': from_type,
                        'source_title': from_title,
                        'target_type': to_type,
                        'target_title': to_title,
                    }

                    for node in nodes:
                        if node['name'] in added_edges[from_resource][to_resource]:
                            node['edge_changes'].append(new_edge)

                    edge_single = new_edge.copy()
                    edge_single.update({'nodes': added_edges[from_resource][to_resource]})
                    edge_changes.append(edge_single)

    for node in nodes:
        node['message_id'] = message_id
        node['pull_request'] = pr.number
        node['base_environment'] = pr.base_ref
        node['repository'] = pr.repo
    for error in errors:
        error['message_id'] = message_id
        error['pull_request'] = pr.number
        error['base_environment'] = pr.base_ref
        error['repository'] = pr.repo
    for warning in warnings:
        warning['message_id'] = message_id
        warning['pull_request'] = pr.number
        warning['base_environment'] = pr.base_ref
        warning['repository'] = pr.repo
    for resource_change in resource_changes:
        resource_change['message_id'] = message_id
        resource_change['pull_request'] = pr.number
        resource_change['base_environment'] = pr.base_ref
        resource_change['repository'] = pr.repo
    for edge_change in edge_changes:
        edge_change['message_id'] = message_id
        edge_change['pull_request'] = pr.number
        edge_change['base_environment'] = pr.base_ref
        edge_change['repository'] = pr.repo

    return summary, nodes, errors, warnings, resource_changes, edge_changes


@asyncio.coroutine
def submit_report(report, pr, es_config, message_id):
    logger = logging.getLogger(__name__)
    logger.debug('Processing report data to send to ElasticSearch', extra={'MESSAGE_ID': message_id})
    summary, nodes, errors, warnings, resource_changes, edge_changes = process_report(report, pr, message_id)
    logger.debug('Preparing processed data for submission to ElasticSearch', extra={'MESSAGE_ID': message_id})
    actions = generate_actions(summary=summary,
                               nodes=nodes,
                               errors=errors,
                               warnings=warnings,
                               resource_changes=resource_changes,
                               edge_changes=edge_changes,
                               pr=pr,
                               index=es_config['index'])
    logger.debug('Attempting to send data to ElasticSearch', extra={'MESSAGE_ID': message_id})
    yield from asyncio.wait_for(send_to_es(actions=actions, config=es_config, message_id=message_id), 60)
