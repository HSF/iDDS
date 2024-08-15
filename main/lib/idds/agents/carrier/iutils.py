#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


import datetime
import logging

from idds.common.constants import (ProcessingStatus, CollectionStatus, ContentStatus,
                                   MessageType, MessageStatus, MessageSource, MessageDestination,
                                   ContentType, ContentRelationType)
from idds.common.utils import setup_logging
from idds.core import (catalog as core_catalog, messages as core_messages)
from idds.agents.common.cache.redis import get_redis_cache

setup_logging(__name__)


def get_logger(logger=None):
    if logger:
        return logger
    logger = logging.getLogger(__name__)
    return logger


def is_process_terminated(processing_status):
    if processing_status in [ProcessingStatus.Finished, ProcessingStatus.Failed,
                             ProcessingStatus.SubFinished, ProcessingStatus.Cancelled,
                             ProcessingStatus.Suspended, ProcessingStatus.Expired,
                             ProcessingStatus.Broken, ProcessingStatus.FinishedOnStep,
                             ProcessingStatus.FinishedOnExec, ProcessingStatus.FinishedTerm]:
        return True
    return False


def handle_new_iprocessing(processing, agent_attributes, plugin=None, func_site_to_cloud=None, max_updates_per_round=2000, executors=None, logger=None, log_prefix=''):
    logger = get_logger(logger)

    work = processing['processing_metadata']['work']
    # transform_id = processing['transform_id']

    try:
        workload_id, errors = plugin.submit(work, logger=logger, log_prefix=log_prefix)
        logger.info(log_prefix + "submit work (workload_id: %s, errors: %s)" % (workload_id, errors))
    except Exception as ex:
        err_msg = "submit work failed with exception: %s" % (ex)
        logger.error(log_prefix + err_msg)
        raise Exception(err_msg)

    processing['workload_id'] = workload_id
    processing['submitted_at'] = datetime.datetime.utcnow()

    # return True, processing, update_collections, new_contents, new_input_dependency_contents, ret_msgs, errors
    return True, processing, [], [], [], [], errors


def handle_update_iprocessing(processing, agent_attributes, plugin=None, max_updates_per_round=2000, use_bulk_update_mappings=True, executors=None, logger=None, log_prefix=''):
    logger = get_logger(logger)

    # work = processing['processing_metadata']['work']

    # request_id = processing['request_id']
    # transform_id = processing['transform_id']
    workload_id = processing['workload_id']

    try:
        status = plugin.poll(workload_id, logger=logger, log_prefix=log_prefix)
        logger.info(log_prefix + "poll work (status: %s, workload_id: %s)" % (status, workload_id))
    except Exception as ex:
        err_msg = "poll work failed with exception: %s" % (ex)
        logger.error(log_prefix + err_msg)
        raise Exception(err_msg)

    return status, [], [], [], [], [], [], []


def handle_abort_iprocessing(processing, agent_attributes, plugin=None, logger=None, log_prefix=''):
    logger = get_logger(logger)

    workload_id = processing['workload_id']

    try:
        status = plugin.abort(workload_id, logger=logger, log_prefix=log_prefix)
        logger.info(log_prefix + "abort work (status: %s, workload_id: %s)" % (status, workload_id))
    except Exception as ex:
        err_msg = "abort work failed with exception: %s" % (ex)
        logger.error(log_prefix + err_msg)
        raise Exception(err_msg)
    return status, [], [], []


def handle_resume_iprocessing(processing, agent_attributes, plugin=None, logger=None, log_prefix=''):
    logger = get_logger(logger)

    workload_id = processing['workload_id']

    try:
        status = plugin.resume(workload_id, logger=logger, log_prefix=log_prefix)
        logger.info(log_prefix + "resume work (status: %s, workload_id: %s)" % (status, workload_id))
    except Exception as ex:
        err_msg = "resume work failed with exception: %s" % (ex)
        logger.error(log_prefix + err_msg)
        raise Exception(err_msg)
    return status, [], []


def sync_iprocessing(processing, agent_attributes, terminate=False, abort=False, logger=None, log_prefix=""):
    # logger = get_logger()

    # request_id = processing['request_id']
    # transform_id = processing['transform_id']
    # workload_id = processing['workload_id']

    # work = processing['processing_metadata']['work']

    u_colls = []
    if processing['substatus'] in [ProcessingStatus.Finished, ProcessingStatus.Failed, ProcessingStatus.SubFinished, ProcessingStatus.Broken]:
        collections = core_catalog.get_collections(transform_id=processing['transform_id'])
        if collections:
            for coll in collections:
                u_coll = {'coll_id': coll['coll_id'],
                          'status': CollectionStatus.Closed}
                u_colls.append(u_coll)

    processing['status'] = processing['substatus']

    return processing, u_colls, None


def get_request_id_transform_id_collection_id_map(request_id, transform_id):
    cache = get_redis_cache()
    coll_tf_id_map_key = "req_id_trf_id_coll_id_map"
    coll_tf_id_map = cache.get(coll_tf_id_map_key, default={})

    if request_id is not None and transform_id is not None:
        if request_id not in coll_tf_id_map or transform_id not in coll_tf_id_map[request_id]:
            colls = core_catalog.get_collections_by_request_ids([request_id])
            for coll in colls:
                if coll['request_id'] not in coll_tf_id_map:
                    coll_tf_id_map[coll['request_id']] = {}
                if coll['transform_id'] not in coll_tf_id_map[coll['request_id']]:
                    coll_tf_id_map[coll['request_id']][coll['transform_id']] = {}
                if coll['relation_type'].value not in coll_tf_id_map[coll['request_id']][coll['transform_id']]:
                    coll_tf_id_map[coll['request_id']][coll['transform_id']][coll['relation_type'].value] = []
                if coll['coll_id'] not in coll_tf_id_map[coll['request_id']][coll['transform_id']][coll['relation_type'].value]:
                    coll_tf_id_map[coll['request_id']][coll['transform_id']][coll['relation_type'].value].append[coll['coll_id']]

            cache.set(coll_tf_id_map_key, coll_tf_id_map)

        return coll_tf_id_map[request_id][transform_id]
    return None


def get_new_asyncresult_content(request_id, transform_id, name, path, workload_id=0, coll_id=0, map_id=0, scope='asyncresult',
                                status=ContentStatus.Available, content_relation_type=ContentRelationType.Output):
    content = {'transform_id': transform_id,
               'coll_id': coll_id,
               'request_id': request_id,
               'workload_id': workload_id,
               'map_id': map_id,
               'scope': scope,
               'name': name,
               'min_id': 0,
               'max_id': 0,
               'status': status,
               'substatus': status,
               'path': path,
               'content_type': ContentType.PseudoContent,
               'content_relation_type': content_relation_type,
               'bytes': 0}
    return content


def handle_messages_asyncresult(messages, logger=None, log_prefix='', update_processing_interval=300):
    logger = get_logger(logger)
    if not log_prefix:
        log_prefix = "<Message_AsyncResult>"

    req_msgs = {}

    for item in messages:
        if 'from_idds' in item:
            if type(item['from_idds']) in [bool] and item['from_idds'] or type(item['from_idds']) in [str] and item['from_idds'].lower() == 'true':
                continue

        msg = item['msg']

        # ret = msg['ret']
        # key = msg['key']
        # internal_id = msg['internal_id']
        # msg_type = msg['type']
        request_id = msg['body']['request_id']
        transform_id = msg['body'].get('transform_id', 0)
        internal_id = msg['body'].get('internal_id', None)
        # if msg_type in ['iworkflow']:

        if request_id not in req_msgs:
            req_msgs[request_id] = {}
        if transform_id not in req_msgs[request_id]:
            req_msgs[request_id][transform_id] = {}
        if internal_id not in req_msgs[request_id][transform_id]:
            req_msgs[request_id][transform_id][internal_id] = []

        msgs = [msg]
        core_messages.add_message(msg_type=MessageType.AsyncResult,
                                  status=MessageStatus.NoNeedDelivery,
                                  destination=MessageDestination.AsyncResult,
                                  source=MessageSource.OutSide,
                                  request_id=request_id,
                                  workload_id=None,
                                  transform_id=transform_id,
                                  internal_id=internal_id,
                                  num_contents=len(msgs),
                                  msg_content=msgs)

        logger.debug(f"{log_prefix} handle_messages_asyncresult, add {len(msgs)} for request_id {request_id} transform_id {transform_id} internal_id {internal_id}")
