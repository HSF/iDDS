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

from idds.common.constants import ProcessingStatus, CollectionStatus
from idds.common.utils import setup_logging
from idds.core import catalog as core_catalog

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
