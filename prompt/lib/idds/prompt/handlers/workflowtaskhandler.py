#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""
Handlers for workflow task STOMP messages published on /topic/panda.workflow.

Message types handled:
- create_workflow_task
- adjust_worker
- close_workflow_task
"""

from idds.common.utils import setup_logging
from idds.common.constants import (
    RequestType,
    RequestStatus,
    RequestLocking,
    TransformType,
    TransformStatus,
    ProcessingType,
    ProcessingStatus,
    CollectionType,
    CollectionStatus,
    CollectionRelationType,
)
from idds.core import requests as core_requests
from idds.core import transforms as core_transforms
from idds.core import catalog as core_catalog
from idds.core import processings as core_processings
from idds.orm.base.session import transactional_session


setup_logging(__name__)


@transactional_session
def create_workflow_task(workflow, logger=None, session=None):
    """
    Create a workflow task from a *create_workflow_task* message.

    :param workflow: The ``content.workflow`` dict from the STOMP/REST message.
                     Expected keys: scope, name, requester, username, transform_tag,
                     cloud, campaign, campaign_scope, campaign_group, campaign_tag,
                     content.{run_id, core_count, memory_per_core, site, panda_attributes, ...}
    :returns: (request_id, transform_id, processing_id, input_coll_id, output_coll_id, workload_id)
    """
    scope = workflow.get('scope')
    name = workflow.get('name')
    requester = workflow.get('requester', 'iDDS')
    username = workflow.get('username', 'iDDS')
    transform_tag = workflow.get('transform_tag', 'EIC')
    cloud = workflow.get('cloud', 'US')
    campaign = workflow.get('campaign', 'EIC')
    campaign_scope = workflow.get('campaign_scope')
    campaign_group = workflow.get('campaign_group')
    campaign_tag = workflow.get('campaign_tag')

    content = workflow.get('content', {})
    run_id = content.get('run_id')
    core_count = content.get('core_count')
    memory_per_core = content.get('memory_per_core')
    site = content.get('site')
    panda_attributes = content.get('panda_attributes', {})

    # workflow_name is the per-scope/campaign name (name without per-run suffix)
    # name format: "<scope>_<transform_tag>_fastprocessing_<site>_<YYYYMMDD>_<run_id>"
    # workflow_name: "<scope>_<transform_tag>_fastprocessing_<site>_<YYYYMMDD>"
    if run_id and name and str(run_id) in name:
        workflow_name = name[: name.rfind('_' + str(run_id))]
    else:
        workflow_name = name

    reqs = core_requests.get_request_ids_by_name(
        scope=scope, name=workflow_name, exact_match=True, session=session
    )

    if reqs:
        request_id = reqs[workflow_name]
    else:
        workflow_req = {
            'scope': scope,
            'name': workflow_name,
            'requester': requester,
            'request_type': RequestType.iWorkflow,
            'username': username,
            'transform_tag': transform_tag,
            'status': RequestStatus.Transforming,
            'locking': RequestLocking.Idle,
            'cloud': cloud,
            'campaign': campaign,
            'campaign_scope': campaign_scope,
            'campaign_group': campaign_group,
            'campaign_tag': campaign_tag,
        }
        request_id = core_requests.add_request(**workflow_req, session=session)

    transform = {
        'request_id': request_id,
        'workload_id': None,
        'transform_type': TransformType.iWork,
        'transform_tag': transform_tag,
        'name': name,
        'status': TransformStatus.New,
        'substatus': TransformStatus.New,
        'transform_metadata': {
            'core_count': core_count,
            'memory_per_core': memory_per_core,
            'site': site,
            'panda_attributes': panda_attributes,
            'run_id': run_id,
            'workflow_content': content,
        },
    }
    transform_id = core_transforms.add_transform(**transform, session=session)

    coll_base = {
        'request_id': request_id,
        'transform_id': transform_id,
        'workload_id': None,
        'scope': scope,
        'name': name,
        'coll_type': CollectionType.Dataset,
        'bytes': 0,
        'total_files': 0,
        'new_files': 0,
        'processed_files': 0,
        'processing_files': 0,
        'coll_metadata': None,
        'status': CollectionStatus.Closed,
    }
    input_coll = dict(coll_base, relation_type=CollectionRelationType.Input)
    output_coll = dict(coll_base, relation_type=CollectionRelationType.Output)
    input_coll_id = core_catalog.add_collection(**input_coll, session=session)
    output_coll_id = core_catalog.add_collection(**output_coll, session=session)

    processing = {
        'request_id': request_id,
        'transform_id': transform_id,
        'workload_id': None,
        'status': ProcessingStatus.New,
        'submitter': 'panda',
        'site': site,
        'processing_type': ProcessingType.iWork,
        'processing_metadata': {
            'core_count': core_count,
            'memory_per_core': memory_per_core,
            'site': site,
            'panda_attributes': panda_attributes,
            'run_id': run_id,
        },
    }
    processing_id = core_processings.add_processing(**processing, session=session)

    if logger:
        logger.info(
            f"create_workflow_task: request_id={request_id}, transform_id={transform_id}, "
            f"processing_id={processing_id}, workload_id=None (pending submission)"
        )

    return request_id, transform_id, processing_id, input_coll_id, output_coll_id, None


def adjust_worker(request_id, transform_id, workload_id, parameters, logger=None):
    """
    Adjust worker resource parameters for an existing workflow task.

    Stores the updated parameters in the transform metadata so the carrier
    agent can apply them on the next poll cycle.

    :param request_id:   iDDS request id.
    :param transform_id: iDDS transform id.
    :param workload_id:  PanDA workload/task id.
    :param parameters:   Dict with keys core_count, memory_per_core, site, content.
    """
    adjust_params = {}
    for key in ('core_count', 'memory_per_core', 'site'):
        if parameters.get(key) is not None:
            adjust_params[key] = parameters[key]

    if adjust_params:
        core_transforms.update_transform(
            transform_id,
            parameters={'transform_metadata': adjust_params},
        )

    if logger:
        logger.info(
            f"adjust_worker: request_id={request_id}, transform_id={transform_id}, "
            f"workload_id={workload_id}, params={adjust_params}"
        )


def close_workflow_task(request_id, parameters, logger=None):
    """
    Close a workflow task by moving its processing(s) to ToCancel status.

    The carrier agent will detect ToCancel and close the PanDA task.

    :param request_id:  iDDS request id.
    :param parameters:  Dict that may include transform_id, workload_id, run_id.
    """
    transform_id = parameters.get('transform_id') if parameters else None
    workload_id = parameters.get('workload_id') if parameters else None

    processings = core_processings.get_processings(
        request_id=request_id,
        transform_id=transform_id,
        workload_id=workload_id,
    )

    for processing in (processings or []):
        proc_id = processing.get('processing_id') or processing.get('id')
        if proc_id:
            core_processings.update_processing(
                proc_id,
                parameters={'status': ProcessingStatus.ToCancel},
            )

    if logger:
        logger.info(
            f"close_workflow_task: request_id={request_id}, transform_id={transform_id}, "
            f"workload_id={workload_id}, marked {len(processings or [])} processing(s) ToCancel"
        )


# ---------------------------------------------------------------------------
# STOMP message dispatchers  (called by transceiver)
# ---------------------------------------------------------------------------

def handle_create_workflow_task(msg, logger=None):
    """
    Dispatch handler for msg_type='create_workflow_task'.

    :param msg: Full STOMP message dict.
    :returns: (request_id, transform_id, processing_id, input_coll_id, output_coll_id, workload_id)
    """
    content = msg.get('content', {})
    workflow = content.get('workflow', content)
    return create_workflow_task(workflow, logger=logger)


def handle_adjust_worker(msg, logger=None):
    """
    Dispatch handler for msg_type='adjust_worker'.

    :param msg: Full STOMP message dict.
    """
    content = msg.get('content', {})
    request_id = content.get('request_id')
    transform_id = content.get('transform_id')
    workload_id = content.get('workload_id')
    parameters = {
        'core_count': content.get('core_count'),
        'memory_per_core': content.get('memory_per_core'),
        'site': content.get('site'),
        'content': content,
    }
    adjust_worker(request_id, transform_id, workload_id, parameters, logger=logger)


def handle_close_workflow_task(msg, logger=None):
    """
    Dispatch handler for msg_type='close_workflow_task'.

    :param msg: Full STOMP message dict.
    """
    content = msg.get('content', {})
    request_id = content.get('request_id')
    close_workflow_task(request_id, parameters=content, logger=logger)
