#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025


import datetime

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

from .panda import PandaClient


setup_logging(__name__)


def get_scope_name(run_id, site, panda_attributes={}):
    """Generate scope and name for a run."""
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    year = utc_now.year
    month = utc_now.month
    day = utc_now.day
    scope = f"EIC_{year}"
    if site is None:
        site = panda_attributes.get("site", None)
    workflow_name = f"EIC_fastprocessing_{site}_{year}{month}{day}"
    name = f"EIC_fastprocessing_{site}_{year}{month}{day}_{run_id}"
    return scope, workflow_name, name


def get_task_parameters(
    request_id,
    transform_id=None,
    name=None,
    num_workers=None,
    core_count=None,
    ram_count=None,
    site=None,
    run_id=None,
    panda_attributes={},
):
    """
    Build PanDA task parameters.
    """
    vo = panda_attributes.get("vo", "wlcg")
    queue = panda_attributes.get("queue", None)
    if site is None:
        site = panda_attributes.get("site", None)
    cloud = panda_attributes.get("cloud", None)
    working_group = panda_attributes.get("working_group", None)
    priority = panda_attributes.get("priority", 900)
    if core_count is None:
        core_count = panda_attributes.get("core_count", 1)
    if ram_count is None:
        ram_count = panda_attributes.get("ram_count", 4000)
    if num_workers is None:
        num_workers = panda_attributes.get("num_workers", 1)
    max_walltime = panda_attributes.get("max_walltime", 3600)
    max_attempt = panda_attributes.get("max_attempt", 3)
    idle_timeout = panda_attributes.get("idle_timeout", 120)

    task_param_map = {}
    task_param_map["vo"] = vo
    if queue:
        task_param_map["site"] = queue
    if site:
        task_param_map["PandaSite"] = site
    if cloud:
        task_param_map["cloud"] = cloud

    if working_group:
        task_param_map["workingGroup"] = working_group

    task_param_map["taskName"] = name
    task_param_map["userName"] = "iDDS"
    task_param_map["taskPriority"] = priority
    task_param_map["architecture"] = ""
    # task_param_map['architecture'] = '@el9'
    task_param_map["transUses"] = ""
    task_param_map["transHome"] = None

    executable = f"--run_id {run_id} --idle_timeout {idle_timeout}"
    task_param_map["transPath"] = (
        "https://storage.googleapis.com/drp-us-central1-containers/run_prompt_wrapper"
    )

    task_param_map["processingType"] = None
    task_param_map["prodSourceLabel"] = "managed"  # managed, test, ptest

    # task_param_map['noWaitParent'] = True
    task_param_map["taskType"] = "iDDS"
    task_param_map["coreCount"] = core_count
    task_param_map["skipScout"] = True
    task_param_map["ramCount"] = ram_count
    # task_param_map['ramUnit'] = 'MB'
    task_param_map["ramUnit"] = "MBPerCoreFixed"

    # task_param_map['inputPreStaging'] = True
    task_param_map["prestagingRuleID"] = 123
    task_param_map["nChunksToWait"] = 1
    task_param_map["maxCpuCount"] = core_count
    task_param_map["maxWalltime"] = max_walltime
    task_param_map["maxFailure"] = max_attempt
    task_param_map["maxAttempt"] = max_attempt

    task_param_map["jobParameters"] = [
        {
            "type": "constant",
            "value": executable,  # noqa: E501
        },
    ]

    in_files = [f"{run_id}"]
    task_param_map["nFiles"] = len(in_files)
    task_param_map["noInput"] = True
    task_param_map["pfnList"] = in_files
    task_param_map["nFilesPerJob"] = 1

    task_param_map["nEvents"] = num_workers
    task_param_map["nEventsPerJob"] = 1

    task_param_map["reqID"] = request_id

    return task_param_map


def submit_task_to_panda(
    request_id,
    transform_id=None,
    name=None,
    num_workers=None,
    core_count=None,
    ram_count=None,
    site=None,
    run_id=None,
    panda_attributes={},
    logger=None,
):
    """
    Submit a task to Panda for processing.
    """

    # Submit the task to Panda
    panda_client = PandaClient()
    task_parameters = get_task_parameters(
        request_id,
        transform_id=transform_id,
        name=name,
        num_workers=num_workers,
        core_count=core_count,
        ram_count=ram_count,
        site=site,
        run_id=run_id,
        panda_attributes=panda_attributes,
    )
    task_id = panda_client.submit(
        task_parameters, logger=logger, log_prefix=f"[run_id={run_id}] "
    )
    if logger:
        logger.info(f"Submitted PanDA task {task_id} for run_id={run_id}")
    return task_id


def close_panda_task(task_id, logger=None):
    """
    Close a PanDA task to prevent further job retries.
    """
    panda_client = PandaClient()
    ret = panda_client.close(task_id, soft=True)
    if logger:
        logger.info(f"Finished PanDA task {task_id} with return code: {ret}")


@transactional_session
def create_workflow_task(message, panda_attributes={}, logger=None, session=None):
    """
    Create a workflow task in iDDS when receiving 'run_imminent' message.

    Message format:
    {
      'msg_type': 'run_imminent',
      'run_id': 20250914185722,
      'created_at': datetime.datetime.utcnow(),
      'content': {
        'num_workers': 2,
        'num_cores_per_worker': 10,
        'num_ram_per_core': 4000.0,  # MB
        ...
      }
    }

    Returns: (request_id, transform_id, processing_id, coll_id, workload_id)
    """
    run_id = message.get("run_id")
    content = message.get("content", {})

    # Extract resource requirements from message
    num_workers = content.get("target_worker_count", 1)
    core_count = content.get("num_cores_per_worker", None)
    ram_count = content.get("num_ram_per_core", None)
    site = content.get("site", None)

    utc_now = datetime.datetime.now(datetime.timezone.utc)
    year = utc_now.year
    month = utc_now.month

    scope, workflow_name, name = get_scope_name(run_id, site, panda_attributes)
    reqs = core_requests.get_request_ids_by_name(
        scope=scope, name=workflow_name, exact_match=True, session=session
    )

    if reqs:
        request_id = reqs[workflow_name]
    else:
        workflow = {
            "scope": scope,
            "name": workflow_name,
            "requester": "iDDS",
            "request_type": RequestType.iWorkflow,
            "username": "EIC",
            "transform_tag": "EIC",
            "status": RequestStatus.Transforming,
            "locking": RequestLocking.Idle,
            "cloud": "US",
            "campaign": "EIC",
            "campaign_scope": f"EIC_{year}",
            "campaign_group": f"EIC_{year}_{month}",
            "campaign_tag": "reco",
        }
        request_id = core_requests.add_request(**workflow, session=session)

    transform = {
        "request_id": request_id,
        "workload_id": None,
        "transform_type": TransformType.iWork,
        "transform_tag": "EIC",
        "name": name,
        "status": TransformStatus.New,
        "substatus": TransformStatus.New,
    }
    transform_id = core_transforms.add_transform(**transform, session=session)

    input_coll = {
        "request_id": request_id,
        "transform_id": transform_id,
        "workload_id": None,
        "scope": scope,
        "name": name,
        'coll_type': CollectionType.Dataset,
        'relation_type': CollectionRelationType.Input,
        'bytes': 0,
        'total_files': 0,
        'new_files': 0,
        'processed_files': 0,
        'processing_files': 0,
        'coll_metadata': None,
        'status': CollectionStatus.Closed,
    }
    output_coll = {
        "request_id": request_id,
        "transform_id": transform_id,
        "workload_id": None,
        "scope": scope,
        "name": name,
        'coll_type': CollectionType.Dataset,
        'relation_type': CollectionRelationType.Output,
        'bytes': 0,
        'total_files': 0,
        'new_files': 0,
        'processed_files': 0,
        'processing_files': 0,
        'coll_metadata': None,
        'status': CollectionStatus.Closed,
    }
    input_coll_id = core_catalog.add_collection(**input_coll, session=session)
    output_coll_id = core_catalog.add_collection(**output_coll, session=session)

    # For now, return a placeholder workload_id
    # In production, this should call submit_task_to_panda()
    workload_id = submit_task_to_panda(
        request_id,
        transform_id=transform_id,
        name=name,
        num_workers=num_workers,
        core_count=core_count,
        ram_count=ram_count,
        site=site,
        run_id=run_id,
        panda_attributes=panda_attributes,
        logger=logger,
    )

    processing = {
        "request_id": request_id,
        "transform_id": transform_id,
        "workload_id": workload_id,
        "status": ProcessingStatus.Submitting,
        "submitter": "panda",
        "site": site,
        "processing_type": ProcessingType.iWork,
    }
    processing_id = core_processings.add_processing(**processing, session=session)

    if logger:
        logger.info(
            f"Created workflow task: request_id={request_id}, transform_id={transform_id}, "
            f"processing_id={processing_id}, workload_id={workload_id}"
        )

    return request_id, transform_id, processing_id, input_coll_id, output_coll_id, workload_id


def create_harvester_worker(
    header,
    msg,
    task_id,
    harvester_publisher,
    timetolive=12 * 3600 * 1000,
    panda_attributes={},
    logger=None,
):
    """
    Send message to harvester to create workers.

    Message format based on prompt.md:
    {
      'msg_type': 'adjuster_worker',
      'run_id': 20250914185722,
      'created_at': datetime.datetime.utcnow(),
      'content': {
        'num_workers': 2,
        'num_cores_per_worker': 10,
        'num_ram_per_core': 4000.0,
        'requested_at': <original message created_at>
      }
    }
    """
    content = msg.get("content", {})
    run_id = msg.get("run_id")
    panda_queue = panda_attributes.get("queue", None)
    created_at_original = msg.get("created_at")

    worker_msg = {
        "msg_type": "adjuster_worker",
        "run_id": run_id,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "content": {
            "num_workers": content.get("num_workers", 1),
            "num_cores_per_worker": content.get("num_cores_per_worker", 1),
            "num_ram_per_core": content.get("num_ram_per_core", 1000),
            "panda_queue": panda_queue,
            "requested_at": created_at_original,
        },
    }

    msg_header = {
        "persistent": "true",
        "ttl": timetolive,
        "vo": "eic",
        "msg_type": "adjuster_worker",
        "run_id": str(run_id),
    }

    if harvester_publisher:
        harvester_publisher.publish(worker_msg, headers=msg_header)

        if logger:
            logger.info(
                f"Sent adjuster_worker message for run_id={run_id}, task_id={task_id}, num_workers={worker_msg['content']['num_workers']}"
            )


def stop_harvester_worker(
    header,
    msg,
    task_id,
    harvester_publisher,
    transformer_broadcaster,
    timetolive=12 * 3600 * 1000,
    panda_attributes={},
    logger=None,
):
    """
    Stop harvester workers when receiving 'run_end' message.

    Based on prompt.md:
    1. Close PanDA task to avoid retrying jobs
    2. Send worker adjuster message to stop creating new workers
    3. Broadcast stop message to all transformers

    Message formats:
    - adjuster_worker: Stop creating new workers
    - stop_transformer: Broadcast to all transformers to stop
    """
    # content = msg.get("content", {})
    run_id = msg.get("run_id")
    panda_queue = panda_attributes.get("queue", None)
    created_at_original = msg.get("created_at")

    # TODO: Close PanDA task
    # close_panda_task(task_id)

    # Send stop message to transformers (broadcast)
    stop_transformer_msg = {
        "msg_type": "stop_transformer",
        "run_id": run_id,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "content": {"task_id": task_id, "requested_at": created_at_original},
    }

    stop_header = {
        "persistent": "true",
        "ttl": timetolive,
        "vo": "eic",
        "msg_type": "stop_transformer",
        "run_id": str(run_id),
    }

    if transformer_broadcaster:
        transformer_broadcaster.publish(stop_transformer_msg, headers=stop_header)
        if logger:
            logger.info(
                f"Sent stop_transformer broadcast for run_id={run_id}, task_id={task_id}"
            )

    # Send message to stop creating new workers
    stop_worker_msg = {
        "msg_type": "adjuster_worker",
        "run_id": run_id,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "content": {
            "num_workers": 0,  # Stop creating new workers
            "num_cores_per_worker": 0,
            "num_ram_per_core": 0,
            "panda_queue": panda_queue,
            "requested_at": created_at_original,
        },
    }

    worker_header = {
        "persistent": "true",
        "ttl": timetolive,
        "vo": "eic",
        "msg_type": "adjuster_worker",
        "run_id": str(run_id),
    }

    if harvester_publisher:
        harvester_publisher.publish(stop_worker_msg, headers=worker_header)
        if logger:
            logger.info(
                f"Sent adjuster_worker (stop) message for run_id={run_id}, task_id={task_id}"
            )


def worker_handler(header, msg, task_id=None, handler_kwargs={}, logger=None):
    """
    Handle worker-related messages based on prompt.md specifications.

    Supported message types:
    - run_imminent: Create workflow task and start workers
    - run_end/run_stop: Stop workers and transformers
    - transformer_heartbeat: Track transformer health

    :param header: Message header (should contain 'run_id')
    :param msg: Message content with format:
                {
                  'msg_type': '<type>',
                  'run_id': 20250914185722,
                  'created_at': datetime,
                  'content': {...}
                }
    :param task_id: Task ID (may be None for run_imminent)
    :param handler_kwargs: Dictionary of handler keyword arguments
    :return: Dictionary with task_id if created
    """
    ret = {}
    msg_type = msg.get("msg_type")
    run_id = msg.get("run_id")

    timetolive = 12 * 3600 * 1000
    panda_attributes = {}

    transformer_broadcaster = handler_kwargs.get("transformer_broadcaster", None)
    worker_publisher = handler_kwargs.get("worker_publisher", None)
    timetolive = handler_kwargs.get("timetolive", timetolive)
    panda_attributes = handler_kwargs.get("panda_attributes", panda_attributes)

    try:
        if msg_type == "run_imminent":
            # Create workflow task and workers
            request_id, transform_id, processing_id, input_coll_id, output_coll_id, workload_id = (
                create_workflow_task(msg, panda_attributes=panda_attributes)
            )
            task_id = workload_id
            create_harvester_worker(
                header,
                msg,
                task_id,
                worker_publisher,
                timetolive=timetolive,
                panda_attributes=panda_attributes,
                logger=logger,
            )
            ret["task_id"] = task_id
            if logger:
                logger.info(f"Handled run_imminent: run_id={run_id}, task_id={task_id}")

        elif msg_type in ["run_end", "run_stop", "end_run"]:
            # Close PanDA task
            ## close_panda_task(task_id, logger=logger)
            # Stop workers and transformers
            stop_harvester_worker(
                header,
                msg,
                task_id,
                worker_publisher,
                transformer_broadcaster,
                timetolive=timetolive,
                panda_attributes=panda_attributes,
                logger=logger,
            )
            if logger:
                logger.info(f"Handled {msg_type}: run_id={run_id}, task_id={task_id}")

        elif msg_type == "transformer_heartbeat":
            transformer_id = msg.get("content", {}).get("id")
            hostname = msg.get("content", {}).get("hostname")
            if logger:
                logger.info(
                    f"Transformer heartbeat: run_id={run_id}, transformer_id={transformer_id}, hostname={hostname}"
                )

        else:
            if logger:
                logger.warning(
                    f"Unknown message type in worker_handler: {msg_type}, run_id={run_id}"
                )

    except Exception as ex:
        if logger:
            logger.error(
                f"Error in worker_handler for msg_type={msg_type}, run_id={run_id}: {ex}",
                exc_info=True,
            )

    return ret
