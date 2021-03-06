#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


"""
operations related to Requests.
"""


from idds.common.constants import RequestStatus, RequestLocking, WorkStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm import requests as orm_requests
from idds.orm import transforms as orm_transforms
from idds.orm import workprogress as orm_workprogresses
# from idds.atlas.worflow.utils import convert_request_metadata_to_workflow


def create_request(scope=None, name=None, requester=None, request_type=None, transform_tag=None,
                   status=RequestStatus.New, locking=RequestLocking.Idle, priority=0,
                   lifetime=30, workload_id=None, request_metadata=None,
                   processing_metadata=None):
    """
    Add a request.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param requestr: The requester, such as panda, user and so on.
    :param request_type: The type of the request, such as ESS, DAOD.
    :param transform_tag: Transform tag, such as ATLAS AMI tag.
    :param status: The request status as integer.
    :param locking: The request locking as integer.
    :param priority: The priority as integer.
    :param lifetime: The life time as umber of days.
    :param workload_id: The external workload id.
    :param request_metadata: The metadata as json.
    :param processing_metadata: The metadata as json.

    :returns: request id.
    """
    if workload_id is None and request_metadata and 'workload_id' in request_metadata:
        workload_id = int(request_metadata['workload_id'])

    # request_metadata = convert_request_metadata_to_workflow(scope, name, workload_id, request_type, request_metadata)
    kwargs = {'scope': scope, 'name': name, 'requester': requester, 'request_type': request_type,
              'transform_tag': transform_tag, 'status': status, 'locking': locking,
              'priority': priority, 'lifetime': lifetime, 'workload_id': workload_id,
              'request_metadata': request_metadata, 'processing_metadata': processing_metadata}
    return orm_requests.create_request(**kwargs)


@transactional_session
def add_request(scope=None, name=None, requester=None, request_type=None, transform_tag=None,
                status=RequestStatus.New, locking=RequestLocking.Idle, priority=0,
                lifetime=30, workload_id=None, request_metadata=None,
                processing_metadata=None, session=None):
    """
    Add a request.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param requestr: The requester, such as panda, user and so on.
    :param request_type: The type of the request, such as ESS, DAOD.
    :param transform_tag: Transform tag, such as ATLAS AMI tag.
    :param status: The request status as integer.
    :param locking: The request locking as integer.
    :param priority: The priority as integer.
    :param lifetime: The life time as umber of days.
    :param workload_id: The external workload id.
    :param request_metadata: The metadata as json.
    :param processing_metadata: The metadata as json.

    :returns: request id.
    """
    if workload_id is None and request_metadata and 'workload_id' in request_metadata:
        workload_id = int(request_metadata['workload_id'])
    # request_metadata = convert_request_metadata_to_workflow(scope, name, workload_id, request_type, request_metadata)

    kwargs = {'scope': scope, 'name': name, 'requester': requester, 'request_type': request_type,
              'transform_tag': transform_tag, 'status': status, 'locking': locking,
              'priority': priority, 'lifetime': lifetime, 'workload_id': workload_id,
              'request_metadata': request_metadata, 'processing_metadata': processing_metadata,
              'session': session}
    return orm_requests.add_request(**kwargs)


@read_session
def get_request_ids_by_workload_id(workload_id, session=None):
    """
    Get request id or raise a NoObject exception.

    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request ids.
    """
    return orm_requests.get_request_ids_by_workload_id(workload_id, session=session)


@read_session
def get_requests(request_id=None, workload_id=None, with_detail=False, to_json=False, session=None):
    """
    Get a request or raise a NoObject exception.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param to_json: return json format.

    :raises NoObject: If no request is founded.

    :returns: Request.
    """
    return orm_requests.get_requests(request_id=request_id, workload_id=workload_id,
                                     with_detail=with_detail, to_json=to_json, session=session)


@transactional_session
def extend_requests(request_id=None, workload_id=None, lifetime=30, session=None):
    """
    extend an request's lifetime.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param lifetime: The life time as umber of days.
    """
    return orm_requests.extend_request(request_id=request_id, workload_id=workload_id, lifetime=lifetime,
                                       session=session)


@transactional_session
def cancel_requests(request_id=None, workload_id=None, session=None):
    """
    cancel an request.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    """
    return orm_requests.cancel_request(request_id=request_id, workload_id=workload_id, session=session)


@transactional_session
def update_request(request_id, parameters, session=None):
    """
    update an request.

    :param request_id: the request id.
    :param parameters: A dictionary of parameters.
    """
    return orm_requests.update_request(request_id, parameters, session=session)


@transactional_session
def update_request_with_transforms(request_id, parameters, new_transforms=None, update_transforms=None, session=None):
    """
    update an request.

    :param request_id: the request id.
    :param parameters: A dictionary of parameters.
    :param new_transforms: list of transforms
    :param update_transforms: list of transforms
    """
    if new_transforms:
        for tf in new_transforms:
            # tf_id = orm_transforms.add_transform(**tf, session=session)
            original_work = tf['transform_metadata']['original_work']
            del tf['transform_metadata']['original_work']
            tf_id = orm_transforms.add_transform(**tf, session=session)

            # work = tf['transform_metadata']['work']
            original_work.set_work_id(tf_id, transforming=True)
            original_work.set_status(WorkStatus.New)
    if update_transforms:
        for tr_id in update_transforms:
            orm_transforms.update_transform(transform_id=tr_id, parameters=update_transforms[tr_id], session=session)
    return orm_requests.update_request(request_id, parameters, session=session)


@transactional_session
def update_request_with_workprogresses(request_id, parameters, new_workprogresses=None, update_workprogresses=None, session=None):
    """
    update an request.

    :param request_id: the request id.
    :param parameters: A dictionary of parameters.
    :param new_workprogresses: list of new workprogresses.
    """
    if new_workprogresses:
        orm_workprogresses.add_workprogresses(new_workprogresses, session=session)
    if update_workprogresses:
        for workprogress_id in update_workprogresses:
            orm_workprogresses.update_workprogress(workprogress_id, update_workprogresses[workprogress_id], session=session)
    return orm_requests.update_request(request_id, parameters, session=session)


@transactional_session
def get_requests_by_status_type(status, request_type=None, time_period=None, locking=False, bulk_size=None, to_json=False, by_substatus=False, session=None):
    """
    Get requests by status and type

    :param status: list of status of the request data.
    :param request_type: The type of the request data.
    :param time_period: Delay of seconds before last update.
    :param locking: Wheter to lock requests to avoid others get the same request.
    :param bulk_size: Size limitation per retrieve.
    :param to_json: return json format.

    :returns: list of Request.
    """
    reqs = orm_requests.get_requests_by_status_type(status, request_type, time_period, locking=locking, bulk_size=bulk_size,
                                                    to_json=to_json, by_substatus=by_substatus, session=session)
    if locking:
        parameters = {'locking': RequestLocking.Locking}
        for req in reqs:
            orm_requests.update_request(request_id=req['request_id'], parameters=parameters, session=session)
    return reqs


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    orm_requests.clean_locking(time_period=time_period, session=session)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the request
    """
    orm_requests.clean_next_poll_at(status=status, session=session)
