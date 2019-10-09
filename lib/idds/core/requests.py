#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
operations related to Requests.
"""


import datetime

from idds.common import exceptions
from idds.common.constants import RequestStatus, RequestType
from idds.orm.base.session import transactional_session
from idds.orm import requests


@transactional_session
def add_request(scope, name, requester=None, request_type=None, transform_tag=None,
                status=None, priority=0, lifetime=30, request_metadata=None, session=None):
    """
    Add a request.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param requestr: The requester, such as panda, user and so on.
    :param request_type: The type of the request, such as ESS, DAOD.
    :param transform_tag: Transform tag, such as ATLAS AMI tag.
    :param status: The request status as integer.
    :param priority: The priority as integer.
    :param lifetime: The life time as umber of days.
    :param request_metadata: The metadata as json.

    :returns: request id.
    """
    kwargs = {'scope': scope, 'name': name, 'requester': requester, 'request_type': request_type,
              'transform_tag': transform_tag, 'status': status, 'priority': priority,
              'lifetime': lifetime, 'request_metadata': request_metadata, 'session': session}

    if request_metadata and 'workload_id' in request_metadata:
        try:
            req = requests.get_request(workload_id=request_metadata['workload_id'], session=session)
            if is_same_request(kwargs, req):
                # updateexpired_at time and status
                if req['status'] in [RequestStatus.New, RequestStatus.Transforming, RequestStatus.Transporting,
                                     RequestStatus.Processing]:
                    new_status = req['status']
                else:
                    new_status = RequestStatus.ReQueue
                update_paramesters = {'status': new_status, 'priority': priority,
                                      'expired_at': datetime.datetime.utcnow() + datetime.timedelta(days=lifetime)}
                requests.update_request(req['requestid'], update_paramesters, session=session)
                return req['requestid']
            else:
                errmsg = "There is already a different request(%s) with the same workload id(%s)" % (req['requestid'],
                                                                                                     request_metadata['workload_id'])
                raise exceptions.ConflictRequestException(errmsg)
        except exceptions.NoObject:
            return requests.add_request(**kwargs)
    else:
        return requests.add_request(**kwargs)


def is_same_request(new_req, req):
    new_request_type = new_req['request_type']
    request_type = req['request_type']
    if isinstance(new_request_type, RequestType):
        new_request_type = new_request_type.value
    if isinstance(request_type, RequestType):
        request_type = request_type.value

    if (new_req['scope'] == req['scope'] and new_req['name'] == req['name']
        and new_req['transform_tag'] == req['transform_tag']   # noqa: W503
        and new_request_type == request_type):                 # noqa: W503
        return True
    return False


def get_request(request_id=None, workload_id=None):
    """
    Get a request or raise a NoObject exception.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.

    :raises NoObject: If no request is founded.

    :returns: Request.
    """
    return requests.get_request(request_id=request_id, workload_id=workload_id)


def extend_request(request_id=None, workload_id=None, lifetime=30):
    """
    extend an request's lifetime.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param lifetime: The life time as umber of days.
    """
    return requests.extend_request(request_id=request_id, workload_id=workload_id, lifetime=lifetime)


def cancel_request(request_id=None, workload_id=None):
    """
    cancel an request.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    """
    return requests.cancel_request(request_id=request_id, workload_id=workload_id)


def update_request(request_id, parameters):
    """
    update an request.

    :param request_id: the request id.
    :param parameters: A dictionary of parameters.
    """
    return requests.update_request(request_id, parameters)
