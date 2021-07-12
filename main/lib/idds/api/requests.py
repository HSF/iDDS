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

from idds.common.constants import RequestStatus

from idds.core import requests


def add_request(scope, name, requester=None, request_type=None, transform_tag=None, workload_id=None,
                status=RequestStatus.New, priority=0, lifetime=None, request_metadata=None):
    """
    Add a request.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param requestr: The requester, such as panda, user and so on.
    :param request_type: The type of the request, such as ESS, DAOD.
    :param transform_tag: Transform tag, such as ATLAS AMI tag.
    :param workload_id: The external workload id.
    :param status: The request status as integer.
    :param priority: The priority as integer.
    :param lifetime: The life time as umber of days.
    :param request_metadata: The metadata as json.

    :returns: request id.
    """
    kwargs = {'scope': scope, 'name': name, 'requester': requester, 'request_type': request_type,
              'transform_tag': transform_tag, 'status': status, 'priority': priority,
              'workload_id': workload_id, 'lifetime': lifetime, 'request_metadata': request_metadata}
    return requests.add_request(**kwargs)


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
