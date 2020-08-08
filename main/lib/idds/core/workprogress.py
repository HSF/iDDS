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
core operations related to workflow model.
"""

from idds.common import exceptions
from idds.common.constants import RequestStatus, RequestLocking
from idds.orm.base.session import read_session, transactional_session
from idds.orm import workprogress as orm_workprogress


def create_workprogress(request_id, scope, name, priority=0, status=WorkProgressStatus.New, locking=WorkProgressLocking.Idle,
                        expired_at=None, errors=None, workprogress_metadata=None, processing_metadata=None):
    """
    Create a workprogress.

    :param request_id: The request id.
    :param scope: The scope.
    :param name: The name.
    :param status: The status as integer.
    :param locking: The locking as integer.
    :param priority: The priority as integer.
    :param expired_at: The datetime when the workprogress will be expired at.
    :param errors: The errors as a json.
    :param workprogress_metadata: The metadata as json.
    :param processing_metadata: The metadata as json.

    :returns: workprogress.
    """
    return orm_workprogress.create_workprogress(request_id=request_id, scope=scope, name=name, priority=priority, status=status,
                                                locking=locking, expired_at=expired_at,
                                                workprogress_metadata=workprogress_metadata,
                                                processing_metadata=processing_metadata)


@transactional_session
def add_workprogress(request_id, scope, name, priority=0, status=WorkProgressStatus.New, locking=WorkProgressLocking.Idle,
                     expired_at=None, errors=None, workprogress_metadata=None, processing_metadata=None,
                     session=None):
    """
    Add a workprogress.

    :param request_id: The request id.
    :param scope: The scope.
    :param name: The name.
    :param status: The status as integer.
    :param locking: The locking as integer.
    :param priority: The priority as integer.
    :param expired_at: The datetime when the workprogress will be expired at.
    :param errors: The errors as a json.
    :param workprogress_metadata: The metadata as json.
    :param processing_metadata: The metadata as json.

    :raises DuplicatedObject: If a workprogress with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: workprogress id.
    """

    return orm_workprogress.add_workprogress(request_id=request_id, scope=scope, name=name, priority=priority, status=status,
                                             locking=locking, expired_at=expired_at,
                                             workprogress_metadata=workprogress_metadata,
                                             processing_metadata=processing_metadata,
                                             session=session)


@read_session
def get_workprogresses(request_id, to_json=False, session=None):
    """
    Get workprogresses with request_id.

    :param request_id: The request_id of the request.
    :param to_json: Whether to return json format.
    :param session: The database session in use.

    :raises NoObject: If no workprogress is founded.

    :returns: list of workprogresses.
    """

    return orm_workprogress.get_workprogresses(request_id=request_id, to_json=to_json, session=session)


@read_session
def get_workprogress(workprogress_id, to_json=False, session=None):
    """
    Get a workprogress or raise a NoObject exception.

    :param workprogress_id: The id of the workprogress.
    :param to_json: whether to return json format.

    :param session: The database session in use.

    :raises NoObject: If no workprogress is founded.

    :returns: Workprogress.
    """
    return orm_workprogress.get_workprogress(workprogress_id=workprogress_id, to_json=to_json, session=session)


@read_session
def get_workprogresses_by_status(status, time_period=None, locking=False, bulk_size=None, to_json=False, session=None):
    """
    Get workprogresses.

    :param status: list of status of the workprogress data.
    :param locking: Wheter to lock workprogresses to avoid others get the same workprogress.
    :param bulk_size: Size limitation per retrieve.
    :param to_json: whether to return json format.

    :raises NoObject: If no workprogresses are founded.

    :returns: list of Workprogress.
    """

    return orm_workprogress.get_workprogresses_by_status(status=status, time_period=time_period, locking=locking,
                                                         bulk_size=bulk_size, to_json=to_json, session=session)


@transactional_session
def update_workprogress(workprogress_id, parameters, session=None):
    """
    update a workprogress.

    :param workprogress_id: the workprogress id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no workprogress is founded.
    :raises DatabaseException: If there is a database error.

    """

    return orm_workprogress.update_workprogress(workprogress_id=workprogress_id, parameters=parameters, session=session)


@transactional_session
def delete_workprogress(workprogress_id=None, session=None):
    """
    delete a workprogress.

    :param workprogress_id: The id of the workprogress.
    :param session: The database session in use.

    :raises NoObject: If no workprogress is founded.
    :raises DatabaseException: If there is a database error.
    """
    return orm_workprogress.update_workprogress(workprogress_id=workprogress_id, session=session)


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clean locking which is older than time period.

    :param time_period in seconds
    """
    return orm_workprogress.clean_locking(time_period=time_period, session=session)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clean next_poll_at.

    :param status: status of the workprogress
    """
    return orm_workprogress.clean_next_poll_at(status=status, session=session)
