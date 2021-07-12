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
operations related to workflow model.
"""

import datetime

import sqlalchemy
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql.expression import asc, desc

from idds.common import exceptions
from idds.common.constants import WorkprogressStatus, WorkprogressLocking
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base import models


def create_workprogress(request_id, workload_id, scope, name, priority=0, status=WorkprogressStatus.New,
                        locking=WorkprogressLocking.Idle, expired_at=None, errors=None,
                        workprogress_metadata=None, processing_metadata=None):
    """
    Create a workprogress.

    :param request_id: The request id.
    :param workload_id: The workload id.
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
    new_wp = models.WorkProgress(request_id=request_id, workload_id=workload_id, scope=scope, name=name,
                                 priority=priority, status=status,
                                 locking=locking, expired_at=expired_at,
                                 workprogress_metadata=workprogress_metadata,
                                 processing_metadata=processing_metadata)
    return new_wp


@transactional_session
def add_workprogress(request_id, workload_id, scope, name, priority=0, status=WorkprogressStatus.New,
                     locking=WorkprogressLocking.Idle,
                     expired_at=None, errors=None, workprogress_metadata=None, processing_metadata=None,
                     session=None):
    """
    Add a workprogress.

    :param request_id: The request id.
    :param workload_id: The workload id.
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

    try:
        new_wp = create_workprogress(request_id=request_id, workload_id=workload_id, scope=scope, name=name,
                                     priority=priority, status=status,
                                     locking=locking, expired_at=expired_at,
                                     workprogress_metadata=workprogress_metadata,
                                     processing_metadata=processing_metadata)
        new_wp.save(session=session)
        wp_id = new_wp.workprogress_id
        return wp_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('workprogress %s already exists!: %s' % (new_wp, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@transactional_session
def add_workprogresses(workprogresses, bulk_size=1000, session=None):
    """
    Add workprogresses.

    :param workprogresses: dict of workprogress.
    :param session: session.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: workprogress ids.
    """
    sub_params = [workprogresses[i:i + bulk_size] for i in range(0, len(workprogresses), bulk_size)]

    try:
        for sub_param in sub_params:
            session.bulk_insert_mappings(models.Workprogress, sub_param)
        wp_ids = [None for _ in range(len(workprogresses))]
        return wp_ids
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Duplicated objects: %s' % (error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_workprogresses(request_id=None, to_json=False, session=None):
    """
    Get workprogresses with request_id.

    :param request_id: The request_id of the request.
    :param to_json: Whether to return json format.
    :param session: The database session in use.

    :raises NoObject: If no workprogress is founded.

    :returns: list of workprogresses.
    """

    try:
        query = session.query(models.Workprogress)\
                       .with_hint(models.Workprogress, "INDEX(WORKPROGRESSES WORKPROGRESS_PK)", 'oracle')
        if request_id is not None:
            query = query.filter(models.Workprogress.request_id == request_id)
        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('workprogress with request_id:%s cannot be found: %s' % (request_id, error))


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

    try:
        query = session.query(models.Workprogress)\
                       .with_hint(models.Workprogress, "INDEX(WORKPROGRESSES WORKPROGRESS_PK)", 'oracle')\
                       .filter(models.Workprogress.workprogress_id == workprogress_id)

        ret = query.first()
        if not ret:
            return None
        else:
            if to_json:
                return ret.to_dict_json()
            else:
                return ret.to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('workprogress workprogress_id: %s cannot be found: %s' % (workprogress_id, error))


@transactional_session
def get_workprogresses_by_status(status, period=None, locking=False, bulk_size=None, to_json=False, session=None):
    """
    Get workprogresses.

    :param status: list of status of the workprogress data.
    :param locking: Wheter to lock workprogresses to avoid others get the same workprogress.
    :param bulk_size: Size limitation per retrieve.
    :param to_json: whether to return json format.

    :raises NoObject: If no workprogresses are founded.

    :returns: list of Workprogress.
    """

    try:
        if status is None:
            raise exceptions.WrongParameterException("status should not be None")
        if not isinstance(status, (list, tuple)):
            status = [status]
        if len(status) == 1:
            status = [status[0], status[0]]

        query = session.query(models.Workprogress)\
                       .with_hint(models.Workprogress, "INDEX(WORKPROGRESSES WORKPROGRESS_STATUS_PRIO_IDX)", 'oracle')\
                       .filter(models.Workprogress.status.in_(status))\
                       .filter(models.Workprogress.next_poll_at < datetime.datetime.utcnow())

        if period is not None:
            query = query.filter(models.Workprogress.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=period))
        if locking:
            query = query.filter(models.Workprogress.locking == WorkprogressLocking.Idle)
        query = query.order_by(asc(models.Workprogress.updated_at))\
                     .order_by(desc(models.Workprogress.priority))
        if bulk_size:
            query = query.limit(bulk_size)

        if locking:
            query = query.with_for_update(nowait=True, skip_locked=True)

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No workprogresses with status: %s, period: %s, locking: %s, %s' % (status, period, locking, error))


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
    try:
        parameters['updated_at'] = datetime.datetime.utcnow()

        session.query(models.Workprogress).filter_by(workprogress_id=workprogress_id)\
               .update(parameters, synchronize_session=False)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Workprogress %s cannot be found: %s' % (workprogress_id, error))


@transactional_session
def delete_workprogress(workprogress_id=None, session=None):
    """
    delete a workprogress.

    :param workprogress_id: The id of the workprogress.
    :param session: The database session in use.

    :raises NoObject: If no workprogress is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        session.query(models.Workprogress).filter_by(workprogress_id=workprogress_id).delete()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Workprogress (workprogress_id: %s) cannot be found: %s' % (workprogress_id, error))


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clean locking which is older than time period.

    :param time_period in seconds
    """

    params = {'locking': 0}
    session.query(models.Workprogress).filter(models.Workprogress.locking == WorkprogressLocking.Locking)\
           .filter(models.Workprogress.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period))\
           .update(params, synchronize_session=False)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clean next_poll_at.

    :param status: status of the workprogress
    """
    if not isinstance(status, (list, tuple)):
        status = [status]
    if len(status) == 1:
        status = [status[0], status[0]]

    params = {'next_poll_at': datetime.datetime.utcnow()}
    session.query(models.Workprogress).filter(models.Workprogress.status.in_(status))\
           .update(params, synchronize_session=False)
