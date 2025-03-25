#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


"""
operations related to Transform.
"""

import datetime

import sqlalchemy
from sqlalchemy import and_, func, select
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql.expression import asc, desc

from idds.common import exceptions
from idds.common.constants import TransformStatus, TransformLocking, CollectionRelationType
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base import models


def create_transform(request_id, workload_id, transform_type, transform_tag=None,
                     priority=0, status=TransformStatus.New, name=None,
                     substatus=TransformStatus.New, locking=TransformLocking.Idle,
                     new_poll_period=1, update_poll_period=10,
                     new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                     parent_transform_id=None, previous_transform_id=None, current_processing_id=None,
                     internal_id=None, has_previous_conditions=None, loop_index=None,
                     cloned_from=None, triggered_conditions=None, untriggered_conditions=None,
                     site=None, retries=0, expired_at=None, transform_metadata=None):
    """
    Create a transform.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param priority: priority.
    :param status: Transform status.
    :param locking: Transform locking.
    :param retries: The number of retries.
    :param expired_at: The datetime when it expires.
    :param transform_metadata: The metadata as json.

    :returns: transform.
    """
    new_transform = models.Transform(request_id=request_id, workload_id=workload_id, transform_type=transform_type,
                                     transform_tag=transform_tag, priority=priority, name=name,
                                     status=status, substatus=substatus, locking=locking,
                                     retries=retries, expired_at=expired_at,
                                     new_retries=new_retries, update_retries=update_retries,
                                     max_new_retries=max_new_retries, max_update_retries=max_update_retries,
                                     parent_transform_id=parent_transform_id,
                                     previous_transform_id=previous_transform_id,
                                     current_processing_id=current_processing_id,
                                     internal_id=internal_id, site=site,
                                     has_previous_conditions=has_previous_conditions,
                                     loop_index=loop_index, cloned_from=cloned_from,
                                     triggered_conditions=triggered_conditions,
                                     untriggered_conditions=untriggered_conditions,
                                     transform_metadata=transform_metadata)
    if new_poll_period:
        new_poll_period = datetime.timedelta(seconds=new_poll_period)
        new_transform.new_poll_period = new_poll_period
    if update_poll_period:
        update_poll_period = datetime.timedelta(seconds=update_poll_period)
        new_transform.update_poll_period = update_poll_period
    return new_transform


@transactional_session
def add_transform(request_id, workload_id, transform_type, transform_tag=None, priority=0, name=None,
                  status=TransformStatus.New, substatus=TransformStatus.New, locking=TransformLocking.Idle,
                  new_poll_period=1, update_poll_period=10, retries=0, expired_at=None,
                  new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                  parent_transform_id=None, previous_transform_id=None, current_processing_id=None,
                  internal_id=None, has_previous_conditions=None, loop_index=None,
                  cloned_from=None, triggered_conditions=None, untriggered_conditions=None,
                  transform_metadata=None, workprogress_id=None, site=None, session=None):
    """
    Add a transform.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param priority: priority.
    :param status: Transform status.
    :param locking: Transform locking.
    :param retries: The number of retries.
    :param expired_at: The datetime when it expires.
    :param transform_metadata: The metadata as json.

    :raises DuplicatedObject: If a transform with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: transform id.
    """
    try:
        new_transform = create_transform(request_id=request_id, workload_id=workload_id, transform_type=transform_type,
                                         transform_tag=transform_tag, priority=priority, name=name,
                                         status=status, substatus=substatus, locking=locking,
                                         retries=retries, expired_at=expired_at,
                                         new_poll_period=new_poll_period,
                                         update_poll_period=update_poll_period,
                                         new_retries=new_retries, update_retries=update_retries,
                                         max_new_retries=max_new_retries, max_update_retries=max_update_retries,
                                         parent_transform_id=parent_transform_id,
                                         previous_transform_id=previous_transform_id,
                                         current_processing_id=current_processing_id,
                                         internal_id=internal_id, site=site,
                                         has_previous_conditions=has_previous_conditions,
                                         loop_index=loop_index, cloned_from=cloned_from,
                                         triggered_conditions=triggered_conditions,
                                         untriggered_conditions=untriggered_conditions,
                                         transform_metadata=transform_metadata)
        new_transform.save(session=session)
        transform_id = new_transform.transform_id

        if workprogress_id:
            new_wp2transform = models.Workprogress2transform(workprogress_id=workprogress_id, transform_id=transform_id)
            new_wp2transform.save(session=session)

        return transform_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Transform already exists!: %s' % (error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@transactional_session
def add_req2transform(request_id, transform_id, session=None):
    """
    Add the relation between request_id and transform_id

    :param request_id: Request id.
    :param transform_id: Transform id.
    :param session: The database session in use.
    """
    try:
        new_req2transform = models.Req2transform(request_id=request_id, transform_id=transform_id)
        new_req2transform.save(session=session)
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Request2Transform already exists!(%s:%s): %s' %
                                          (request_id, transform_id, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@transactional_session
def add_wp2transform(workprogress_id, transform_id, session=None):
    """
    Add the relation between workprogress_id and transform_id

    :param workprogress_id: Workprogress id.
    :param transform_id: Transform id.
    :param session: The database session in use.
    """
    try:
        new_wp2transform = models.Workprogress2transform(workprogress_id=workprogress_id, transform_id=transform_id)
        new_wp2transform.save(session=session)
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Workprogress2Transform already exists!(%s:%s): %s' %
                                          (workprogress_id, transform_id, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_transform(transform_id, request_id=None, to_json=False, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_id: Transform id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transform.
    """

    try:
        query = session.query(models.Transform)\
                       .filter(models.Transform.transform_id == transform_id)
        if request_id:
            query = query.filter(models.Transform.request_id == request_id)
        ret = query.first()
        if not ret:
            return None
        else:
            if to_json:
                return ret.to_dict_json()
            else:
                return ret.to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Transform(transform_id: %s) cannot be found: %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_transform_by_id_status(transform_id, status=None, locking=False, session=None):
    """
    Get a transform or raise a NoObject exception.

    :param transform_id: The id of the transform.
    :param status: request status.
    :param locking: the locking status.

    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Transform.
    """

    try:
        query = select(models.Transform).where(models.Transform.transform_id == transform_id)

        if status:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]
            query = query.where(models.Transform.status.in_(status))

        if locking:
            query = query.where(models.Transform.locking == TransformLocking.Idle)
            query = query.with_for_update(skip_locked=True)

        ret = session.execute(query).fetchone()
        if not ret:
            return None
        else:
            return ret[0].to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('transform transform_id: %s cannot be found: %s' % (transform_id, error))


@read_session
def get_transform_by_name(request_id, name, session=None):
    """
    Get a transform or raise a NoObject exception.

    :param request_id: The request id.
    :param name: transform name.
    :param locking: the locking status.

    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Transform.
    """

    try:
        query = select(models.Transform).where(models.Transform.request_id == request_id)
        query = query.where(models.Transform.name == name)

        ret = session.execute(query).fetchone()
        if not ret:
            return None
        else:
            return ret[0].to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject(f'transform (request_id: {request_id}, name: {name}) cannot be found: {error}')


@read_session
def get_transforms_with_input_collection(transform_type, transform_tag, coll_scope, coll_name, to_json=False, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param coll_scope: The collection scope.
    :param coll_name: The collection name.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transform.
    """

    try:
        subquery = session.query(models.Collection.transform_id)\
                          .filter(models.Collection.scope == coll_scope)\
                          .filter(models.Collection.name == coll_name)\
                          .filter(models.Collection.relation_type == CollectionRelationType.Input)\
                          .subquery()
        query = session.query(models.Transform)\
                       .join(subquery, and_(subquery.c.transform_id == models.Transform.transform_id,
                                            models.Transform.transform_type == transform_type,
                                            models.Transform.transform_tag == transform_tag))
        tmp = query.all()
        rets = []
        if tmp:
            for transf in tmp:
                if to_json:
                    rets.append(transf.to_dict_json())
                else:
                    rets.append(transf.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Transform(transform_type: %s, transform_tag: %s, coll_scope: %s, coll_name: %s) cannot be found: %s' %
                                  (transform_type, transform_tag, coll_scope, coll_name, error))
    except Exception as error:
        raise error


@read_session
def get_transform_ids(workprogress_id=None, request_id=None, workload_id=None, transform_id=None, session=None):
    """
    Get transform ids or raise a NoObject exception.

    :param workprogress_id: Workprogress id.
    :param workload_id: Workload id.
    :param transform_id: Transform id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform ids.
    """
    try:
        query = session.query(models.Transform.transform_id)
        if request_id:
            query = query.filter(models.Transform.request_id == request_id)
        if workload_id:
            query = query.filter(models.Transform.workload_id == workload_id)
        if transform_id:
            query = query.filter(models.Transform.transform_id == transform_id)
        if workprogress_id:
            query = query.join(models.Workprogress2transform, and_(models.Workprogress2transform.workprogress_id == workprogress_id))

        tmp = query.all()
        ret_ids = []
        if tmp:
            for t in tmp:
                ret_ids.append(t[0])
        return ret_ids
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No transforms attached with request id (%s) and transform_id (%s): %s' %
                                  (request_id, transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_transforms(request_id=None, workload_id=None, transform_id=None, loop_index=None, internal_ids=None,
                   to_json=False, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param request_id: Request id.
    :param workload_id: Workload id.
    :param transform_id: Transform id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transforms.
    """
    try:
        query = session.query(models.Transform)
        if request_id:
            query = query.filter(models.Transform.request_id == request_id)
        if workload_id:
            query = query.filter(models.Transform.workload_id == workload_id)
        if transform_id:
            query = query.filter(models.Transform.transform_id == transform_id)
        if loop_index is not None:
            query = query.filter(models.Transform.loop_index == loop_index)
        if internal_ids:
            if not isinstance(internal_ids, (list, tuple)):
                internal_ids = [internal_ids]
            if len(internal_ids) == 1:
                internal_ids = [internal_ids[0], internal_ids[0]]
            query = query.filter(models.Transform.internal_ids.in_(internal_ids))

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
        raise exceptions.NoObject('No transforms attached with request id (%s): %s' %
                                  (request_id, error))
    except Exception as error:
        raise error


@transactional_session
def get_transforms_by_status(status, period=None, transform_ids=[], locking=False, locking_for_update=False,
                             bulk_size=None, to_json=False, by_substatus=False, only_return_id=False,
                             order_by_fifo=False, min_request_id=None, new_poll=False, update_poll=False, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param status: Transform status or list of transform status.
    :param period: Time period in seconds.
    :param locking: Whether to retrieved unlocked items.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform.
    """
    try:
        if status:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]

        if only_return_id:
            query = session.query(models.Transform.transform_id)
        else:
            query = session.query(models.Transform)

        if status:
            if by_substatus:
                query = query.filter(models.Transform.substatus.in_(status))
            else:
                query = query.filter(models.Transform.status.in_(status))
        if new_poll:
            query = query.filter(models.Transform.updated_at + models.Transform.new_poll_period <= datetime.datetime.utcnow())
        if update_poll:
            query = query.filter(models.Transform.updated_at + models.Transform.update_poll_period <= datetime.datetime.utcnow())

        if transform_ids:
            query = query.filter(models.Transform.transform_id.in_(transform_ids))
        if min_request_id:
            query = query.filter(models.Transform.request_id >= min_request_id)
        # if period:
        #     query = query.filter(models.Transform.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=period))
        if locking:
            query = query.filter(models.Transform.locking == TransformLocking.Idle)

        if locking_for_update:
            query = query.with_for_update(skip_locked=True)
        else:
            if order_by_fifo:
                query = query.order_by(desc(models.Transform.priority)).order_by(asc(models.Transform.transform_id))
            else:
                query = query.order_by(asc(models.Transform.updated_at)).order_by(desc(models.Transform.priority))

        if bulk_size:
            query = query.limit(bulk_size)

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if only_return_id:
                    rets.append(t[0])
                else:
                    if to_json:
                        rets.append(t.to_dict_json())
                    else:
                        rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No transforms attached with status (%s): %s' %
                                  (status, error))
    except Exception as error:
        raise error


@transactional_session
def update_transform(transform_id, parameters, locking=False, session=None):
    """
    update a transform.

    :param transform_id: the transform id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        parameters['updated_at'] = datetime.datetime.utcnow()

        if 'new_poll_period' in parameters and type(parameters['new_poll_period']) not in [datetime.timedelta]:
            parameters['new_poll_period'] = datetime.timedelta(seconds=parameters['new_poll_period'])
        if 'update_poll_period' in parameters and type(parameters['update_poll_period']) not in [datetime.timedelta]:
            parameters['update_poll_period'] = datetime.timedelta(seconds=parameters['update_poll_period'])

        if 'status' in parameters and parameters['status'] in [TransformStatus.Finished, TransformStatus.Finished.value,
                                                               TransformStatus.Failed, TransformStatus.Failed.value]:
            parameters['finished_at'] = datetime.datetime.utcnow()

        if 'transform_metadata' in parameters and 'work' in parameters['transform_metadata']:
            work = parameters['transform_metadata']['work']
            if work is not None:
                if hasattr(work, 'refresh_work'):
                    work.refresh_work()
                if 'running_metadata' not in parameters:
                    parameters['running_metadata'] = {}
                parameters['running_metadata']['work_data'] = work.metadata
        if 'transform_metadata' in parameters:
            del parameters['transform_metadata']
        if 'running_metadata' in parameters:
            parameters['_running_metadata'] = parameters['running_metadata']
            del parameters['running_metadata']

        query = session.query(models.Transform).filter_by(transform_id=transform_id)
        if locking:
            query = query.filter(models.Transform.locking == TransformLocking.Idle)
            query = query.with_for_update(skip_locked=True)

        num_rows = query.update(parameters, synchronize_session=False)
        return num_rows
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Transfrom %s cannot be found: %s' % (transform_id, error))
    return 0


@transactional_session
def delete_transform(transform_id=None, session=None):
    """
    delete a transform.

    :param transform_id: The id of the transform.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        session.query(models.Req2transform).filter_by(transform_id=transform_id).delete()
        session.query(models.Transform).filter_by(transform_id=transform_id).delete()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Transfrom %s cannot be found: %s' % (transform_id, error))


@transactional_session
def clean_locking(time_period=3600, min_request_id=None, health_items=[], session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    health_dict = {}
    for item in health_items:
        hostname = item['hostname']
        pid = item['pid']
        thread_id = item['thread_id']
        if hostname not in health_dict:
            health_dict[hostname] = {}
        if pid not in health_dict[hostname]:
            health_dict[hostname][pid] = []
        if thread_id not in health_dict[hostname][pid]:
            health_dict[hostname][pid].append(thread_id)
    query = session.query(models.Transform.transform_id,
                          models.Transform.locking_hostname,
                          models.Transform.locking_pid,
                          models.Transform.locking_thread_id,
                          models.Transform.locking_thread_name)
    query = query.filter(models.Transform.locking == TransformLocking.Locking)
    if min_request_id:
        query = query.filter(models.Transform.request_id >= min_request_id)

    lost_transform_ids = []
    tmp = query.all()
    if tmp:
        for req in tmp:
            tf_id, locking_hostname, locking_pid, locking_thread_id, locking_thread_name = req
            if locking_hostname not in health_dict or locking_pid not in health_dict[locking_hostname]:
                lost_transform_ids.append({"transform_id": tf_id, 'locking': 0})

    session.bulk_update_mappings(models.Transform, lost_transform_ids)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the transform
    """
    if not isinstance(status, (list, tuple)):
        status = [status]
    if len(status) == 1:
        status = [status[0], status[0]]

    params = {'next_poll_at': datetime.datetime.utcnow()}
    session.query(models.Transform).filter(models.Transform.status.in_(status))\
           .update(params, synchronize_session=False)


@read_session
def get_num_active_transforms(active_status=None, session=None):
    if active_status and not isinstance(active_status, (list, tuple)):
        active_status = [active_status]
    if active_status and len(active_status) == 1:
        active_status = [active_status[0], active_status[0]]

    try:
        query = session.query(models.Transform.status, models.Transform.site, func.count(models.Transform.transform_id))
        if active_status:
            query = query.filter(models.Transform.status.in_(active_status))
        query = query.group_by(models.Transform.status, models.Transform.site)
        tmp = query.all()
        return tmp
    except Exception as error:
        raise error


@read_session
def get_active_transforms(active_status=None, session=None):
    if active_status and not isinstance(active_status, (list, tuple)):
        active_status = [active_status]
    if active_status and len(active_status) == 1:
        active_status = [active_status[0], active_status[0]]

    try:
        query = session.query(models.Transform.request_id,
                              models.Transform.transform_id,
                              models.Transform.site,
                              models.Transform.status)
        if active_status:
            query = query.filter(models.Transform.status.in_(active_status))
        tmp = query.all()
        return tmp
    except Exception as error:
        raise error
