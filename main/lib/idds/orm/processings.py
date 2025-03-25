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
operations related to Processings.
"""

import datetime

import sqlalchemy
from sqlalchemy import func, select
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql.expression import asc

from idds.common import exceptions
from idds.common.constants import ProcessingType, ProcessingStatus, ProcessingLocking, GranularityType
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base import models


def create_processing(request_id, workload_id, transform_id, status=ProcessingStatus.New, locking=ProcessingLocking.Idle, submitter=None,
                      granularity=None, granularity_type=GranularityType.File, expired_at=None, processing_metadata=None,
                      new_poll_period=1, update_poll_period=10, processing_type=ProcessingType.Workflow,
                      new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                      substatus=ProcessingStatus.New, output_metadata=None):
    """
    Create a processing.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: Transform id.
    :param status: processing status.
    :param locking: processing locking.
    :param submitter: submitter name.
    :param granularity: Granularity size.
    :param granularity_type: Granularity type.
    :param expired_at: The datetime when it expires.
    :param processing_metadata: The metadata as json.

    :returns: processing.
    """
    new_processing = models.Processing(request_id=request_id, workload_id=workload_id, transform_id=transform_id,
                                       status=status, substatus=substatus, locking=locking,
                                       submitter=submitter, granularity=granularity, granularity_type=granularity_type,
                                       expired_at=expired_at, processing_metadata=processing_metadata,
                                       new_retries=new_retries, update_retries=update_retries,
                                       processing_type=processing_type,
                                       max_new_retries=max_new_retries, max_update_retries=max_update_retries,
                                       output_metadata=output_metadata)

    if new_poll_period:
        new_poll_period = datetime.timedelta(seconds=new_poll_period)
        new_processing.new_poll_period = new_poll_period
    if update_poll_period:
        update_poll_period = datetime.timedelta(seconds=update_poll_period)
        new_processing.update_poll_period = update_poll_period
    return new_processing


@transactional_session
def add_processing(request_id, workload_id, transform_id, status=ProcessingStatus.New,
                   locking=ProcessingLocking.Idle, submitter=None, substatus=ProcessingStatus.New,
                   granularity=None, granularity_type=GranularityType.File, expired_at=None,
                   processing_metadata=None, new_poll_period=1, update_poll_period=10,
                   processing_type=ProcessingType.Workflow,
                   new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                   output_metadata=None, session=None):
    """
    Add a processing.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: Transform id.
    :param status: processing status.
    :param locking: processing locking.
    :param submitter: submitter name.
    :param granularity: Granularity size.
    :param granularity_type: Granularity type.
    :param expired_at: The datetime when it expires.
    :param processing_metadata: The metadata as json.

    :raises DuplicatedObject: If a processing with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: processing id.
    """
    try:
        new_processing = create_processing(request_id=request_id, workload_id=workload_id, transform_id=transform_id,
                                           status=status, substatus=substatus, locking=locking, submitter=submitter,
                                           granularity=granularity, granularity_type=granularity_type,
                                           expired_at=expired_at, new_poll_period=new_poll_period,
                                           update_poll_period=update_poll_period, processing_type=processing_type,
                                           new_retries=new_retries, update_retries=update_retries,
                                           max_new_retries=max_new_retries, max_update_retries=max_update_retries,
                                           processing_metadata=processing_metadata, output_metadata=output_metadata)
        new_processing.save(session=session)
        proc_id = new_processing.processing_id
        return proc_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Processing already exists!: %s' % (error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_processing(processing_id, request_id=None, transform_id=None, to_json=False, session=None):
    """
    Get processing or raise a NoObject exception.

    :param processing_id: Processing id.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processing.
    """

    try:
        query = session.query(models.Processing)\
                       .filter_by(processing_id=processing_id)
        if request_id is not None:
            query = query.filter_by(request_id=request_id)
        if transform_id is not None:
            query = query.filter_by(transform_id=transform_id)

        ret = query.first()
        if not ret:
            return None
        else:
            if to_json:
                return ret.to_dict_json()
            else:
                return ret.to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processing(processing_id: %s) cannot be found: %s' %
                                  (processing_id, error))
    except Exception as error:
        raise error


@read_session
def get_processing_by_id_status(processing_id, status=None, locking=False, session=None):
    """
    Get a processing or raise a NoObject exception.

    :param processing_id: The id of the processing.
    :param status: request status.
    :param locking: the locking status.

    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Processing.
    """

    try:
        query = select(models.Processing).filter(models.Processing.processing_id == processing_id)

        if status:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]
            query = query.where(models.Processing.status.in_(status))

        if locking:
            query = query.where(models.Processing.locking == ProcessingLocking.Idle)
            query = query.with_for_update(skip_locked=True)

        ret = session.execute(query).fetchone()
        if not ret:
            return None
        else:
            return ret[0].to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('processing processing_id: %s cannot be found: %s' % (processing_id, error))


@read_session
def get_processings(request_id=None, workload_id=None, transform_id=None, to_json=False, session=None):
    """
    Get processing or raise a NoObject exception.

    :param processing_id: Processing id.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processing.
    """

    try:
        query = session.query(models.Processing)

        if request_id:
            query = query.filter(models.Processing.request_id == request_id)
        if workload_id:
            query = query.filter(models.Processing.workload_id == workload_id)
        if transform_id:
            query = query.filter(models.Processing.transform_id == transform_id)

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
        raise exceptions.NoObject('Processing(request_id: %s, workload_id: %s, transform_id: %s) cannot be found: %s' %
                                  (request_id, workload_id, transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_processings_by_transform_id(transform_id=None, to_json=False, session=None):
    """
    Get processings or raise a NoObject exception.

    :param tranform_id: Transform id.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """

    try:
        query = session.query(models.Processing)\
                       .filter_by(transform_id=transform_id)
        query = query.order_by(asc(models.Processing.processing_id))

        ret = query.all()
        if not ret:
            return []
        else:
            items = []
            for t in ret:
                if to_json:
                    items.append(t.to_dict_json())
                else:
                    items.append(t.to_dict())
            return items
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processings(transform_id: %s) cannot be found: %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@transactional_session
def get_processings_by_status(status, period=None, processing_ids=[], locking=False, locking_for_update=False,
                              bulk_size=None, submitter=None, to_json=False, by_substatus=False, only_return_id=False,
                              min_request_id=None, new_poll=False, update_poll=False, for_poller=False, session=None):
    """
    Get processing or raise a NoObject exception.

    :param status: Processing status of list of processing status.
    :param period: Time period in seconds.
    :param locking: Whether to retrieve only unlocked items.
    :param bulk_size: bulk size limitation.
    :param submitter: The submitter name.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """

    try:
        if status:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]

        if only_return_id:
            query = session.query(models.Processing.processing_id)
        else:
            query = session.query(models.Processing)

        if status:
            if by_substatus:
                query = query.filter(models.Processing.substatus.in_(status))
            else:
                query = query.filter(models.Processing.status.in_(status))
        if new_poll:
            query = query.filter(models.Processing.updated_at + models.Processing.new_poll_period <= datetime.datetime.utcnow())
        if update_poll:
            query = query.filter(models.Processing.updated_at + models.Processing.update_poll_period <= datetime.datetime.utcnow())

        if processing_ids:
            query = query.filter(models.Processing.processing_id.in_(processing_ids))
        if min_request_id:
            query = query.filter(models.Processing.request_id >= min_request_id)
        # if period:
        #     query = query.filter(models.Processing.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=period))
        if locking:
            query = query.filter(models.Processing.locking == ProcessingLocking.Idle)
        if submitter:
            query = query.filter(models.Processing.submitter == submitter)

        # if for_poller:
        #     query = query.order_by(asc(models.Processing.poller_updated_at))
        if locking_for_update:
            query = query.with_for_update(skip_locked=True)
        else:
            query = query.order_by(asc(models.Processing.updated_at))

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
        raise exceptions.NoObject('No processing attached with status (%s): %s' % (status, error))
    except Exception as error:
        raise error


@transactional_session
def update_processing(processing_id, parameters, locking=False, session=None):
    """
    update a processing.

    :param processing_id: the transform id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        if 'new_poll_period' in parameters and type(parameters['new_poll_period']) not in [datetime.timedelta]:
            parameters['new_poll_period'] = datetime.timedelta(seconds=parameters['new_poll_period'])
        if 'update_poll_period' in parameters and type(parameters['update_poll_period']) not in [datetime.timedelta]:
            parameters['update_poll_period'] = datetime.timedelta(seconds=parameters['update_poll_period'])

        parameters['updated_at'] = datetime.datetime.utcnow()
        if 'status' in parameters and parameters['status'] in [ProcessingStatus.Finished, ProcessingStatus.Failed,
                                                               ProcessingStatus.Lost]:
            parameters['finished_at'] = datetime.datetime.utcnow()

        if parameters and 'processing_metadata' in parameters and 'processing' in parameters['processing_metadata']:
            proc = parameters['processing_metadata']['processing']
            if proc is not None:
                if 'running_metadata' not in parameters:
                    parameters['running_metadata'] = {}
                parameters['running_metadata']['processing_data'] = proc.metadata
        if parameters and 'processing_metadata' in parameters:
            del parameters['processing_metadata']
        if parameters and 'running_metadata' in parameters:
            parameters['_running_metadata'] = parameters['running_metadata']
            del parameters['running_metadata']

        query = session.query(models.Processing).filter_by(processing_id=processing_id)
        if locking:
            query = query.filter(models.Processing.locking == ProcessingLocking.Idle)
            query = query.with_for_update(skip_locked=True)

        num_rows = query.update(parameters, synchronize_session=False)
        return num_rows
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processing %s cannot be found: %s' % (processing_id, error))
    return 0


@transactional_session
def delete_processing(processing_id=None, session=None):
    """
    delete a processing.

    :param processing_id: The id of the processing.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        session.query(models.Processing).filter_by(processing_id=processing_id).delete()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processing %s cannot be found: %s' % (processing_id, error))


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
    query = session.query(models.Processing.processing_id,
                          models.Processing.locking_hostname,
                          models.Processing.locking_pid,
                          models.Processing.locking_thread_id,
                          models.Processing.locking_thread_name)
    query = query.filter(models.Processing.locking == ProcessingLocking.Locking)
    if min_request_id:
        query = query.filter(models.Processing.request_id >= min_request_id)

    lost_processing_ids = []
    tmp = query.all()
    if tmp:
        for req in tmp:
            pr_id, locking_hostname, locking_pid, locking_thread_id, locking_thread_name = req
            if locking_hostname not in health_dict or locking_pid not in health_dict[locking_hostname]:
                lost_processing_ids.append({"processing_id": pr_id, 'locking': 0})

    session.bulk_update_mappings(models.Processing, lost_processing_ids)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the processing
    """
    if not isinstance(status, (list, tuple)):
        status = [status]
    if len(status) == 1:
        status = [status[0], status[0]]

    params = {'next_poll_at': datetime.datetime.utcnow()}
    session.query(models.Processing).filter(models.Processing.status.in_(status))\
           .update(params, synchronize_session=False)


@read_session
def get_num_active_processings(active_status=None, session=None):
    if active_status and not isinstance(active_status, (list, tuple)):
        active_status = [active_status]
    if active_status and len(active_status) == 1:
        active_status = [active_status[0], active_status[0]]

    try:
        query = session.query(models.Processing.status, models.Processing.site, func.count(models.Processing.processing_id))
        if active_status:
            query = query.filter(models.Processing.status.in_(active_status))
        query = query.group_by(models.Processing.status, models.Processing.site)
        tmp = query.all()
        return tmp
    except Exception as error:
        raise error


@read_session
def get_active_processings(active_status=None, session=None):
    if active_status and not isinstance(active_status, (list, tuple)):
        active_status = [active_status]
    if active_status and len(active_status) == 1:
        active_status = [active_status[0], active_status[0]]

    try:
        query = session.query(models.Processing.request_id,
                              models.Processing.transform_id,
                              models.Processing.processing_id,
                              models.Processing.site,
                              models.Processing.status)
        if active_status:
            query = query.filter(models.Processing.status.in_(active_status))
        tmp = query.all()
        return tmp
    except Exception as error:
        raise error
