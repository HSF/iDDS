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
operations related to Requests.
"""

import datetime
import random

import sqlalchemy
from sqlalchemy import and_, func
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql.expression import asc, desc

from idds.common import exceptions
from idds.common.constants import RequestType, RequestStatus, RequestLocking
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base import models


def create_request(scope=None, name=None, requester=None, request_type=None,
                   username=None, userdn=None, transform_tag=None,
                   status=RequestStatus.New, locking=RequestLocking.Idle, priority=0,
                   lifetime=None, workload_id=None, request_metadata=None,
                   new_poll_period=1, update_poll_period=10, site=None,
                   new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                   processing_metadata=None):
    """
    Create a request.

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

    :returns: request.
    """

    is_pseudo_input = None
    if request_type in [RequestType.HyperParameterOpt, RequestType.HyperParameterOpt.value]:
        if not scope:
            scope = 'hpo'
            is_pseudo_input = True
        if not name:
            if workload_id is not None:
                name = 'hpo.%s.' % workload_id
            else:
                name = 'hpo.'
            name = name + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))
            is_pseudo_input = True

    if is_pseudo_input:
        if not request_metadata:
            request_metadata = {}
        request_metadata['is_pseudo_input'] = True

    if lifetime:
        expired_at = datetime.datetime.utcnow() + datetime.timedelta(days=lifetime)
    else:
        expired_at = None

    new_request = models.Request(scope=scope, name=name, requester=requester, request_type=request_type,
                                 username=username, userdn=userdn,
                                 transform_tag=transform_tag, status=status, locking=locking,
                                 priority=priority, workload_id=workload_id,
                                 expired_at=expired_at, site=site,
                                 new_retries=new_retries, update_retries=update_retries,
                                 max_new_retries=max_new_retries, max_update_retries=max_update_retries,
                                 request_metadata=request_metadata, processing_metadata=processing_metadata)
    if new_poll_period:
        new_poll_period = datetime.timedelta(seconds=new_poll_period)
        new_request.new_poll_period = new_poll_period
    if update_poll_period:
        update_poll_period = datetime.timedelta(seconds=update_poll_period)
        new_request.update_poll_period = update_poll_period
    return new_request


@transactional_session
def add_request(scope=None, name=None, requester=None, request_type=None,
                username=None, userdn=None, transform_tag=None,
                status=RequestStatus.New, locking=RequestLocking.Idle, priority=0,
                lifetime=None, workload_id=None, request_metadata=None,
                new_poll_period=1, update_poll_period=10, site=None,
                new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
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

    :raises DuplicatedObject: If an request with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: request id.
    """

    try:
        new_request = create_request(scope=scope, name=name, requester=requester, request_type=request_type,
                                     username=username, userdn=userdn,
                                     transform_tag=transform_tag, status=status, locking=locking,
                                     priority=priority, workload_id=workload_id, lifetime=lifetime,
                                     new_poll_period=new_poll_period, site=site,
                                     update_poll_period=update_poll_period,
                                     new_retries=new_retries, update_retries=update_retries,
                                     max_new_retries=max_new_retries, max_update_retries=max_update_retries,
                                     request_metadata=request_metadata, processing_metadata=processing_metadata)
        new_request.save(session=session)
        request_id = new_request.request_id
        return request_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Request %s:%s already exists!: %s' % (scope, name, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_request_ids_by_workload_id(workload_id, session=None):
    """
    Get request id or raise a NoObject exception.

    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request id.
    """

    if workload_id is None:
        return exceptions.WrongParameterException("workload_id should not be None")

    try:
        query = session.query(models.Request.request_id)\
                       .filter(models.Request.workload_id == workload_id)
        tmp = query.all()
        ret_ids = []
        if tmp:
            for req in tmp:
                ret_ids.append(req[0])
        return ret_ids
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('request with workload_id:%s cannot be found: %s' % (workload_id, error))


@read_session
def get_request_ids_by_name(name, session=None):
    """
    Get request ids or raise a NoObject exception.

    :param name: name of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request {name:id} dict.
    """
    try:
        query = session.query(models.Request.request_id, models.Request.name)\
                       .filter(models.Request.name.like(name.replace('*', '%')))
        tmp = query.all()
        ret_ids = {}
        if tmp:
            for req in tmp:
                ret_ids[req[1]] = req[0]
        return ret_ids
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('request with name:%s cannot be found: %s' % (name, error))


@read_session
def get_request_ids(request_id=None, workload_id=None, session=None):
    """
    Get request id or raise a NoObject exception.

    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request id.
    """
    if request_id:
        return [request_id]
    return get_request_ids_by_workload_id(workload_id)


@read_session
def get_request(request_id, to_json=False, session=None):
    """
    Get a request or raise a NoObject exception.

    :param request_id: The id of the request.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request.
    """

    try:
        query = session.query(models.Request)\
                       .filter(models.Request.request_id == request_id)

        ret = query.first()
        if not ret:
            return None
        else:
            if to_json:
                return ret.to_dict_json()
            else:
                return ret.to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('request request_id: %s cannot be found: %s' % (request_id, error))


@read_session
def get_request_by_id_status(request_id, status=None, locking=False, session=None):
    """
    Get a request or raise a NoObject exception.

    :param request_id: The id of the request.
    :param status: request status.
    :param locking: the locking status.

    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request.
    """

    try:
        query = session.query(models.Request)\
                       .filter(models.Request.request_id == request_id)

        if status:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]
            query = query.filter(models.Request.status.in_(status))

        if locking:
            query = query.filter(models.Request.locking == RequestLocking.Idle)
            query = query.with_for_update(skip_locked=True)

        ret = query.first()
        if not ret:
            return None
        else:
            return ret.to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('request request_id: %s cannot be found: %s' % (request_id, error))


@read_session
def get_requests(request_id=None, workload_id=None, with_detail=False, with_metadata=False,
                 with_request=False, with_transform=False, with_processing=False, to_json=False, session=None):
    """
    Get a request or raise a NoObject exception.

    :param workload_id: The workload id of the request.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request.
    """
    try:
        if with_request or not (with_transform or with_processing or with_detail or with_metadata):
            if with_metadata:
                query = session.query(models.Request)

                if request_id:
                    query = query.filter(models.Request.request_id == request_id)
                if workload_id:
                    query = query.filter(models.Request.workload_id == workload_id)

                tmp = query.all()
                rets = []
                if tmp:
                    for t in tmp:
                        if to_json:
                            t_dict = t.to_dict_json()
                        else:
                            t_dict = t.to_dict()
                        rets.append(t_dict)
                return rets
            else:
                query = session.query(models.Request.request_id,
                                      models.Request.scope,
                                      models.Request.name,
                                      models.Request.requester,
                                      models.Request.request_type,
                                      models.Request.username,
                                      models.Request.userdn,
                                      models.Request.transform_tag,
                                      models.Request.workload_id,
                                      models.Request.priority,
                                      models.Request.status,
                                      models.Request.substatus,
                                      models.Request.locking,
                                      models.Request.created_at,
                                      models.Request.updated_at,
                                      models.Request.next_poll_at,
                                      models.Request.accessed_at,
                                      models.Request.expired_at,
                                      models.Request.errors)

                if request_id:
                    query = query.filter(models.Request.request_id == request_id)
                if workload_id:
                    query = query.filter(models.Request.workload_id == workload_id)

                tmp = query.all()
                rets = []
                if tmp:
                    for t in tmp:
                        t2 = dict(zip(t.keys(), t))
                        rets.append(t2)
                return rets
        elif with_transform:
            subquery1 = session.query(models.Collection.coll_id, models.Collection.transform_id,
                                      models.Collection.scope.label("input_coll_scope"),
                                      models.Collection.name.label("input_coll_name"),
                                      models.Collection.status.label("input_coll_status"),
                                      models.Collection.bytes.label("input_coll_bytes"),
                                      models.Collection.total_files.label("input_total_files"),
                                      models.Collection.processed_files.label("input_processed_files"),
                                      models.Collection.new_files.label("input_new_files"),
                                      models.Collection.failed_files.label("input_failed_files"),
                                      models.Collection.missing_files.label("input_missing_files"),
                                      models.Collection.processing_files.label("input_processing_files")).filter(models.Collection.relation_type == 0)
            subquery1 = subquery1.subquery()

            subquery2 = session.query(models.Collection.coll_id, models.Collection.transform_id,
                                      models.Collection.scope.label("output_coll_scope"),
                                      models.Collection.name.label("output_coll_name"),
                                      models.Collection.status.label("output_coll_status"),
                                      models.Collection.bytes.label("output_coll_bytes"),
                                      models.Collection.total_files.label("output_total_files"),
                                      models.Collection.processed_files.label("output_processed_files"),
                                      models.Collection.new_files.label("output_new_files"),
                                      models.Collection.failed_files.label("output_failed_files"),
                                      models.Collection.missing_files.label("output_missing_files"),
                                      models.Collection.processing_files.label("output_processing_files")).filter(models.Collection.relation_type == 1)
            subquery2 = subquery2.subquery()
            if True:
                query = session.query(models.Request.request_id,
                                      models.Request.scope,
                                      models.Request.name,
                                      models.Request.requester,
                                      models.Request.request_type,
                                      models.Request.username,
                                      models.Request.userdn,
                                      models.Request.transform_tag,
                                      models.Request.workload_id,
                                      models.Request.priority,
                                      models.Request.status,
                                      models.Request.substatus,
                                      models.Request.locking,
                                      models.Request.created_at,
                                      models.Request.updated_at,
                                      models.Request.next_poll_at,
                                      models.Request.accessed_at,
                                      models.Request.expired_at,
                                      models.Request.errors,
                                      models.Transform.transform_id,
                                      models.Transform.transform_type,
                                      models.Transform.workload_id.label("transform_workload_id"),
                                      models.Transform.status.label("transform_status"),
                                      models.Transform.created_at.label("transform_created_at"),
                                      models.Transform.updated_at.label("transform_updated_at"),
                                      models.Transform.finished_at.label("transform_finished_at"),
                                      subquery1.c.input_coll_scope, subquery1.c.input_coll_name,
                                      subquery1.c.input_coll_status, subquery1.c.input_coll_bytes,
                                      subquery1.c.input_total_files,
                                      subquery1.c.input_processed_files,
                                      subquery1.c.input_processing_files,
                                      subquery1.c.input_new_files,
                                      subquery1.c.input_failed_files,
                                      subquery1.c.input_missing_files,
                                      subquery2.c.output_coll_scope, subquery2.c.output_coll_name,
                                      subquery2.c.output_coll_status, subquery2.c.output_coll_bytes,
                                      subquery2.c.output_total_files,
                                      subquery2.c.output_processed_files,
                                      subquery2.c.output_processing_files,
                                      subquery2.c.output_new_files,
                                      subquery2.c.output_failed_files,
                                      subquery2.c.output_missing_files)

            if request_id:
                query = query.filter(models.Request.request_id == request_id)
            if workload_id:
                query = query.filter(models.Request.workload_id == workload_id)

            query = query.outerjoin(models.Transform, and_(models.Request.request_id == models.Transform.request_id))
            query = query.outerjoin(subquery1, and_(subquery1.c.transform_id == models.Transform.transform_id))
            query = query.outerjoin(subquery2, and_(subquery2.c.transform_id == models.Transform.transform_id))
            query = query.order_by(asc(models.Request.request_id))

            tmp = query.all()
            rets = []
            if tmp:
                for t in tmp:
                    # t2 = dict(t)
                    t2 = dict(zip(t.keys(), t))

                    if 'request_metadata' in t2 and t2['request_metadata']:
                        if 'workflow' in t2['request_metadata']:
                            workflow = t2['request_metadata']['workflow']
                            workflow_data = None
                            if 'processing_metadata' in t2 and t2['processing_metadata'] and 'workflow_data' in t2['processing_metadata']:
                                workflow_data = t2['processing_metadata']['workflow_data']
                            if workflow is not None and workflow_data is not None:
                                workflow.metadata = workflow_data
                                t2['request_metadata']['workflow'] = workflow
                        if 'build_workflow' in t2['request_metadata']:
                            build_workflow = t2['request_metadata']['build_workflow']
                            build_workflow_data = None
                            if 'processing_metadata' in t2 and t2['processing_metadata'] and 'build_workflow_data' in t2['processing_metadata']:
                                build_workflow_data = t2['processing_metadata']['build_workflow_data']
                            if build_workflow is not None and build_workflow_data is not None:
                                build_workflow.metadata = build_workflow_data
                                t2['request_metadata']['build_workflow'] = build_workflow

                    rets.append(t2)
            return rets
        elif with_processing:
            subquery = session.query(models.Processing.processing_id,
                                     models.Processing.transform_id,
                                     models.Processing.workload_id,
                                     models.Processing.status.label("processing_status"),
                                     models.Processing.created_at.label("processing_created_at"),
                                     models.Processing.updated_at.label("processing_updated_at"),
                                     models.Processing.finished_at.label("processing_finished_at"))
            subquery = subquery.subquery()

            if True:
                query = session.query(models.Request.request_id,
                                      models.Request.scope,
                                      models.Request.name,
                                      models.Request.requester,
                                      models.Request.request_type,
                                      models.Request.username,
                                      models.Request.userdn,
                                      models.Request.transform_tag,
                                      models.Request.workload_id,
                                      models.Request.priority,
                                      models.Request.status,
                                      models.Request.substatus,
                                      models.Request.locking,
                                      models.Request.created_at,
                                      models.Request.updated_at,
                                      models.Request.next_poll_at,
                                      models.Request.accessed_at,
                                      models.Request.expired_at,
                                      models.Request.errors,
                                      models.Transform.transform_id,
                                      models.Transform.workload_id.label("transform_workload_id"),
                                      models.Transform.status.label("transform_status"),
                                      subquery.c.processing_id,
                                      subquery.c.workload_id.label("processing_workload_id"),
                                      subquery.c.processing_status,
                                      subquery.c.processing_created_at,
                                      subquery.c.processing_updated_at,
                                      subquery.c.processing_finished_at)

            if request_id:
                query = query.filter(models.Request.request_id == request_id)
            if workload_id:
                query = query.filter(models.Request.workload_id == workload_id)

            query = query.outerjoin(models.Transform, and_(models.Request.request_id == models.Transform.request_id))
            query = query.outerjoin(subquery, and_(subquery.c.transform_id == models.Transform.transform_id))
            query = query.order_by(asc(models.Request.request_id))

            tmp = query.all()
            rets = []
            if tmp:
                for t in tmp:
                    # t2 = dict(t)
                    t2 = dict(zip(t.keys(), t))

                    if 'request_metadata' in t2 and t2['request_metadata']:
                        if 'workflow' in t2['request_metadata']:
                            workflow = t2['request_metadata']['workflow']
                            workflow_data = None
                            if 'processing_metadata' in t2 and t2['processing_metadata'] and 'workflow_data' in t2['processing_metadata']:
                                workflow_data = t2['processing_metadata']['workflow_data']
                            if workflow is not None and workflow_data is not None:
                                workflow.metadata = workflow_data
                                t2['request_metadata']['workflow'] = workflow
                        if 'build_workflow' in t2['request_metadata']:
                            build_workflow = t2['request_metadata']['build_workflow']
                            build_workflow_data = None
                            if 'processing_metadata' in t2 and t2['processing_metadata'] and 'build_workflow_data' in t2['processing_metadata']:
                                build_workflow_data = t2['processing_metadata']['build_workflow_data']
                            if build_workflow is not None and build_workflow_data is not None:
                                build_workflow.metadata = build_workflow_data
                                t2['request_metadata']['build_workflow'] = build_workflow

                    rets.append(t2)
            return rets
        elif with_detail or with_metadata:
            subquery1 = session.query(models.Collection.coll_id, models.Collection.transform_id,
                                      models.Collection.scope.label("input_coll_scope"),
                                      models.Collection.name.label("input_coll_name"),
                                      models.Collection.status.label("input_coll_status"),
                                      models.Collection.bytes.label("input_coll_bytes"),
                                      models.Collection.total_files.label("input_total_files"),
                                      models.Collection.processed_files.label("input_processed_files"),
                                      models.Collection.new_files.label("input_new_files"),
                                      models.Collection.failed_files.label("input_failed_files"),
                                      models.Collection.missing_files.label("input_missing_files"),
                                      models.Collection.processing_files.label("input_processing_files")).filter(models.Collection.relation_type == 0)
            subquery1 = subquery1.subquery()

            subquery2 = session.query(models.Collection.coll_id, models.Collection.transform_id,
                                      models.Collection.scope.label("output_coll_scope"),
                                      models.Collection.name.label("output_coll_name"),
                                      models.Collection.status.label("output_coll_status"),
                                      models.Collection.bytes.label("output_coll_bytes"),
                                      models.Collection.total_files.label("output_total_files"),
                                      models.Collection.processed_files.label("output_processed_files"),
                                      models.Collection.new_files.label("output_new_files"),
                                      models.Collection.failed_files.label("output_failed_files"),
                                      models.Collection.missing_files.label("output_missing_files"),
                                      models.Collection.processing_files.label("output_processing_files")).filter(models.Collection.relation_type == 1)
            subquery2 = subquery2.subquery()

            if with_metadata:
                query = session.query(models.Request.request_id,
                                      models.Request.scope,
                                      models.Request.name,
                                      models.Request.requester,
                                      models.Request.request_type,
                                      models.Request.username,
                                      models.Request.userdn,
                                      models.Request.transform_tag,
                                      models.Request.workload_id,
                                      models.Request.priority,
                                      models.Request.status,
                                      models.Request.substatus,
                                      models.Request.locking,
                                      models.Request.created_at,
                                      models.Request.updated_at,
                                      models.Request.next_poll_at,
                                      models.Request.accessed_at,
                                      models.Request.expired_at,
                                      models.Request.errors,
                                      models.Request._request_metadata.label('request_metadata'),
                                      models.Request._processing_metadata.label('processing_metadata'),
                                      models.Transform.transform_id,
                                      models.Transform.transform_type,
                                      models.Transform.name.label("transform_name"),
                                      models.Transform.workload_id.label("transform_workload_id"),
                                      models.Transform.status.label("transform_status"),
                                      models.Transform.created_at.label("transform_created_at"),
                                      models.Transform.updated_at.label("transform_updated_at"),
                                      models.Transform.finished_at.label("transform_finished_at"),
                                      subquery1.c.input_coll_scope, subquery1.c.input_coll_name,
                                      subquery1.c.input_coll_status, subquery1.c.input_coll_bytes,
                                      subquery1.c.input_total_files,
                                      subquery1.c.input_processed_files,
                                      subquery1.c.input_processing_files,
                                      subquery1.c.input_new_files,
                                      subquery1.c.input_failed_files,
                                      subquery1.c.input_missing_files,
                                      subquery2.c.output_coll_scope, subquery2.c.output_coll_name,
                                      subquery2.c.output_coll_status, subquery2.c.output_coll_bytes,
                                      subquery2.c.output_total_files,
                                      subquery2.c.output_processed_files,
                                      subquery2.c.output_processing_files,
                                      subquery2.c.output_new_files,
                                      subquery2.c.output_failed_files,
                                      subquery2.c.output_missing_files,)
            else:
                query = session.query(models.Request.request_id,
                                      models.Request.scope,
                                      models.Request.name,
                                      models.Request.requester,
                                      models.Request.request_type,
                                      models.Request.username,
                                      models.Request.userdn,
                                      models.Request.transform_tag,
                                      models.Request.workload_id,
                                      models.Request.priority,
                                      models.Request.status,
                                      models.Request.substatus,
                                      models.Request.locking,
                                      models.Request.created_at,
                                      models.Request.updated_at,
                                      models.Request.next_poll_at,
                                      models.Request.accessed_at,
                                      models.Request.expired_at,
                                      models.Request.errors,
                                      models.Transform.transform_id,
                                      models.Transform.transform_type,
                                      models.Transform.name.label("transform_name"),
                                      models.Transform.workload_id.label("transform_workload_id"),
                                      models.Transform.status.label("transform_status"),
                                      models.Transform.created_at.label("transform_created_at"),
                                      models.Transform.updated_at.label("transform_updated_at"),
                                      models.Transform.finished_at.label("transform_finished_at"),
                                      subquery1.c.input_coll_scope, subquery1.c.input_coll_name,
                                      subquery1.c.input_coll_status, subquery1.c.input_coll_bytes,
                                      subquery1.c.input_total_files,
                                      subquery1.c.input_processed_files,
                                      subquery1.c.input_processing_files,
                                      subquery1.c.input_new_files,
                                      subquery1.c.input_failed_files,
                                      subquery1.c.input_missing_files,
                                      subquery2.c.output_coll_scope, subquery2.c.output_coll_name,
                                      subquery2.c.output_coll_status, subquery2.c.output_coll_bytes,
                                      subquery2.c.output_total_files,
                                      subquery2.c.output_processed_files,
                                      subquery2.c.output_processing_files,
                                      subquery2.c.output_new_files,
                                      subquery2.c.output_failed_files,
                                      subquery2.c.output_missing_files,)

            if request_id:
                query = query.filter(models.Request.request_id == request_id)
            if workload_id:
                query = query.filter(models.Request.workload_id == workload_id)

            query = query.outerjoin(models.Transform, and_(models.Request.request_id == models.Transform.request_id))
            query = query.outerjoin(subquery1, and_(subquery1.c.transform_id == models.Transform.transform_id))
            query = query.outerjoin(subquery2, and_(subquery2.c.transform_id == models.Transform.transform_id))
            query = query.order_by(asc(models.Request.request_id))

            tmp = query.all()
            rets = []
            if tmp:
                for t in tmp:
                    # t2 = dict(t)
                    t2 = dict(zip(t.keys(), t))

                    if 'request_metadata' in t2 and t2['request_metadata']:
                        if 'workflow' in t2['request_metadata']:
                            workflow = t2['request_metadata']['workflow']
                            workflow_data = None
                            if 'processing_metadata' in t2 and t2['processing_metadata'] and 'workflow_data' in t2['processing_metadata']:
                                workflow_data = t2['processing_metadata']['workflow_data']
                            if workflow is not None and workflow_data is not None:
                                workflow.metadata = workflow_data
                                t2['request_metadata']['workflow'] = workflow
                        if 'build_workflow' in t2['request_metadata']:
                            build_workflow = t2['request_metadata']['build_workflow']
                            build_workflow_data = None
                            if 'processing_metadata' in t2 and t2['processing_metadata'] and 'build_workflow_data' in t2['processing_metadata']:
                                build_workflow_data = t2['processing_metadata']['build_workflow_data']
                            if build_workflow is not None and build_workflow_data is not None:
                                build_workflow.metadata = build_workflow_data
                                t2['request_metadata']['build_workflow'] = build_workflow

                    rets.append(t2)
            return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('request workload_id: %s cannot be found: %s' % (workload_id, error))


@transactional_session
def extend_requests(request_id=None, workload_id=None, lifetime=30, session=None):
    """
    extend an request's lifetime.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param lifetime: The life time as umber of days.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        query = session.query(models.Request)
        if request_id:
            query = query.filter_by(request_id=request_id)
        else:
            query = query.filter_by(workload_id=workload_id)

        update_items = {'expired_at': datetime.datetime.utcnow() + datetime.timedelta(days=lifetime)}
        query.update(update_items, synchronize_session=False)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request (workload_id: %s) cannot be found: %s' % (workload_id, error))


@transactional_session
def cancel_requests(request_id=None, workload_id=None, session=None):
    """
    cancel an request.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        query = session.query(models.Request)
        if request_id:
            query = query.filter_by(request_id=request_id)
        else:
            query = query.filter_by(workload_id=workload_id)

        update_items = {'status': RequestStatus.Cancel}
        query.update(update_items, synchronize_session=False)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request %s cannot be found: %s' % (request_id, error))


@read_session
def get_requests_by_requester(scope, name, requester, to_json=False, session=None):
    """
    Get requests.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param requestr: The requester, such as panda, user and so on.
    :param to_json: return json format.

    :raises NoObject: If no request is founded.

    :returns: list of Request.
    """

    try:
        query = session.query(models.Request)\
                       .filter(models.Request.requester == requester)\
                       .filter(models.Request.scope == scope)\
                       .filter(models.Request.name.like(name.replace('*', '%')))
        tmp = query.all()
        rets = []
        if tmp:
            for req in tmp:
                if to_json:
                    rets.append(req.to_dict_json())
                else:
                    rets.append(req.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No requests with scope:name(%s:%s) and requester(%s) %s' % (scope, name, requester, error))


@transactional_session
def get_requests_by_status_type(status, request_type=None, time_period=None, request_ids=[], locking=False,
                                locking_for_update=False, bulk_size=None, to_json=False, by_substatus=False,
                                min_request_id=None, new_poll=False, update_poll=False, only_return_id=False,
                                session=None):
    """
    Get requests.

    :param status: list of status of the request data.
    :param request_type: The type of the request data.
    :param locking: Wheter to lock requests to avoid others get the same request.
    :param bulk_size: Size limitation per retrieve.
    :param to_json: return json format.

    :raises NoObject: If no request are founded.

    :returns: list of Request.
    """

    try:
        if status:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]

        if only_return_id:
            query = session.query(models.Request.request_id)
        else:
            query = session.query(models.Request)

        if status:
            if by_substatus:
                query = query.filter(models.Request.substatus.in_(status))
            else:
                query = query.filter(models.Request.status.in_(status))
        if new_poll:
            query = query.filter(models.Request.updated_at + models.Request.new_poll_period <= datetime.datetime.utcnow())
        if update_poll:
            query = query.filter(models.Request.updated_at + models.Request.update_poll_period <= datetime.datetime.utcnow())

        if request_type is not None:
            query = query.filter(models.Request.request_type == request_type)
        if request_ids:
            query = query.filter(models.Request.request_id.in_(request_ids))
        else:
            if min_request_id is not None:
                query = query.filter(models.Request.request_id >= min_request_id)
        if locking:
            query = query.filter(models.Request.locking == RequestLocking.Idle)

        if locking_for_update:
            query = query.with_for_update(skip_locked=True)
        else:
            # query = query.order_by(asc(models.Request.updated_at))\
            #              .order_by(desc(models.Request.priority))
            query = query.order_by(desc(models.Request.priority))\
                         .order_by(asc(models.Request.updated_at))

        if bulk_size:
            query = query.limit(bulk_size)

        tmp = query.all()
        rets = []
        if tmp:
            for req in tmp:
                if only_return_id:
                    rets.append(req[0])
                else:
                    if to_json:
                        rets.append(req.to_dict_json())
                    else:
                        rets.append(req.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No requests with status: %s, request_type: %s, time_period: %s, locking: %s, %s' % (status, request_type, time_period, locking, error))


@transactional_session
def update_request(request_id, parameters, update_request_metadata=False, session=None):
    """
    update an request.

    :param request_id: the request id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        parameters['updated_at'] = datetime.datetime.utcnow()

        if 'new_poll_period' in parameters and type(parameters['new_poll_period']) not in [datetime.timedelta]:
            parameters['new_poll_period'] = datetime.timedelta(seconds=parameters['new_poll_period'])
        if 'update_poll_period' in parameters and type(parameters['update_poll_period']) not in [datetime.timedelta]:
            parameters['update_poll_period'] = datetime.timedelta(seconds=parameters['update_poll_period'])

        if 'request_metadata' in parameters:
            if 'workflow' in parameters['request_metadata']:
                workflow = parameters['request_metadata']['workflow']

                if workflow is not None:
                    if hasattr(workflow, 'refresh_works'):
                        workflow.refresh_works()
                    if 'processing_metadata' not in parameters or not parameters['processing_metadata']:
                        parameters['processing_metadata'] = {}
                    parameters['processing_metadata']['workflow_data'] = workflow.metadata
            if 'build_workflow' in parameters['request_metadata']:
                build_workflow = parameters['request_metadata']['build_workflow']

                if build_workflow is not None:
                    build_workflow.refresh_works()
                    if 'processing_metadata' not in parameters or not parameters['processing_metadata']:
                        parameters['processing_metadata'] = {}
                    parameters['processing_metadata']['build_workflow_data'] = build_workflow.metadata

        if 'request_metadata' in parameters:
            if not update_request_metadata:
                del parameters['request_metadata']
            else:
                parameters['_request_metadata'] = parameters['request_metadata']
                del parameters['request_metadata']
        if 'processing_metadata' in parameters:
            parameters['_processing_metadata'] = parameters['processing_metadata']
            del parameters['processing_metadata']
        session.query(models.Request).filter_by(request_id=request_id)\
               .update(parameters, synchronize_session=False)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request %s cannot be found: %s' % (request_id, error))


@transactional_session
def delete_requests(request_id=None, workload_id=None, session=None):
    """
    delete an request.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        if request_id:
            session.query(models.Request).filter_by(request_id=request_id).delete()
        else:
            session.query(models.Request).filter_by(workload_id=workload_id).delete()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request (request_id: %s, workload_id: %s) cannot be found: %s' % (request_id, workload_id, error))


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """

    params = {'locking': 0}
    session.query(models.Request).filter(models.Request.locking == RequestLocking.Locking)\
           .filter(models.Request.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period))\
           .update(params, synchronize_session=False)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the request
    """
    if not isinstance(status, (list, tuple)):
        status = [status]
    if len(status) == 1:
        status = [status[0], status[0]]

    params = {'next_poll_at': datetime.datetime.utcnow()}
    session.query(models.Request).filter(models.Request.status.in_(status))\
           .update(params, synchronize_session=False)


@read_session
def get_last_request_id(status, older_than=None, session=None):
    """
    Get last request id which is older than a timestamp.

    :param status: status of the request.
    :param older_than: days older than current timestamp.

    :returns request_id
    """
    if not isinstance(status, (list, tuple)):
        status = [status]
    if len(status) == 1:
        status = [status[0], status[0]]

    query = session.query(models.Request.request_id)
    if status:
        query = query.filter(models.Request.status.in_(status))
    query = query.filter(models.Request.updated_at <= datetime.datetime.utcnow() - datetime.timedelta(days=older_than))
    query = query.order_by(desc(models.Request.request_id))
    ret = query.first()
    if ret:
        return ret[0]
    return ret


@read_session
def get_num_active_requests(active_status=None, session=None):
    if active_status and not isinstance(active_status, (list, tuple)):
        active_status = [active_status]
    if active_status and len(active_status) == 1:
        active_status = [active_status[0], active_status[0]]

    try:
        query = session.query(models.Request.status, models.Request.site, func.count(models.Request.request_id))
        if active_status:
            query = query.filter(models.Request.status.in_(active_status))
        query = query.group_by(models.Request.status, models.Request.site)
        tmp = query.all()
        return tmp
    except Exception as error:
        raise error


@read_session
def get_active_requests(active_status=None, session=None):
    if active_status and not isinstance(active_status, (list, tuple)):
        active_status = [active_status]
    if active_status and len(active_status) == 1:
        active_status = [active_status[0], active_status[0]]

    try:
        query = session.query(models.Request.request_id, models.Request.status, models.Request.site)
        if active_status:
            query = query.filter(models.Request.status.in_(active_status))
        tmp = query.all()
        return tmp
    except Exception as error:
        raise error


@read_session
def get_min_request_id(difference=1000, session=None):
    try:
        seq = models.get_request_sequence()
        row = session.query(seq.next_value()).one()
        if row:
            max_request_id = row[0]
            return max_request_id - difference
        else:
            return 0
    except Exception:
        try:
            query = session.query(func.max(models.Request.request_id))
            row = query.one()
            if row:
                max_request_id = row[0]
                return max_request_id - difference
            else:
                return 0
        except Exception as error:
            raise error
