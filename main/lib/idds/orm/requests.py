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
import json

import sqlalchemy
from sqlalchemy import BigInteger, Integer
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql import text, bindparam, outparam

from idds.common import exceptions
from idds.common.constants import RequestType, RequestStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base import models
from idds.orm.base.utils import row2dict


@transactional_session
def add_request(scope, name, requester=None, request_type=None, transform_tag=None,
                status=RequestStatus.New, priority=0, lifetime=30, request_metadata=None, session=None):
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

    :raises DuplicatedObject: If an request with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: request id.
    """
    if isinstance(request_type, RequestType):
        request_type = request_type.value
    if isinstance(status, RequestStatus):
        status = status.value

    insert_request_sql = """insert into atlas_idds.requests(scope, name, requester, request_type, transform_tag, priority,
                                                 status, created_at, updated_at, expired_at, request_metadata)
                             values(:scope, :name, :requester, :request_type, :transform_tag, :priority, :status,
                             :created_at, :updated_at, :expired_at, :request_metadata) RETURNING request_id into :request_id
                         """

    insert_req2worload_sql = """insert into atlas_idds.req2workload(request_id, workload_id)values(:request_id, :workload_id)"""

    stmt = text(insert_request_sql)
    stmt = stmt.bindparams(outparam("request_id", type_=BigInteger().with_variant(Integer, "sqlite")))
    req2workload_stmt = text(insert_req2worload_sql)

    try:
        request_id = None
        ret = session.execute(stmt, {"scope": scope, "name": name, "requester": requester, "request_type": request_type,
                                     "transform_tag": transform_tag, "priority": priority, 'status': status,
                                     'created_at': datetime.datetime.utcnow(), 'updated_at': datetime.datetime.utcnow(),
                                     'expired_at': datetime.datetime.utcnow() + datetime.timedelta(days=lifetime),
                                     'request_metadata': json.dumps(request_metadata) if request_metadata else request_metadata,
                                     'request_id': request_id})
        request_id = ret.out_parameters['request_id'][0]

        if request_metadata and 'workload_id' in request_metadata:
            session.execute(req2workload_stmt, {'request_id': request_id, 'workload_id': request_metadata['workload_id']})
        return request_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Request %s:%s already exists!: %s' % (scope, name, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@transactional_session
def add_request_new(scope, name, requester=None, request_type=None, transform_tag=None,
                    status=RequestStatus.New, priority=0, lifetime=30, request_metadata=None, session=None):
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

    :raises DuplicatedObject: If an request with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: request id.
    """
    if isinstance(request_type, RequestType):
        request_type = request_type.value
    if isinstance(status, RequestStatus):
        status = status.value

    insert_req2worload_sql = """insert into atlas_idds.req2workload(request_id, workload_id)values(:request_id, :workload_id)"""
    req2workload_stmt = text(insert_req2worload_sql)

    try:
        new_request = models.Request(scope=scope, name=name, requester=requester, request_type=request_type,
                                     transform_tag=transform_tag, status=status, priority=priority,
                                     expired_at=datetime.datetime.utcnow() + datetime.timedelta(days=lifetime),
                                     request_metadata=request_metadata)
        new_request.save(session=session)
        request_id = new_request.request_id

        if request_metadata and 'workload_id' in request_metadata:
            session.execute(req2workload_stmt, {'request_id': request_id, 'workload_id': request_metadata['workload_id']})
        return request_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Request %s:%s already exists!: %s' % (scope, name, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_request_id_by_workload_id(workload_id, session=None):
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
        req2workload_select = "select request_id from atlas_idds.req2workload where workload_id=:workload_id"
        req2workload_stmt = text(req2workload_select)
        result = session.execute(req2workload_stmt, {'workload_id': workload_id})
        row = result.fetchone()
        if row:
            request_id = row[0]
            return request_id
        else:
            raise exceptions.NoObject('request with workload_id:%s cannot be found.' % (workload_id))
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('request with workload_id:%s cannot be found: %s' % (workload_id, error))


@read_session
def get_request_id(request_id=None, workload_id=None, session=None):
    """
    Get request id or raise a NoObject exception.

    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request id.
    """
    if request_id:
        return request_id
    return get_request_id_by_workload_id(workload_id)


def convert_request_to_dict(request):
    request = row2dict(request)
    if request['errors']:
        request['errors'] = json.loads(request['errors'])
    if request['request_metadata']:
        request['request_metadata'] = json.loads(request['request_metadata'])
    if request['request_type'] is not None:
        request['request_type'] = RequestType(request['request_type'])
    if request['status'] is not None:
        request['status'] = RequestStatus(request['status'])
    return request


@read_session
def get_request(request_id=None, workload_id=None, session=None):
    """
    Get a request or raise a NoObject exception.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request.
    """

    try:
        if not request_id and workload_id:
            request_id = get_request_id_by_workload_id(workload_id)

        req_select = """select request_id, scope, name, requester, request_type, transform_tag, priority,
                        status, created_at, updated_at, accessed_at, expired_at, errors, request_metadata
                        from atlas_idds.requests where request_id=:request_id
                     """
        req_stmt = text(req_select)
        result = session.execute(req_stmt, {'request_id': request_id})
        request = result.fetchone()

        if request is None:
            raise exceptions.NoObject('request request_id: %s, workload_id: %s cannot be found' % (request_id, workload_id))

        request = convert_request_to_dict(request)

        return request
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('request request_id: %s, workload_id: %s cannot be found: %s' % (request_id, workload_id, error))


@transactional_session
def extend_request(request_id=None, workload_id=None, lifetime=30, session=None):
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
        if not request_id and workload_id:
            request_id = get_request_id_by_workload_id(workload_id)
        req_update = "update atlas_idds.requests set expired_at=:expired_at where request_id=:request_id"
        req_stmt = text(req_update)
        session.execute(req_stmt, {'expired_at': datetime.datetime.utcnow() + datetime.timedelta(days=lifetime),
                                   'request_id': request_id})
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request %s cannot be found: %s' % (request_id, error))


@transactional_session
def cancel_request(request_id=None, workload_id=None, session=None):
    """
    cancel an request.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        if not request_id and workload_id:
            request_id = get_request_id_by_workload_id(workload_id)

        req_update = "update atlas_idds.requests set status=:status where request_id=:request_id"
        req_stmt = text(req_update)
        session.execute(req_stmt, {'status': RequestStatus.Cancel.value,
                                   'request_id': request_id})
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request %s cannot be found: %s' % (request_id, error))


@read_session
def get_requests_by_requester(scope, name, requester, session=None):
    """
    Get requests.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param requestr: The requester, such as panda, user and so on.

    :raises NoObject: If no request is founded.

    :returns: list of Request.
    """

    try:
        req_select = """select request_id, scope, name, requester, request_type, transform_tag, priority,
                        status, created_at, updated_at, accessed_at, expired_at, errors, request_metadata
                        from atlas_idds.requests where scope=:scope and requester=:requester and name=:name
                     """
        req_stmt = text(req_select)
        result = session.execute(req_stmt, {'scope': scope, 'name': name.replace('*', '%'), 'requester': requester})
        requests = result.fetchall()
        return [convert_request_to_dict(req) for req in requests]
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No requests with scope:name(%s:%s) and requester(%s) %s' % (scope, name, requester, error))


@read_session
def get_requests_by_status_type(status, request_type=None, time_period=None, session=None):
    """
    Get requests.

    :param status: list of status of the request data.
    :param request_type: The type of the request data.

    :raises NoObject: If no request are founded.

    :returns: list of Request.
    """

    try:
        if status is None:
            raise exceptions.WrongParameterException("status should not be None")
        if not isinstance(status, (list, tuple)):
            status = [status]
        new_status = []
        for st in status:
            if isinstance(st, RequestStatus):
                st = st.value
            new_status.append(st)
        status = new_status
        if request_type is None:
            if time_period is None:
                req_select = """select request_id, scope, name, requester, request_type, transform_tag, priority,
                                status, created_at, updated_at, accessed_at, expired_at, errors, request_metadata
                                from atlas_idds.requests where status in :status order by priority desc
                             """
                req_stmt = text(req_select)
                req_stmt = req_stmt.bindparams(bindparam('status', expanding=True))
                result = session.execute(req_stmt, {'status': status})
            else:
                req_select = """select request_id, scope, name, requester, request_type, transform_tag, priority,
                                status, created_at, updated_at, accessed_at, expired_at, errors, request_metadata
                                from atlas_idds.requests where status in :status and updated_at < :updated_at
                                order by priority desc
                             """
                req_stmt = text(req_select)
                req_stmt = req_stmt.bindparams(bindparam('status', expanding=True))
                result = session.execute(req_stmt, {'status': status,
                                                    'updated_at': datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period)})
        else:
            if isinstance(request_type, RequestType):
                request_type = request_type.value
            if time_period is None:
                req_select = """select request_id, scope, name, requester, request_type, transform_tag, priority,
                                status, created_at, updated_at, accessed_at, expired_at, errors, request_metadata
                                from atlas_idds.requests where status in :status and request_type=:request_type
                                order by priority desc
                             """
                req_stmt = text(req_select)
                req_stmt = req_stmt.bindparams(bindparam('status', expanding=True))
                result = session.execute(req_stmt, {'status': status, 'request_type': request_type})
            else:
                req_select = """select request_id, scope, name, requester, request_type, transform_tag, priority,
                                status, created_at, updated_at, accessed_at, expired_at, errors, request_metadata
                                from atlas_idds.requests where status in :status and request_type=:request_type
                                and updated_at < :updated_at order by priority desc
                             """
                req_stmt = text(req_select)
                req_stmt = req_stmt.bindparams(bindparam('status', expanding=True))
                result = session.execute(req_stmt, {'status': status, 'request_type': request_type,
                                                    'updated_at': datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period)})
        requests = result.fetchall()
        return [convert_request_to_dict(req) for req in requests]
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No requests with status: %s, request_type: %s, time_period: %s' % (status, request_type, time_period, error))


@transactional_session
def update_request(request_id, parameters, session=None):
    """
    update an request.

    :param request_id: the request id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        if 'request_type' in parameters and isinstance(parameters['request_type'], RequestType):
            parameters['request_type'] = parameters['request_type'].value
        if 'status' in parameters and isinstance(parameters['status'], RequestStatus):
            parameters['status'] = parameters['status'].value
        if 'request_metadata' in parameters:
            parameters['request_metadata'] = json.dumps(parameters['request_metadata'])
        if 'errors' in parameters:
            parameters['errors'] = json.dumps(parameters['errors'])

        parameters['updated_at'] = datetime.datetime.utcnow()

        req_update = "update atlas_idds.requests set "
        for key in parameters.keys():
            req_update += key + "=:" + key + ","
        req_update = req_update[:-1]
        req_update += " where request_id=:request_id"

        req_stmt = text(req_update)
        parameters['request_id'] = request_id
        session.execute(req_stmt, parameters)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request %s cannot be found: %s' % (request_id, error))


@transactional_session
def delete_request(request_id=None, workload_id=None, session=None):
    """
    delete an request.

    :param request_id: The id of the request.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        if not request_id and workload_id:
            request_id = get_request_id_by_workload_id(workload_id)

        req2workload_delete = "delete from atlas_idds.req2workload where request_id=:request_id"
        req2workload_stmt = text(req2workload_delete)
        session.execute(req2workload_stmt, {'request_id': request_id})

        req_delete = "delete from atlas_idds.requests where request_id=:request_id"
        req_stmt = text(req_delete)
        session.execute(req_stmt, {'request_id': request_id})
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Request %s cannot be found: %s' % (request_id, error))
