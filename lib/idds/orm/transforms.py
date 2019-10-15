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
operations related to Transform.
"""

import datetime
import json

import sqlalchemy
from sqlalchemy import BigInteger, Integer
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql import text, outparam

from idds.common import exceptions
from idds.common.constants import TransformType, TransformStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base.utils import row2dict


@transactional_session
def add_transform(transform_type, transform_tag=None, priority=0, status=TransformStatus.New, retries=0,
                  expired_at=None, transform_metadata=None, request_id=None, session=None):
    """
    Add a transform.

    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param priority: priority.
    :param status: Transform status.
    :param retries: The number of retries.
    :param expired_at: The datetime when it expires.
    :param transform_metadata: The metadata as json.

    :raises DuplicatedObject: If a transform with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: content id.
    """
    if isinstance(transform_type, TransformType):
        transform_type = transform_type.value
    if isinstance(status, TransformStatus):
        status = status.value
    if transform_metadata:
        transform_metadata = json.dumps(transform_metadata)

    insert = """insert into atlas_idds.transforms(transform_type, transform_tag, priority, status, retries,
                                                  created_at, expired_at, transform_metadata)
                values(:transform_type, :transform_tag, :priority, :status, :retries, :created_at,
                       :expired_at, :transform_metadata) returning transform_id into :transform_id
             """
    stmt = text(insert)
    stmt = stmt.bindparams(outparam("transform_id", type_=BigInteger().with_variant(Integer, "sqlite")))

    try:
        transform_id = None
        ret = session.execute(stmt, {'transform_type': transform_type, 'transform_tag': transform_tag,
                                     'priority': priority, 'status': status, 'retries': retries,
                                     'created_at': datetime.datetime.utcnow(), 'expired_at': expired_at,
                                     'transform_metadata': transform_metadata, 'transform_id': transform_id})

        transform_id = ret.out_parameters['transform_id'][0]

        if request_id:
            insert_req2transforms = """insert into atlas_idds.req2transforms(request_id, transform_id)
                                       values(:request_id, :transform_id)
                                    """
            stmt = text(insert_req2transforms)
            session.execute(stmt, {'request_id': request_id, 'transform_id': transform_id})
        return transform_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Transform already exists!: %s' % (error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_transform(transform_id, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_id: Transform id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transform.
    """

    try:
        select = """select * from atlas_idds.transforms where transform_id=:transform_id"""
        stmt = text(select)
        result = session.execute(stmt, {'transform_id': transform_id})
        transform = result.fetchone()

        if transform is None:
            raise exceptions.NoObject('Transform(transform_id: %s) cannot be found' %
                                      (transform_id))

        transform = row2dict(transform)
        if transform['transform_type']:
            transform['transform_type'] = TransformType(transform['transform_type'])
        if transform['status'] is not None:
            transform['status'] = TransformStatus(transform['status'])
        if transform['transform_metadata']:
            transform['transform_metadata'] = json.loads(transform['transform_metadata'])

        return transform
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Transform(transform_id: %s) cannot be found: %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@transactional_session
def update_transform(transform_id, parameters, session=None):
    """
    update a transform.

    :param transform_id: the transform id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        if 'transform_type' in parameters and isinstance(parameters['transform_type'], TransformType):
            parameters['transform_type'] = parameters['transform_type'].value
        if 'status' in parameters and isinstance(parameters['status'], TransformStatus):
            parameters['status'] = parameters['status'].value
        if 'transform_metadata' in parameters:
            parameters['transform_metadata'] = json.dumps(parameters['transform_metadata'])

        update = "update atlas_idds.transforms set "
        for key in parameters.keys():
            update += key + "=:" + key + ","
        update = update[:-1]
        update += " where transform_id=:transform_id"

        if 'status' in parameters and parameters['status'] in [TransformStatus.Finished, TransformStatus.Failed]:
            parameters['finished_at'] = datetime.datetime.utcnow()
        stmt = text(update)
        parameters['transform_id'] = transform_id
        session.execute(stmt, parameters)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Transfrom %s cannot be found: %s' % (transform_id, error))


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
        delete_req2transform = "delete from atlas_idds.req2transforms where transform_id=:transform_id"
        stmt = text(delete_req2transform)
        session.execute(stmt, {'transform_id': transform_id})
        delete = "delete from atlas_idds.transforms where transform_id=:transform_id"
        stmt = text(delete)
        session.execute(stmt, {'transform_id': transform_id})
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Transfrom %s cannot be found: %s' % (transform_id, error))
