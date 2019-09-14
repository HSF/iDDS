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
operations related to Processings.
"""

import datetime
import json

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql import text

from idds.common import exceptions
from idds.common.constants import GranularityType, ProcessingStatus
from idds.orm.session import read_session, transactional_session
from idds.orm.utils import row2dict


@transactional_session
def add_processing(transform_id, status, submitter=None, granularity=None, granularity_type=None,
                   expired_at=None, processing_metadata=None, session=None):
    """
    Add a processing.

    :param transform_id: Transform id.
    :param status: processing status.
    :param submitter: submitter name.
    :param granularity: Granularity size.
    :param granularity_type: Granularity type.
    :param expired_at: The datetime when it expires.
    :param processing_metadata: The metadata as json.

    :raises DuplicatedObject: If a processing with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: processing id.
    """
    if isinstance(granularity_type, GranularityType):
        granularity_type = granularity_type.value
    if isinstance(status, ProcessingStatus):
        status = status.value
    if processing_metadata:
        processing_metadata = json.dumps(processing_metadata)

    insert = """insert into atlas_idds.processings(transform_id, status, submitter, granularity_type,
                                                   granularity, created_at, expired_at, processing_metadata)
                values(:transform_id, :status, :submitter, :granularity_type, :granularity, :created_at,
                       :expired_at, :processing_metadata)
             """
    get_id = """select max(processing_id) from atlas_idds.processings"""

    stmt = text(insert)
    id_stmt = text(get_id)

    try:
        session.execute(stmt, {'transform_id': transform_id, 'status': status, 'submitter': submitter,
                               'granularity_type': granularity_type, 'granularity': granularity,
                               'created_at': datetime.datetime.utcnow(), 'expired_at': expired_at,
                               'processing_metadata': processing_metadata})

        result = session.execute(id_stmt)
        id = result.fetchone()[0]

        return id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Processing already exists!: %s' % (error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_processing(processing_id=None, transform_id=None, retries=0, session=None):
    """
    Get processing or raise a NoObject exception.

    :param processing_id: Processing id.
    :param tranform_id: Transform id.
    :param retries: Transform retries.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processing.
    """

    try:
        if processing_id:
            select = """select * from atlas_idds.processings where processing_id=:processing_id"""
            stmt = text(select)
            result = session.execute(stmt, {'processing_id': processing_id})
        else:
            # TODO: add retries to retrieve only the processing coressponding to the transform and retries
            select = """select * from atlas_idds.processings where transform_id=:transform_id"""
            stmt = text(select)
            result = session.execute(stmt, {'transform_id': transform_id})
        processing = result.fetchone()

        if processing is None:
            raise exceptions.NoObject('Processing(processing_id: %s, transform_id: %s, retries: %s) cannot be found' %
                                      (processing_id, transform_id, retries))

        processing = row2dict(processing)
        if processing['granularity_type'] is not None:
            processing['granularity_type'] = GranularityType(processing['granularity_type'])
        if processing['status'] is not None:
            processing['status'] = ProcessingStatus(processing['status'])
        if processing['processing_metadata']:
            processing['processing_metadata'] = json.loads(processing['processing_metadata'])

        return processing
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processing(processing_id: %s, transform_id: %s, retries: %s) cannot be found: %s' %
                                  (processing_id, transform_id, retries, error))
    except Exception as error:
        raise error


@transactional_session
def update_processing(processing_id, parameters, session=None):
    """
    update a processing.

    :param processing_id: the transform id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        if 'granularity_type' in parameters and isinstance(parameters['granularity_type'], GranularityType):
            parameters['granularity_type'] = parameters['granularity_type'].value
        if 'status' in parameters and isinstance(parameters['status'], ProcessingStatus):
            parameters['status'] = parameters['status'].value
        if 'processing_metadata' in parameters:
            parameters['processing_metadata'] = json.dumps(parameters['processing_metadata'])

        update = "update atlas_idds.processings set "
        for key in parameters.keys():
            update += key + "=:" + key + ","
        update = update[:-1]
        update += " where processing_id=:processing_id"

        if parameters['status'] in [ProcessingStatus.Finished, ProcessingStatus.Failed, ProcessingStatus.Lost]:
            parameters['finished_at'] = datetime.datetime.utcnow()
        stmt = text(update)
        parameters['processing_id'] = processing_id
        session.execute(stmt, parameters)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processing %s cannot be found: %s' % (processing_id, error))


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
        delete = "delete from atlas_idds.processings where processing_id=:processing_id"
        stmt = text(delete)
        session.execute(stmt, {'processing_id': processing_id})
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processing %s cannot be found: %s' % (processing_id, error))
