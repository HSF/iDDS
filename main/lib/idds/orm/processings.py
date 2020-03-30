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
from sqlalchemy import BigInteger, Integer
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql import text, bindparam, outparam

from idds.common import exceptions
from idds.common.constants import GranularityType, ProcessingStatus, ProcessingLocking
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base.utils import row2dict


@transactional_session
def add_processing(transform_id, status=ProcessingStatus.New, locking=ProcessingLocking.Idle, submitter=None,
                   granularity=None, granularity_type=None, expired_at=None, processing_metadata=None,
                   output_metadata=None, session=None):
    """
    Add a processing.

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
    if isinstance(granularity_type, GranularityType):
        granularity_type = granularity_type.value
    if isinstance(status, ProcessingStatus):
        status = status.value
    if isinstance(locking, ProcessingLocking):
        locking = locking.value
    if processing_metadata:
        processing_metadata = json.dumps(processing_metadata)
    if output_metadata:
        output_metadata = json.dumps(output_metadata)

    insert = """insert into atlas_idds.processings(transform_id, status, locking, submitter, granularity_type,
                                                   granularity, created_at, updated_at, expired_at, processing_metadata,
                                                   output_metadata)
                values(:transform_id, :status, :locking, :submitter, :granularity_type, :granularity, :created_at,
                       :updated_at, :expired_at, :processing_metadata, :output_metadata) returning processing_id into :processing_id
             """
    stmt = text(insert)
    stmt = stmt.bindparams(outparam("processing_id", type_=BigInteger().with_variant(Integer, "sqlite")))

    try:
        processing_id = None
        ret = session.execute(stmt, {'transform_id': transform_id, 'status': status, 'locking': locking,
                                     'submitter': submitter, 'granularity_type': granularity_type, 'granularity': granularity,
                                     'created_at': datetime.datetime.utcnow(), 'updated_at': datetime.datetime.utcnow(),
                                     'expired_at': expired_at, 'processing_metadata': processing_metadata,
                                     'output_metadata': output_metadata, 'processing_id': processing_id})
        processing_id = ret.out_parameters['processing_id'][0]

        return processing_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Processing already exists!: %s' % (error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_processing(processing_id, session=None):
    """
    Get processing or raise a NoObject exception.

    :param processing_id: Processing id.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processing.
    """

    try:
        select = """select * from atlas_idds.processings where processing_id=:processing_id"""
        stmt = text(select)
        result = session.execute(stmt, {'processing_id': processing_id})
        processing = result.fetchone()

        if processing is None:
            raise exceptions.NoObject('Processing(processing_id: %s) cannot be found' % (processing_id))

        processing = row2dict(processing)
        if processing['granularity_type'] is not None:
            processing['granularity_type'] = GranularityType(processing['granularity_type'])
        if processing['status'] is not None:
            processing['status'] = ProcessingStatus(processing['status'])
        if processing['locking'] is not None:
            processing['locking'] = ProcessingLocking(processing['locking'])
        if processing['processing_metadata']:
            processing['processing_metadata'] = json.loads(processing['processing_metadata'])
        if processing['output_metadata']:
            processing['output_metadata'] = json.loads(processing['output_metadata'])

        return processing
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processing(processing_id: %s) cannot be found: %s' %
                                  (processing_id, error))
    except Exception as error:
        raise error


@read_session
def get_processings_by_transform_id(transform_id=None, session=None):
    """
    Get processings or raise a NoObject exception.

    :param tranform_id: Transform id.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """

    try:
        select = """select * from atlas_idds.processings where transform_id=:transform_id"""
        stmt = text(select)
        result = session.execute(stmt, {'transform_id': transform_id})
        processings = result.fetchall()

        ret = []
        for processing in processings:
            processing = row2dict(processing)
            if processing['granularity_type'] is not None:
                processing['granularity_type'] = GranularityType(processing['granularity_type'])
            if processing['status'] is not None:
                processing['status'] = ProcessingStatus(processing['status'])
            if processing['locking'] is not None:
                processing['locking'] = ProcessingLocking(processing['locking'])
            if processing['processing_metadata']:
                processing['processing_metadata'] = json.loads(processing['processing_metadata'])
            if processing['output_metadata']:
                processing['output_metadata'] = json.loads(processing['output_metadata'])

            ret.append(processing)
        return ret
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Processings(transform_id: %s) cannot be found: %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_processings_by_status(status, period=None, locking=False, bulk_size=None, session=None):
    """
    Get processing or raise a NoObject exception.

    :param status: Processing status of list of processing status.
    :param period: Time period in seconds.
    :param locking: Whether to retrieve only unlocked items.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """

    try:
        if not isinstance(status, (list, tuple)):
            status = [status]
        new_status = []
        for st in status:
            if isinstance(st, ProcessingStatus):
                st = st.value
            new_status.append(st)
        status = new_status

        select = """select * from atlas_idds.processings where status in :status"""
        params = {'status': status}

        if period:
            select = select + " and updated_at < :updated_at"
            params['updated_at'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=period)
        if locking:
            select = select + " and locking=:locking"
            params['locking'] = ProcessingLocking.Idle.value
        if bulk_size:
            select = select + " FETCH FIRST %s ROWS ONLY" % bulk_size

        stmt = text(select)
        stmt = stmt.bindparams(bindparam('status', expanding=True))
        result = session.execute(stmt, params)

        processings = result.fetchall()

        if processings is None:
            raise exceptions.NoObject('Processing(status: %s, period: %s) cannot be found' %
                                      (status, period))

        new_processings = []
        for processing in processings:
            processing = row2dict(processing)
            if processing['granularity_type'] is not None:
                processing['granularity_type'] = GranularityType(processing['granularity_type'])
            if processing['status'] is not None:
                processing['status'] = ProcessingStatus(processing['status'])
            if processing['processing_metadata']:
                processing['processing_metadata'] = json.loads(processing['processing_metadata'])
            if processing['output_metadata']:
                processing['output_metadata'] = json.loads(processing['output_metadata'])
            new_processings.append(processing)

        return new_processings
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No processing attached with status (%s): %s' % (status, error))
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
        if 'locking' in parameters and isinstance(parameters['locking'], ProcessingLocking):
            parameters['locking'] = parameters['locking'].value
        if 'processing_metadata' in parameters:
            parameters['processing_metadata'] = json.dumps(parameters['processing_metadata'])
        if 'output_metadata' in parameters:
            parameters['output_metadata'] = json.dumps(parameters['output_metadata'])

        parameters['updated_at'] = datetime.datetime.utcnow()

        update = "update atlas_idds.processings set "
        for key in parameters.keys():
            update += key + "=:" + key + ","
        update = update[:-1]
        update += " where processing_id=:processing_id"

        if 'status' in parameters and parameters['status'] in [ProcessingStatus.Finished, ProcessingStatus.Failed, ProcessingStatus.Lost]:
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


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """

    params = {'locking': 0,
              'updated_at': datetime.datetime.utcnow() - datetime.timedelta(seconds=time_period)}
    sql = "update atlas_idds.processings set locking = :locking where locking = 1 and updated_at < :updated_at"
    stmt = text(sql)
    session.execute(stmt, params)
