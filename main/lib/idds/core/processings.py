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


from idds.orm.base.session import read_session, transactional_session

from idds.orm import processings as orm_processings


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
    return orm_processings.add_processing(transform_id=transform_id, status=status, submitter=submitter,
                                          granularity=granularity, granularity_type=granularity_type,
                                          expired_at=expired_at, processing_metadata=processing_metadata,
                                          session=session)


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
    return orm_processings.get_processing(processing_id=processing_id, transform_id=transform_id,
                                          retries=retries, session=session)


@read_session
def get_processings_by_status(status, time_period=None, session=None):
    """
    Get processing or raise a NoObject exception.

    :param status: Processing status of list of processing status.
    :param time_period: Time period in seconds.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """
    return orm_processings.get_processings_by_status(status=status, period=time_period, session=session)


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
    return orm_processings.update_processing(processing_id=processing_id, parameters=parameters, session=session)


@transactional_session
def delete_processing(processing_id=None, session=None):
    """
    delete a processing.

    :param processing_id: The id of the processing.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.
    :raises DatabaseException: If there is a database error.
    """
    return orm_processings.delete_processing(processing_id=processing_id, session=session)
