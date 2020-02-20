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
from idds.common.constants import ProcessingLocking
from idds.orm import (processings as orm_processings,
                      collections as orm_collections,
                      contents as orm_contents,
                      messages as orm_messages,
                      transforms as orm_transforms)


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


@transactional_session
def get_processings_by_status(status, time_period=None, locking=False, bulk_size=None, session=None):
    """
    Get processing or raise a NoObject exception.

    :param status: Processing status of list of processing status.
    :param time_period: Time period in seconds.
    :param locking: Whether to retrieve only unlocked items and lock them.
    :param session: The database session in use.

    :raises NoObject: If no processing is founded.

    :returns: Processings.
    """
    processings = orm_processings.get_processings_by_status(status=status, period=time_period, locking=locking,
                                                            bulk_size=bulk_size, session=session)
    if locking:
        parameters = {'locking': ProcessingLocking.Locking}
        for processing in processings:
            orm_processings.update_processing(processing['processing_id'], parameters=parameters, session=session)
    return processings


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


@transactional_session
def update_processing_with_collection_contents(updated_processing, updated_collection=None, updated_files=None,
                                               coll_msg_content=None, file_msg_content=None, transform_updates=None,
                                               session=None):
    """
    Update processing with collection, contents, file messages and collection messages.

    :param updated_processing: dict with processing id and parameters.
    :param updated_collection: dict with collection id and parameters.
    :param updated_files: list of content files.
    :param coll_msg_content: message with collection info.
    :param file_msg_content: message with files info.
    """
    if updated_files:
        orm_contents.update_contents(updated_files, with_content_id=True, session=session)
    if file_msg_content:
        orm_messages.add_message(msg_type=file_msg_content['msg_type'],
                                 status=file_msg_content['status'],
                                 source=file_msg_content['source'],
                                 transform_id=file_msg_content['transform_id'],
                                 num_contents=file_msg_content['num_contents'],
                                 msg_content=file_msg_content['msg_content'],
                                 session=session)
    if updated_collection:
        orm_collections.update_collection(coll_id=updated_collection['coll_id'],
                                          parameters=updated_collection['parameters'],
                                          session=session)
    if coll_msg_content:
        orm_messages.add_message(msg_type=coll_msg_content['msg_type'],
                                 status=coll_msg_content['status'],
                                 source=coll_msg_content['source'],
                                 transform_id=coll_msg_content['transform_id'],
                                 num_contents=coll_msg_content['num_contents'],
                                 msg_content=coll_msg_content['msg_content'],
                                 session=session)
    if updated_processing:
        orm_processings.update_processing(processing_id=updated_processing['processing_id'],
                                          parameters=updated_processing['parameters'],
                                          session=session)
    if transform_updates:
        orm_transforms.update_transform(transform_id=transform_updates['transform_id'],
                                        parameters=transform_updates['parameters'],
                                        session=session)
