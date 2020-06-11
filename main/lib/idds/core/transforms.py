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
operations related to Transform.
"""

from idds.common import exceptions

from idds.common.constants import (TransformStatus,
                                   TransformLocking,
                                   CollectionStatus,
                                   ContentStatus,
                                   ProcessingStatus)
from idds.orm.base.session import read_session, transactional_session
from idds.orm import (transforms as orm_transforms,
                      collections as orm_collections,
                      contents as orm_contents,
                      messages as orm_messages,
                      processings as orm_processings)


@transactional_session
def add_transform(transform_type, transform_tag=None, priority=0, status=TransformStatus.New, locking=TransformLocking.Idle,
                  retries=0, expired_at=None, transform_metadata=None, request_id=None, collections=None, session=None):
    """
    Add a transform.

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
    if collections is None or len(collections) == 0:
        msg = "Transform must have collections, such as input collection, output collection and log collection"
        raise exceptions.WrongParameterException(msg)
    transform_id = orm_transforms.add_transform(transform_type=transform_type, transform_tag=transform_tag,
                                                priority=priority, status=status, locking=locking, retries=retries,
                                                expired_at=expired_at, transform_metadata=transform_metadata,
                                                request_id=request_id, session=session)
    for collection in collections:
        collection['transform_id'] = transform_id
        orm_collections.add_collection(**collection, session=session)


@read_session
def get_transform(transform_id, to_json=False, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_id: Transform id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transform.
    """
    return orm_transforms.get_transform(transform_id=transform_id, to_json=to_json, session=session)


@read_session
def get_transforms_with_input_collection(transform_type, transform_tag, coll_scope, coll_name, to_json=False, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param coll_scope: The collection scope.
    :param coll_name: The collection name.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transforms.
    """
    return orm_transforms.get_transforms_with_input_collection(transform_type, transform_tag, coll_scope,
                                                               coll_name, to_json=to_json, session=session)


@read_session
def get_transform_ids(request_id, session=None):
    """
    Get transform ids or raise a NoObject exception.

    :param request_id: Request id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform ids.
    """
    return orm_transforms.get_transform_ids(request_id=request_id, session=session)


@read_session
def get_transforms(request_id, to_json=False, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param request_id: Request id.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform.
    """
    return orm_transforms.get_transforms(request_id=request_id, to_json=to_json, session=session)


@read_session
def get_transforms_by_status(status, period=None, locking=False, bulk_size=None, to_json=False, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param status: Transform status or list of transform status.
    :param session: The database session in use.
    :param locking: Whether to lock retrieved items.
    :param to_json: return json format.

    :raises NoObject: If no transform is founded.

    :returns: list of transform.
    """
    transforms = orm_transforms.get_transforms_by_status(status=status, period=period, locking=locking,
                                                         bulk_size=bulk_size, to_json=to_json, session=session)
    if locking:
        parameters = {'locking': TransformLocking.Locking}
        for transform in transforms:
            orm_transforms.update_transform(transform_id=transform['transform_id'], parameters=parameters, session=session)
    return transforms


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
    orm_transforms.update_transform(transform_id=transform_id, parameters=parameters, session=session)


@transactional_session
def add_transform_outputs(transform, input_collection, output_collection, input_contents, output_contents,
                          processing, to_cancel_processing=None, messages=None, message_bulk_size=1000, session=None):
    """
    For input contents, add corresponding output contents.

    :param transform: the transform.
    :param input_collection: The input collection.
    :param output_collection: The output collection.
    :param input_contents: The input contents.
    :param output_contents: The corresponding output contents.
    :param session: The database session in use.

    :raises DatabaseException: If there is a database error.
    """
    if output_contents:
        orm_contents.add_contents(output_contents, session=session)

    if messages:
        if not type(messages) in [list, tuple]:
            messages = [messages]
        for message in messages:
            orm_messages.add_message(msg_type=message['msg_type'],
                                     status=message['status'],
                                     source=message['source'],
                                     transform_id=message['transform_id'],
                                     num_contents=message['num_contents'],
                                     msg_content=message['msg_content'],
                                     bulk_size=message_bulk_size,
                                     session=session)

    if input_contents:
        update_input_contents = []
        for input_content in input_contents:
            update_input_content = {'content_id': input_content['content_id'],
                                    'status': ContentStatus.Mapped,
                                    'path': None}
            update_input_contents.append(update_input_content)
        if update_input_contents:
            orm_contents.update_contents(update_input_contents, with_content_id=True, session=session)

    if output_collection:
        # TODO, the status and new_files should be updated
        orm_collections.update_collection(output_collection['coll_id'],
                                          {'status': CollectionStatus.Processing},
                                          session=session)

    if to_cancel_processing:
        to_cancel_params = {'status': ProcessingStatus.Cancel}
        for to_cancel_id in to_cancel_processing:
            orm_processings.update_processing(processing_id=to_cancel_id, parameters=to_cancel_params, session=session)
    processing_id = None
    if processing:
        processing_id = orm_processings.add_processing(**processing, session=session)

    if transform:
        if processing_id is not None:
            if not transform['transform_metadata']:
                transform['transform_metadata'] = {'processing_id': processing_id}
            else:
                transform['transform_metadata']['processing_id'] = processing_id

        parameters = {'status': transform['status'],
                      'locking': transform['locking'],
                      'transform_metadata': transform['transform_metadata']}
        orm_transforms.update_transform(transform_id=transform['transform_id'],
                                        parameters=parameters,
                                        session=session)


@transactional_session
def delete_transform(transform_id=None, session=None):
    """
    delete a transform.

    :param transform_id: The id of the transform.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.
    """
    orm_transforms.delete_transform(transform_id=transform_id, session=session)


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    orm_transforms.clean_locking(time_period=time_period, session=session)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the transform
    """
    orm_transforms.clean_next_poll_at(status=status, session=session)
