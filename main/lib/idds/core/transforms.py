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

from idds.common import exceptions

from idds.common.constants import (TransformStatus,
                                   CollectionStatus,
                                   ContentStatus)
from idds.orm.base.session import read_session, transactional_session
from idds.orm import (transforms as orm_transforms,
                      collections as orm_collections,
                      contents as orm_contents,
                      processings as orm_processings)


@transactional_session
def add_transform(transform_type, transform_tag=None, priority=0, status=TransformStatus.New, retries=0,
                  expired_at=None, transform_metadata=None, request_id=None, collections=None, session=None):
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

    :returns: transform id.
    """
    if collections is None or len(collections) == 0:
        msg = "Transform must have collections, such as input collection, output collection and log collection"
        raise exceptions.WrongParameterException(msg)
    transform_id = orm_transforms.add_transform(transform_type=transform_type, transform_tag=transform_tag,
                                                priority=priority, status=status, retries=retries,
                                                expired_at=expired_at, transform_metadata=transform_metadata,
                                                request_id=request_id, session=session)
    for collection in collections:
        collection['transform_id'] = transform_id
        orm_collections.add_collection(**collection, session=session)


@read_session
def get_transform(transform_id, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_id: Transform id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transform.
    """
    return orm_transforms.get_transform(transform_id=transform_id, session=session)


@read_session
def get_transform_with_input_collection(transform_type, transform_tag, coll_scope, coll_name, session=None):
    """
    Get transform or raise a NoObject exception.

    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param coll_scope: The collection scope.
    :param coll_name: The collection name.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: Transform.
    """
    return orm_transforms.get_transform_with_input_collection(transform_type, transform_tag, coll_scope,
                                                              coll_name, session=session)


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
def get_transforms(request_id, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param request_id: Request id.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform.
    """
    return orm_transforms.get_transforms(request_id=request_id, session=session)


@read_session
def get_transforms_by_status(status, period=None, session=None):
    """
    Get transforms or raise a NoObject exception.

    :param status: Transform status or list of transform status.
    :param session: The database session in use.

    :raises NoObject: If no transform is founded.

    :returns: list of transform.
    """
    return orm_transforms.get_transforms_by_status(status=status, period=period, session=session)


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
def trigger_update_transform_status(transform_id, input_collection_changed=False,
                                    output_collection_changed=False, session=None):
    """
    update transform status based on input/output collection changes.

    :param transform_id: the transform id.
    :param input_collection_changed: Whether input collection is changed.
    :param output_collection_changed: Whether output collection is changed.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    if not input_collection_changed and not output_collection_changed:
        return

    transform = orm_transforms.get_transform(transform_id, session=session)
    status = transform['status']
    transform_metadata = transform['transform_metadata']

    if 'input_collection_changed' not in transform_metadata:
        transform_metadata['input_collection_changed'] = input_collection_changed
    else:
        transform_metadata['input_collection_changed'] = transform_metadata['input_collection_changed'] or input_collection_changed
    if 'output_collection_changed' not in transform_metadata:
        transform_metadata['output_collection_changed'] = output_collection_changed
    else:
        transform_metadata['output_collection_changed'] = transform_metadata['output_collection_changed'] or output_collection_changed

    if isinstance(status, TransformStatus):
        status = status.value

    new_status = status
    if input_collection_changed:
        if status in [TransformStatus.ToCancel.value, TransformStatus.Cancelling.value,
                      TransformStatus.Failed.value, TransformStatus.Cancelled.value]:
            new_status = status
        elif status in [TransformStatus.New.value, TransformStatus.Extend.value]:
            new_status = TransformStatus.Ready.value
        elif status in [TransformStatus.Transforming.value]:
            new_status = TransformStatus.Transforming.value
        elif status in [TransformStatus.Finished.value, TransformStatus.SubFinished.value]:
            new_status = TransformStatus.Transforming.value

    elif input_collection_changed or output_collection_changed:
        if status in [TransformStatus.ToCancel.value, TransformStatus.Cancelling.value,
                      TransformStatus.Failed.value, TransformStatus.Cancelled.value]:
            new_status = status
        else:
            new_status = TransformStatus.Transforming.value

    parameters = {'status': new_status, 'transform_metadata': transform_metadata}
    orm_transforms.update_transform(transform_id=transform_id, parameters=parameters, session=session)


@transactional_session
def add_transform_outputs(transform, input_collection, output_collection, input_contents, output_contents,
                          processing, session=None):
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
    orm_contents.add_contents(output_contents, session=session)

    update_input_contents = []
    for input_content in input_contents:
        update_input_content = {'content_id': input_content['content_id'],
                                'status': ContentStatus.Mapped,
                                'path': None}
        update_input_contents.append(update_input_content)
    if update_input_contents:
        orm_contents.update_contents(update_input_contents, with_content_id=True, session=session)

    orm_collections.update_collection(output_collection['coll_id'],
                                      {'coll_status': CollectionStatus.Processing},
                                      session=session)

    transform_metadata = transform['transform_metadata']
    transform_metadata['input_collection_changed'] = False
    parameters = {'status': TransformStatus.Transforming,
                  'transform_metadata': transform_metadata}
    orm_transforms.update_transform(transform_id=transform['transform_id'],
                                    parameters=parameters,
                                    session=session)

    if processing:
        orm_processings.add_processing(**processing)


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
