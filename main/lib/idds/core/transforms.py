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

from idds.common.constants import TransformStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm import transforms as orm_transforms
from idds.orm import collections as orm_collections


@transactional_session
def add_transform(transform_type, transform_tag=None, priority=0, status=TransformStatus.New, retries=0,
                  expired_at=None, transform_metadata=None, request_id=None, collections=None,
                  output_collection=None, log_collection=None, session=None):
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
def delete_transform(transform_id=None, session=None):
    """
    delete a transform.

    :param transform_id: The id of the transform.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.
    """
    orm_transforms.delete_transform(transform_id=transform_id, session=session)
