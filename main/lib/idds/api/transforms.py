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

from idds.core import transforms


def add_transform(transform_type, transform_tag=None, priority=0, status=None, retries=0,
                  expired_at=None, transform_metadata=None, request_id=None):
    """
    Add a transform.

    :param transform_type: Transform type.
    :param transform_tag: Transform tag.
    :param priority: priority.
    :param status: Transform status.
    :param retries: The number of retries.
    :param expired_at: The datetime when it expires.
    :param transform_metadata: The metadata as json.

    :returns: content id.
    """
    kwargs = {'transform_type': transform_type, 'transform_tag': transform_tag, 'priority': priority,
              'status': status, 'retries': retries, 'expired_at': expired_at,
              'transform_metadata': transform_metadata, 'request_id': request_id}
    return transforms.add_transform(**kwargs)


def get_transform(transform_id):
    """
    Get transform.

    :param transform_id: Transform id.

    :returns: Transform.
    """
    return transforms.get_transform(transform_id)


def update_transform(transform_id, parameters):
    """
    update a transform.

    :param transform_id: the transform id.
    :param parameters: A dictionary of parameters.

    """
    return transforms.update_transform(transform_id, parameters)


def delete_transform(transform_id=None):
    """
    delete a transform.

    :param transform_id: The id of the transform.
    """
    return transforms.delete_transform(transform_id)
