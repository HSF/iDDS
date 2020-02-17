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
operations related to collections.
"""

from idds.core import collections


def add_collection(scope, name, coll_type=None, request_id=None, transform_id=None,
                   relation_type=None, coll_size=0, status=None, total_files=0, retries=0,
                   expired_at=None, coll_metadata=None):
    """
    Add a collection.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param type: The type of dataset as dataset or container.
    :param request_id: The request id related to this collection.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param size: The size of the collection.
    :param status: The status.
    :param total_files: Number of total files.
    :param retries: Number of retries.
    :param expired_at: The datetime when it expires.
    :param coll_metadata: The metadata as json.

    :returns: collection id.
    """
    kwargs = {'scope': scope, 'name': name, 'coll_type': coll_type, 'request_id': request_id,
              'transform_id': transform_id, 'relation_type': relation_type, 'coll_size': coll_size,
              'status': status, 'total_files': total_files, 'retries': retries,
              'expired_at': expired_at, 'coll_metadata': coll_metadata}
    return collections.add_collection(**kwargs)


def get_collection(coll_id=None, transform_id=None, relation_type=None):
    """
    Get a collection or raise a NoObject exception.

    :param coll_id: The id of the collection.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.

    :returns: Collection.
    """
    return collections.get_collection(coll_id=coll_id, transform_id=transform_id, relation_type=relation_type)


def update_collection(coll_id, parameters):
    """
    update a collection.

    :param coll_id: the collection id.
    :param parameters: A dictionary of parameters.

    """
    return collections.update_collection(coll_id=coll_id, parameters=parameters)


def delete_collection(coll_id=None):
    """
    delete a collection.

    :param request_id: The id of the request.

    """
    return collections.delete_collection(coll_id=coll_id)
