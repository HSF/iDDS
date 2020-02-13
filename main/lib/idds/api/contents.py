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
operations related to Requests.
"""

from idds.core import contents


def add_content(coll_id, scope, name, min_id, max_id, content_type=None, status=None,
                bytes=0, md5=None, adler32=None, processing_id=None, storage_id=None, retries=0,
                path=None, expired_at=None, collcontent_metadata=None):
    """
    Add a content.

    :param coll_id: collection id.
    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param min_id: The minimal id of the content.
    :param max_id: The maximal id of the content.
    :param content_type: The type of the content.
    :param status: content status.
    :param bytes: The size of the content.
    :param md5: md5 checksum.
    :param alder32: adler32 checksum.
    :param processing_id: The processing id.
    :param storage_id: The storage id.
    :param retries: The number of retries.
    :param path: The content path.
    :param expired_at: The datetime when it expires.
    :param content_metadata: The metadata as json.

    :returns: content id.
    """
    kwargs = {'coll_id': coll_id, 'scope': scope, 'name': name, 'min_id': min_id, 'max_id': max_id,
              'content_type': content_type, 'status': status, 'bytes': bytes,
              'md5': md5, 'adler32': adler32, 'processing_id': processing_id, 'storage_id': storage_id,
              'retries': retries, 'path': path, 'expired_at': expired_at,
              'collcontent_metadata': collcontent_metadata}
    return contents.add_content(kwargs)


def get_content(content_id=None, coll_id=None, scope=None, name=None, content_type=None, min_id=None, max_id=None):
    """
    Get content or raise a NoObject exception.

    :param content_id: Content id.
    :param coll_id: Collection id.
    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param min_id: The minimal id of the content.
    :param max_id: The maximal id of the content.
    :param content_type: The type of the content.

    :returns: Content.
    """
    return contents.get_content(content_id=content_id, coll_id=coll_id, scope=scope, name=name,
                                content_type=content_type, min_id=min_id, max_id=max_id)


def update_content(content_id, parameters):
    """
    update a content.

    :param content_id: the content id.
    :param parameters: A dictionary of parameters.

    """
    return contents.update_content(content_id, parameters)


def delete_content(content_id=None):
    """
    delete a content.

    :param content_id: The id of the content.

    """
    return contents.delete_content(content_id)
