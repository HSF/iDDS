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
operations related to Requests.
"""

import datetime

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.sql.expression import asc

from idds.common import exceptions
from idds.common.constants import (ContentType, ContentStatus, ContentLocking,
                                   ContentRelationType)
from idds.orm.base.session import read_session, transactional_session
from idds.orm.base import models


def create_content(request_id, workload_id, transform_id, coll_id, map_id, scope, name,
                   min_id, max_id, content_type=ContentType.File,
                   status=ContentStatus.New, content_relation_type=ContentRelationType.Input,
                   bytes=0, md5=None, adler32=None, processing_id=None, storage_id=None, retries=0,
                   locking=ContentLocking.Idle, path=None, expired_at=None, content_metadata=None):
    """
    Create a content.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: transform id.
    :param coll_id: collection id.
    :param map_id: The id to map inputs to outputs.
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

    :returns: content.
    """

    new_content = models.Content(request_id=request_id, workload_id=workload_id,
                                 transform_id=transform_id, coll_id=coll_id, map_id=map_id,
                                 scope=scope, name=name, min_id=min_id, max_id=max_id,
                                 content_type=content_type, content_relation_type=content_relation_type,
                                 status=status, bytes=bytes, md5=md5,
                                 adler32=adler32, processing_id=processing_id, storage_id=storage_id,
                                 retries=retries, path=path, expired_at=expired_at, locking=locking,
                                 content_metadata=content_metadata)
    return new_content


@transactional_session
def add_content(request_id, workload_id, transform_id, coll_id, map_id, scope, name, min_id=0, max_id=0,
                content_type=ContentType.File, status=ContentStatus.New, content_relation_type=ContentRelationType.Input,
                bytes=0, md5=None, adler32=None, processing_id=None, storage_id=None, retries=0,
                locking=ContentLocking.Idle, path=None, expired_at=None, content_metadata=None, session=None):
    """
    Add a content.

    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: transform id.
    :param coll_id: collection id.
    :param map_id: The id to map inputs to outputs.
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

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: content id.
    """

    try:
        new_content = create_content(request_id=request_id, workload_id=workload_id, transform_id=transform_id,
                                     coll_id=coll_id, map_id=map_id, content_relation_type=content_relation_type,
                                     scope=scope, name=name, min_id=min_id, max_id=max_id,
                                     content_type=content_type, status=status, bytes=bytes, md5=md5,
                                     adler32=adler32, processing_id=processing_id, storage_id=storage_id,
                                     retries=retries, path=path, expired_at=expired_at,
                                     content_metadata=content_metadata)
        new_content.save(session=session)
        content_id = new_content.content_id
        return content_id
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Content transform_id:map_id(%s:%s) already exists!: %s' %
                                          (transform_id, map_id, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@transactional_session
def add_contents(contents, bulk_size=10000, session=None):
    """
    Add contents.

    :param contents: dict of contents.
    :param session: session.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: content id.
    """
    default_params = {'request_id': None, 'workload_id': None,
                      'transform_id': None, 'coll_id': None, 'map_id': None,
                      'scope': None, 'name': None, 'min_id': 0, 'max_id': 0,
                      'content_type': ContentType.File, 'status': ContentStatus.New,
                      'locking': ContentLocking.Idle, 'content_relation_type': ContentRelationType.Input,
                      'bytes': 0, 'md5': None, 'adler32': None, 'processing_id': None,
                      'storage_id': None, 'retries': 0, 'path': None,
                      'expired_at': datetime.datetime.utcnow() + datetime.timedelta(days=30),
                      'content_metadata': None}

    for content in contents:
        for key in default_params:
            if key not in content:
                content[key] = default_params[key]

    sub_params = [contents[i:i + bulk_size] for i in range(0, len(contents), bulk_size)]

    try:
        for sub_param in sub_params:
            session.bulk_insert_mappings(models.Content, sub_param)
        content_ids = [None for _ in range(len(contents))]
        return content_ids
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Duplicated objects: %s' % (error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)


@read_session
def get_content(content_id=None, coll_id=None, scope=None, name=None, content_type=None, min_id=0, max_id=0, to_json=False, session=None):
    """
    Get contentor raise a NoObject exception.

    :param content_id: content id.
    :param coll_id: collection id.
    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param min_id: The minimal id of the content.
    :param max_id: The maximal id of the content.
    :param content_type: The type of the content.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no content is founded.

    :returns: Content.
    """

    try:
        if content_id:
            query = session.query(models.Content)\
                           .filter(models.Content.content_id == content_id)
        else:
            if content_type in [ContentType.File, ContentType.File.value]:
                query = session.query(models.Content)\
                               .with_hint(models.Content, "INDEX(CONTENTS CONTENTS_ID_NAME_IDX)", 'oracle')\
                               .filter(models.Content.coll_id == coll_id)\
                               .filter(models.Content.scope == scope)\
                               .filter(models.Content.name == name)\
                               .filter(models.Content.content_type == content_type)
            else:
                query = session.query(models.Content)\
                               .with_hint(models.Content, "INDEX(CONTENTS CONTENTS_ID_NAME_IDX)", 'oracle')\
                               .filter(models.Content.coll_id == coll_id)\
                               .filter(models.Content.scope == scope)\
                               .filter(models.Content.name == name)\
                               .filter(models.Content.min_id == min_id)\
                               .filter(models.Content.max_id == max_id)
                if content_type:
                    query = query.filter(models.Content.content_type == content_type)

        ret = query.first()
        if not ret:
            return None
        else:
            if to_json:
                return ret.to_dict_json()
            else:
                return ret.to_dict()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('content(content_id: %s, coll_id: %s, scope: %s, name: %s, content_type: %s, min_id: %s, max_id: %s) cannot be found: %s' %
                                  (content_id, coll_id, scope, name, content_type, min_id, max_id, error))
    except Exception as error:
        raise error


@read_session
def get_match_contents(coll_id, scope, name, content_type=None, min_id=None, max_id=None, to_json=False, session=None):
    """
    Get contents which matches the query or raise a NoObject exception.

    :param coll_id: collection id.
    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param min_id: The minimal id of the content.
    :param max_id: The maximal id of the content.
    :param content_type: The type of the content.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no content is founded.

    :returns: list of Content ids.
    """

    try:
        query = session.query(models.Content)\
                       .with_hint(models.Content, "INDEX(CONTENTS CONTENTS_ID_NAME_IDX)", 'oracle')\
                       .filter(models.Content.coll_id == coll_id)\
                       .filter(models.Content.scope == scope)\
                       .filter(models.Content.name.like(name.replace('*', '%')))

        if content_type is not None:
            query = query.filter(models.Content.content_tye == content_type)
        if min_id is not None:
            query = query.filter(models.Content.min_id <= min_id)
        if max_id is not None:
            query = query.filter(models.Content.max_id >= max_id)

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No match contents for (coll_id: %s, scope: %s, name: %s, content_type: %s, min_id: %s, max_id: %s): %s' %
                                  (coll_id, scope, name, content_type, min_id, max_id, error))
    except Exception as error:
        raise error


@read_session
def get_contents(scope=None, name=None, coll_id=None, status=None, to_json=False, session=None):
    """
    Get content or raise a NoObject exception.

    :param scope: The scope of the content data.
    :param name: The name of the content data.
    :param coll_id: list of Collection ids.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no content is founded.

    :returns: list of contents.
    """

    try:
        if status is not None:
            if not isinstance(status, (tuple, list)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]
        if coll_id is not None:
            if not isinstance(coll_id, (tuple, list)):
                coll_id = [coll_id]
            if len(coll_id) == 1:
                coll_id = [coll_id[0], coll_id[0]]

        query = session.query(models.Content)
        query = query.with_hint(models.Content, "INDEX(CONTENTS CONTENTS_ID_NAME_IDX)", 'oracle')

        if coll_id:
            query = query.filter(models.Content.coll_id.in_(coll_id))
        if scope:
            query = query.filter(models.Content.scope == scope)
        if name:
            query = query.filter(models.Content.name.like(name.replace('*', '%')))
        if status is not None:
            query = query.filter(models.Content.status.in_(status))

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No record can be found with (scope=%s, name=%s, coll_id=%s): %s' %
                                  (scope, name, coll_id, error))
    except Exception as error:
        raise error


@read_session
def get_contents_by_transform(transform_id, to_json=False, session=None):
    """
    Get content or raise a NoObject exception.

    :param transform_id: transform id.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no content is founded.

    :returns: list of contents.
    """

    try:
        query = session.query(models.Content)
        query = query.with_hint(models.Content, "INDEX(CONTENTS CONTENTS_REQ_TF_COLL_IDX)", 'oracle')
        query = query.filter(models.Content.transform_id == transform_id)
        query = query.order_by(asc(models.Content.map_id))

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No record can be found with (transform_id=%s): %s' %
                                  (transform_id, error))
    except Exception as error:
        raise error


@read_session
def get_input_contents(request_id, coll_id, name=None, to_json=False, session=None):
    """
    Get content or raise a NoObject exception.

    :param request_id: request id.
    :param coll_id: collection id.
    :param name: content name.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no content is founded.

    :returns: list of contents.
    """

    try:
        query = session.query(models.Content)
        query = query.with_hint(models.Content, "INDEX(CONTENTS CONTENTS_REQ_TF_COLL_IDX)", 'oracle')
        query = query.filter(models.Content.request_id == request_id)
        query = query.filter(models.Content.coll_id == coll_id)

        if name:
            query = query.filter(models.Content.name == name)
        # query = query.filter(models.Content.content_relation_type == ContentRelationType.Input)
        query = query.order_by(asc(models.Content.map_id))

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                if to_json:
                    rets.append(t.to_dict_json())
                else:
                    rets.append(t.to_dict())
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No record can be found with (transform_id=%s, coll_id=%s, name=%s): %s' %
                                  (request_id, coll_id, name, error))
    except Exception as error:
        raise error


@read_session
def get_content_status_statistics(coll_id=None, session=None):
    """
    Get statistics group by status

    :param coll_id: Collection id.
    :param session: The database session in use.

    :returns: statistics group by status, as a dict.
    """
    try:
        query = session.query(models.Content.status, func.count(models.Content.content_id))
        query = query.with_hint(models.Content, "INDEX(CONTENTS CONTENTS_REQ_TF_COLL_IDX)", 'oracle')
        if coll_id:
            query = query.filter(models.Content.coll_id == coll_id)
        query = query.group_by(models.Content.status)
        tmp = query.all()
        rets = {}
        if tmp:
            for status, count in tmp:
                rets[status] = count
        return rets
    except Exception as error:
        raise error


@transactional_session
def update_content(content_id, parameters, session=None):
    """
    update a content.

    :param content_id: the content id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        parameters['updated_at'] = datetime.datetime.utcnow()

        session.query(models.Content).filter_by(content_id=content_id)\
               .update(parameters, synchronize_session=False)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Content %s cannot be found: %s' % (content_id, error))


@transactional_session
def update_contents(parameters, session=None):
    """
    update contents.

    :param parameters: list of dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        for parameter in parameters:
            parameter['updated_at'] = datetime.datetime.utcnow()

        session.bulk_update_mappings(models.Content, parameters)
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Content cannot be found: %s' % (error))


@transactional_session
def delete_content(content_id=None, session=None):
    """
    delete a content.

    :param content_id: The id of the content.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.
    """
    try:
        session.query(models.Content).filter_by(content_id=content_id).delete()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Content %s cannot be found: %s' % (content_id, error))
