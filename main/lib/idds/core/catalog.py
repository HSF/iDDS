#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2021


"""
operations related to Catalog(Collections and Contents).
"""


from idds.common import exceptions
from idds.common.constants import (CollectionType, CollectionStatus, CollectionLocking,
                                   CollectionRelationType, ContentStatus)
from idds.orm.base.session import read_session, transactional_session
from idds.orm import (transforms as orm_transforms,
                      collections as orm_collections,
                      contents as orm_contents,
                      messages as orm_messages)


@transactional_session
def get_collections_by_status(status, relation_type=CollectionRelationType.Input, time_period=None,
                              locking=False, bulk_size=None, to_json=False, session=None):
    """
    Get collections by status, relation_type and time_period or raise a NoObject exception.

    :param status: The collection status.
    :param relation_type: The relation_type of the collection to the transform.
    :param time_period: time period in seconds since last update.
    :param locking: Whether to retrieve unlocked files and lock them.
    :param to_json: return json format.

    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    colls = orm_collections.get_collections_by_status(status=status, relation_type=relation_type, bulk_size=bulk_size,
                                                      time_period=time_period, locking=locking, to_json=to_json,
                                                      session=session)

    if locking:
        parameters = {'locking': CollectionLocking.Locking}
        for coll in colls:
            orm_collections.update_collection(coll_id=coll['coll_id'], parameters=parameters, session=session)
    return colls


@read_session
def get_collections(scope=None, name=None, request_id=None, workload_id=None, transform_id=None,
                    relation_type=None, to_json=False, session=None):
    """
    Get collections by scope, name, request_id and workload id.

    :param scope: scope of the collection.
    :param name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param to_json: return json format.
    :param session: The database session in use.

    :returns: dict of collections
    """
    collections = orm_collections.get_collections(scope=scope, name=name, request_id=request_id,
                                                  workload_id=workload_id, transform_id=transform_id,
                                                  to_json=to_json,
                                                  relation_type=relation_type, session=session)
    return collections


@transactional_session
def add_collection(request_id, workload_id, scope, name, coll_type=CollectionType.Dataset, transform_id=None,
                   relation_type=CollectionRelationType.Input, bytes=0, status=CollectionStatus.New,
                   total_files=0, new_files=0, processing_files=0, processed_files=0, retries=0,
                   expired_at=None, coll_metadata=None, session=None):
    """
    Add a collection.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param coll_type: The type of dataset as dataset or container.
    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param bytes: The size of the collection.
    :param status: The status.
    :param total_files: Number of total files.
    :param retries: Number of retries.
    :param expired_at: The datetime when it expires.
    :param coll_metadata: The metadata as json.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: collection id.
    """
    orm_collections.add_collection(request_id=request_id, workload_id=workload_id,
                                   scope=scope, name=name, coll_type=coll_type,
                                   transform_id=transform_id, relation_type=relation_type,
                                   bytes=bytes, status=status, total_files=total_files,
                                   new_files=new_files, processing_files=processing_files,
                                   processed_files=processed_files, retries=retries, expired_at=expired_at,
                                   coll_metadata=coll_metadata, session=session)


@transactional_session
def update_collection(coll_id, parameters, msg=None, session=None):
    """
    update a collection.

    :param coll_id: the collection id.
    :param parameters: A dictionary of parameters.
    :param msg: messages.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.

    """
    orm_collections.update_collection(coll_id=coll_id, parameters=parameters, session=session)

    if msg:
        orm_messages.add_message(msg_type=msg['msg_type'],
                                 status=msg['status'],
                                 source=msg['source'],
                                 transform_id=msg['transform_id'],
                                 num_contents=msg['num_contents'],
                                 msg_content=msg['msg_content'],
                                 session=session)


@read_session
def get_collection(coll_id=None, transform_id=None, relation_type=None, to_json=False, session=None):
    """
    Get a collection or raise a NoObject exception.

    :param coll_id: The id of the collection.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Collection.
    """
    return orm_collections.get_collection(coll_id=coll_id, transform_id=transform_id,
                                          relation_type=relation_type, to_json=to_json,
                                          session=session)


@transactional_session
def add_contents(contents, bulk_size=1000, session=None):
    """
    Add contents.

    :param contents: dict of contents.
    :param bulk_size: bulk per insert to db.
    :param session: session.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: content id.
    """
    return orm_contents.add_contents(contents=contents, bulk_size=bulk_size,
                                     session=session)


@transactional_session
def update_input_collection_with_contents(coll, parameters, contents, bulk_size=1000, session=None):
    """
    update a collection.

    :param coll_id: the collection id.
    :param parameters: A dictionary of parameters.
    :param contents: dict of contents.
    :param bulk_size: bulk per insert to db.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.

    :returns new contents
    """
    new_files = 0
    processed_files = 0
    avail_contents = orm_contents.get_contents(coll_id=coll['coll_id'], session=session)
    avail_contents_dict = {}
    for content in avail_contents:
        key = '%s:%s:%s:%s' % (content['scope'], content['name'], content['min_id'], content['max_id'])
        avail_contents_dict[key] = content
        if content['status'] in [ContentStatus.Mapped, ContentStatus.Mapped.value]:
            processed_files += 1
        if content['status'] in [ContentStatus.New, ContentStatus.New.value]:
            new_files += 1

    to_addes = []
    # to_updates = []
    for content in contents:
        key = '%s:%s:%s:%s' % (content['scope'], content['name'], content['min_id'], content['max_id'])
        if key in avail_contents_dict:
            """
            to_update = {'content_id': content['content_id'],
                         'status': content['status']}
            if 'bytes' in content:
                to_update['bytes'] = content['bytes']
            if 'md5' in content:
                to_update['md5'] = content['md5']
            if 'adler32' in content:
                to_update['adler32'] = content['adler32']
            if 'expired_at' in content:
                to_update['expired_at'] = content['expired_at']
            to_updates.append(to_updated)
            # not to do anything, no need to update
            """
            pass
        else:
            to_addes.append(content)

    # there are new files
    if to_addes:
        add_contents(to_addes, bulk_size=bulk_size, session=session)

    if 'total_files' in parameters:
        total_files = parameters['total_files']
    else:
        total_files = coll['total_files']
    parameters['processed_files'] = processed_files
    parameters['new_files'] = new_files
    if processed_files == total_files:
        parameters['status'] = CollectionStatus.Closed

    update_collection(coll['coll_id'], parameters, session=session)
    return to_addes


@transactional_session
def update_contents(parameters, session=None):
    """
    updatecontents.

    :param parameters: list of dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    return orm_contents.update_contents(parameters, session=session)


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
    return orm_contents.update_content(content_id, parameters, session=session)


@read_session
def get_contents(coll_scope=None, coll_name=None, request_id=None, workload_id=None, transform_id=None,
                 relation_type=None, status=None, to_json=False, session=None):
    """
    Get contents with collection scope, collection name, request id, workload id and relation type.

    :param coll_scope: scope of the collection.
    :param coll_name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation type between the collection and transform: input, outpu, logs and etc.
    :param to_json: return json format.
    :param session: The database session in use.

    :returns: dict of contents
    """
    collections = get_collections(scope=coll_scope, name=coll_name, request_id=request_id,
                                  workload_id=workload_id, transform_id=transform_id,
                                  relation_type=relation_type, to_json=to_json, session=session)

    coll_ids = [coll['coll_id'] for coll in collections]
    if coll_ids:
        rets = orm_contents.get_contents(coll_id=coll_ids, status=status, to_json=to_json, session=session)
    else:
        rets = []
    return rets


@read_session
def get_contents_by_coll_id_status(coll_id, status=None, to_json=False, session=None):
    """
    Get contents or raise a NoObject exception.

    :param coll_id: Collection id.
    :param status: Content status or list of content status.
    :param to_json: return json format.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.

    :returns: list of contents.
    """
    return orm_contents.get_contents(coll_id=coll_id, status=status, to_json=to_json, session=session)


@transactional_session
def register_output_contents(coll_scope, coll_name, contents, request_id=None, workload_id=None,
                             relation_type=CollectionRelationType.Output, session=None):
    """
    register contents with collection scope, collection name, request id, workload id and contents.

    :param coll_scope: scope of the collection.
    :param coll_name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param contents: list of contents [{'scope': <scope>, 'name': <name>, 'min_id': min_id, 'max_id': max_id,
                                        'status': <status>, 'path': <path>}].
    :param session: The database session in use.
    """
    transform_ids = orm_transforms.get_transform_ids(request_id=request_id,
                                                     workload_id=workload_id,
                                                     session=session)

    if transform_ids:
        collections = orm_collections.get_collections(scope=coll_scope, name=coll_name, transform_id=transform_ids,
                                                      relation_type=relation_type, session=session)
    else:
        collections = []

    coll_def = "request_id=%s, workload_id=%s, coll_scope=%s" % (request_id, workload_id, coll_scope)
    coll_def += ", coll_name=%s, relation_type: %s" % (coll_name, relation_type)

    if len(collections) != 1:
        msg = "There should be only one collection matched. However there are %s collections" % len(collections)
        msg += coll_def
        raise exceptions.WrongParameterException(msg)

    coll_id = collections[0]['coll_id']

    keys = ['scope', 'name', 'min_id', 'max_id']
    for content in contents:
        ex_content = orm_contents.get_content(coll_id=coll_id, scope=content['scope'],
                                              name=content['name'], min_id=content['min_id'],
                                              max_id=content['max_id'], session=session)

        content_def = "scope: %s, name: %s, min_id: %s, max_id: %s" % (content['scope'],
                                                                       content['name'],
                                                                       content['min_id'],
                                                                       content['max_id'])

        if not ex_content:
            msg = "No matched content in collection(%s) with content(%s)" % (coll_def, content_def)
            raise exceptions.WrongParameterException(msg)

        for key in keys:
            if key in content:
                del content[key]
        content['content_id'] = ex_content['content_id']

    orm_contents.update_contents(contents, session=session)


@read_session
def get_match_contents(coll_scope, coll_name, scope, name, min_id=None, max_id=None,
                       request_id=None, workload_id=None, relation_type=None,
                       only_return_best_match=False, to_json=False, session=None):
    """
    Get matched contents with collection scope, collection name, scope, name, min_id, max_id,
    request id, workload id and only_return_best_match.

    :param coll_scope: scope of the collection.
    :param coll_name: name the the collection.
    :param scope: scope of the content.
    :param name: name of the content.
    :param min_id: min_id of the content.
    :param max_id: max_id of the content.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param only_return_best_match: only return best matched content if it's true.
    :param session: The database session in use.

    :returns: list of contents
    """
    transform_ids = orm_transforms.get_transform_ids(request_id=request_id,
                                                     workload_id=workload_id,
                                                     session=session)

    if transform_ids:
        collections = orm_collections.get_collections(scope=coll_scope, name=coll_name, transform_id=transform_ids,
                                                      relation_type=relation_type, session=session)
    else:
        collections = []

    coll_def = "request_id=%s, workload_id=%s, coll_scope=%s" % (request_id, workload_id, coll_scope)
    coll_def += ", coll_name=%s, relation_type: %s" % (coll_name, relation_type)

    if len(collections) != 1:
        msg = "There should be only one collection matched. However there are %s collections" % len(collections)
        msg += coll_def
        raise exceptions.WrongParameterException(msg)

    coll_id = collections[0]['coll_id']

    contents = orm_contents.get_match_contents(coll_id=coll_id, scope=scope, name=name, min_id=min_id, max_id=max_id,
                                               to_json=to_json, session=session)

    if not only_return_best_match:
        return contents

    if len(contents) == 1:
        return contents

    content = None
    for row in contents:
        if (not content) or (content['max_id'] - content['min_id'] > row['max_id'] - row['min_id']):
            content = row
    return [content]


@read_session
def get_content_status_statistics(coll_id=None, session=None):
    """
    Get statistics group by status

    :param coll_id: Collection id.
    :param session: The database session in use.

    :returns: statistics group by status, as a dict.
    """
    return orm_contents.get_content_status_statistics(coll_id=coll_id, session=session)


@transactional_session
def clean_locking(time_period=3600, session=None):
    """
    Clearn locking which is older than time period.

    :param time_period in seconds
    """
    orm_collections.clean_locking(time_period=time_period, session=session)


@transactional_session
def clean_next_poll_at(status, session=None):
    """
    Clearn next_poll_at.

    :param status: status of the collection
    """
    orm_collections.clean_next_poll_at(status=status, session=session)


@read_session
def get_output_content_by_request_id_content_name(request_id, content_scope, content_name, transform_id=None,
                                                  content_type=None, min_id=None, max_id=None, to_json=False,
                                                  session=None):
    """
    Get output content by request_id and content name

    :param request_id: requestn id.
    :param content_name: The name of the content.
    :param to_json: return json format.
    :param session: The database session in use.

    :returns: content of the output collection.
    """
    transform_ids = orm_transforms.get_transform_ids(request_id, session=session)

    found_transform_id = None
    if transform_ids:
        if len(transform_ids) == 1:
            found_transform_id = transform_ids[0]
        elif len(transform_ids) > 1 and transform_id is None:
            raise "Number of the transforms(%s) is bigger than 1 and transform id is not provided" % len(transform_ids)
        else:
            for tf_id in transform_ids:
                if tf_id == transform_id:
                    found_transform_id = tf_id
                    break

    coll_id = None
    if found_transform_id:
        coll_id = orm_collections.get_collection_id(transform_id=found_transform_id,
                                                    relation_type=CollectionRelationType.Output,
                                                    session=session)
    content = None
    if coll_id:
        content = orm_contents.get_content(coll_id=coll_id, scope=content_scope, name=content_name, content_type=content_type,
                                           min_id=min_id, max_id=max_id, to_json=to_json, session=session)
    return content


@read_session
def get_output_contents_by_request_id_status(request_id, name, content_status, limit, transform_id=None, to_json=False, session=None):
    """
    Get output content by request_id and content name

    :param request_id: requestn id.
    :param name: the content name.
    :param content_status: The content status.
    :param limit: limit number of contents.
    :param to_json: return json format.
    :param session: The database session in use.

    :returns: content of the output collection.
    """
    transform_ids = orm_transforms.get_transform_ids(request_id, session=session)

    found_transform_id = None
    if transform_ids:
        if len(transform_ids) == 1:
            found_transform_id = transform_ids[0]
        elif len(transform_ids) > 1 and transform_id is None:
            raise "Number of the transforms(%s) is bigger than 1 and transform id is not provided" % len(transform_ids)
        else:
            for tf_id in transform_ids:
                if tf_id == transform_id:
                    found_transform_id = tf_id
                    break

    coll_id = None
    if found_transform_id:
        coll_id = orm_collections.get_collection_id(transform_id=found_transform_id,
                                                    relation_type=CollectionRelationType.Output,
                                                    session=session)

    contents = []
    if coll_id:
        contents = orm_contents.get_contents(coll_id=coll_id, status=content_status, to_json=to_json, session=session)

    if name:
        new_contents = []
        for content in contents:
            if str(content['name']) == str(name):
                new_contents.append(content)
        contents = new_contents

    if contents and limit and len(contents) > limit:
        contents = contents[:limit]
    return contents
