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
operations related to Catalog(Collections and Contents).
"""


from idds.common import exceptions
from idds.common.constants import (CollectionType, CollectionStatus, CollectionLocking,
                                   CollectionRelationType, ContentStatus)
from idds.orm.base.session import read_session, transactional_session
from idds.orm import (requests as orm_requests,
                      transforms as orm_transforms,
                      collections as orm_collections,
                      contents as orm_contents,
                      messages as orm_messages)


@read_session
def get_collections_by_request(request_id=None, workload_id=None, session=None):
    """
    Get collections of a request.

    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :returns: dict of {'transform_id': []}
    """
    request_id = orm_requests.get_request_id(request_id, workload_id, session=session)
    transform_ids = orm_transforms.get_transform_ids(request_id, session=session)
    collections = orm_collections.get_collections_by_transform_ids(transform_ids=transform_ids,
                                                                   session=session)
    rets = {}
    for collection in collections:
        if request_id not in rets:
            rets[request_id] = {}
        transform_id = collection['transform_id']
        if transform_id not in rets[request_id]:
            rets[request_id][transform_id] = []
        rets[request_id][transform_id].append(collection)
    return rets


@read_session
def get_collections_by_request_transform_id(request_id=None, transform_id=None, session=None):
    """
    Get collections by request_id and transform id or raise a NoObject exception.

    :param request_id: The request id related to this collection.
    :param transform_id: The transform id related to this collection.
    :param session: The database session in use.
    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    if request_id is not None:
        request_id = orm_requests.get_request_id(request_id=request_id, session=session)
        transform_ids = orm_transforms.get_transform_ids(request_id, transform_id=transform_id, session=session)
    else:
        if transform_id is None:
            transform_ids = []
        else:
            transform_ids = [transform_id]

    if transform_ids:
        collections = orm_collections.get_collections_by_transform_ids(transform_ids=transform_ids,
                                                                       session=session)
    else:
        collections = []

    rets = {}
    for collection in collections:
        if request_id not in rets:
            rets[request_id] = {}
        transform_id = collection['transform_id']
        if transform_id not in rets[request_id]:
            rets[request_id][transform_id] = []
        rets[request_id][transform_id].append(collection)
    return rets


@transactional_session
def get_collections_by_status(status, relation_type=CollectionRelationType.Input, time_period=None,
                              locking=False, bulk_size=None, session=None):
    """
    Get collections by status, relation_type and time_period or raise a NoObject exception.

    :param status: The collection status.
    :param relation_type: The relation_type of the collection to the transform.
    :param time_period: time period in seconds since last update.
    :param locking: Whether to retrieve unlocked files and lock them.
    :param session: The database session in use.

    :raises NoObject: If no collections are founded.

    :returns: list of Collections.
    """
    colls = orm_collections.get_collections_by_status(status=status, relation_type=relation_type, bulk_size=bulk_size,
                                                      time_period=time_period, locking=locking, session=session)
    if locking:
        parameters = {'locking': CollectionLocking.Locking}
        for coll in colls:
            orm_collections.update_collection(coll_id=coll['coll_id'], parameters=parameters, session=session)
    return colls


@read_session
def get_collections(scope, name, request_id=None, workload_id=None, session=None):
    """
    Get collections by scope, name, request_id and workload id.

    :param scope: scope of the collection.
    :param name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param session: The database session in use.

    :returns: dict of collections
    """
    if scope is None and name is None:
        return get_collections_by_request(request_id=request_id, workload_id=workload_id, session=session)

    if request_id is None and workload_id is not None:
        request_id = orm_requests.get_request_id(request_id, workload_id, session=session)

    transform_ids = None
    if request_id:
        transform_ids = orm_transforms.get_transform_ids(request_id, session=session)

    collections = orm_collections.get_collections(scope=scope, name=name, transform_ids=transform_ids,
                                                  session=session)
    rets = {}
    for collection in collections:
        if request_id not in rets:
            rets[request_id] = {}
        transform_id = collection['transform_id']
        if transform_id not in rets[request_id]:
            rets[request_id][transform_id] = []
        rets[request_id][transform_id].append(collection)
    return rets


@transactional_session
def add_collection(scope, name, type=CollectionType.Dataset, transform_id=None,
                   relation_type=CollectionRelationType.Input, coll_size=0, status=CollectionStatus.New,
                   total_files=0, retries=0, expired_at=None, coll_metadata=None, session=None):
    """
    Add a collection.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param type: The type of dataset as dataset or container.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param size: The size of the collection.
    :param status: The status.
    :param total_files: Number of total files.
    :param retries: Number of retries.
    :param expired_at: The datetime when it expires.
    :param coll_metadata: The metadata as json.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: collection id.
    """
    orm_collections.add_collection(scope=scope, name=name, type=type,
                                   transform_id=transform_id, relation_type=relation_type,
                                   coll_size=coll_size, status=status, total_files=total_files,
                                   retries=retries, expired_at=expired_at, coll_metadata=coll_metadata,
                                   session=session)


@transactional_session
def update_collection(coll_id, parameters, session=None):
    """
    update a collection.

    :param coll_id: the collection id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.

    """
    orm_collections.update_collection(coll_id=coll_id, parameters=parameters, session=session)


@transactional_session
def update_collection_with_msg(coll_id, parameters, msg=None, session=None):
    """
    update a collection.

    :param coll_id: the collection id.
    :param parameters: A dictionary of parameters.
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
def get_collection(coll_id=None, transform_id=None, relation_type=None, session=None):
    """
    Get a collection or raise a NoObject exception.

    :param coll_id: The id of the collection.
    :param transform_id: The transform id related to this collection.
    :param relation_type: The relation between this collection and its transform,
                          such as Input, Output, Log and so on.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Collection.
    """
    return orm_collections.get_collection(coll_id=coll_id, transform_id=transform_id,
                                          relation_type=relation_type, session=session)


@transactional_session
def add_contents(contents, returning_id=False, bulk_size=100, session=None):
    """
    Add contents.

    :param contents: dict of contents.
    :param returning_id: whether to return id.
    :param bulk_size: bulk per insert to db.
    :param session: session.

    :raises DuplicatedObject: If a collection with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: content id.
    """
    return orm_contents.add_contents(contents=contents, returning_id=returning_id, bulk_size=bulk_size,
                                     session=session)


@transactional_session
def update_input_collection_with_contents(coll, parameters, contents, returning_id=False, bulk_size=100, session=None):
    """
    update a collection.

    :param coll_id: the collection id.
    :param parameters: A dictionary of parameters.
    :param contents: dict of contents.
    :param returning_id: whether to return id.
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
        add_contents(to_addes, returning_id=returning_id, bulk_size=bulk_size, session=session)

    parameters['processed_files'] = processed_files
    parameters['new_files'] = new_files
    if processed_files == coll['total_files']:
        parameters['status'] = CollectionStatus.Closed
    update_collection(coll['coll_id'], parameters, session=session)
    return to_addes


@transactional_session
def update_contents(parameters, with_content_id=False, session=None):
    """
    updatecontents.

    :param parameters: list of dictionary of parameters.
    :param with_content_id: whether content_id is included.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    """
    return orm_contents.update_contents(parameters, with_content_id=with_content_id, session=session)


@read_session
def get_contents(coll_scope=None, coll_name=None, request_id=None, workload_id=None, relation_type=None, session=None):
    """
    Get contents with collection scope, collection name, request id, workload id and relation type.

    :param coll_scope: scope of the collection.
    :param coll_name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param relation_type: The relation type between the collection and transform: input, outpu, logs and etc.
    :param session: The database session in use.

    :returns: dict of contents
    """
    if request_id is None and workload_id is None:
        raise exceptions.WrongParameterException("Either request_id or workload_id should not be None")

    req_transfomr_collections = get_collections(scope=coll_scope, name=coll_name, request_id=request_id,
                                                workload_id=workload_id, session=session)

    rets = {}
    for request_id in req_transfomr_collections:
        rets[request_id] = {}
        for transform_id in req_transfomr_collections[request_id]:
            rets[request_id][transform_id] = {}
            for collection in req_transfomr_collections[request_id][transform_id]:
                if relation_type is not None:
                    if isinstance(relation_type, CollectionRelationType):
                        relation_type = relation_type.value
                if relation_type is None or collection['relation_type'].value == relation_type:
                    scope = collection['scope']
                    name = collection['name']
                    coll_id = collection['coll_id']
                    coll_relation_type = collection['relation_type']
                    scope_name = '%s:%s' % (scope, name)
                    contents = orm_contents.get_contents(coll_id=coll_id, session=session)
                    rets[request_id][transform_id][scope_name] = {'collection': collection,
                                                                  'relation_type': coll_relation_type,
                                                                  'contents': contents}
    return rets


@read_session
def get_contents_by_coll_id_status(coll_id, status=None, session=None):
    """
    Get contents or raise a NoObject exception.

    :param coll_id: Collection id.
    :param status: Content status or list of content status.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.

    :returns: list of contents.
    """
    return orm_contents.get_contents(coll_id=coll_id, status=status, session=session)


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

    if (request_id is None and workload_id is None) or coll_scope is None or coll_name is None:
        msg = "Only one of (request_id, workload_id) can be None. All other parameters should not be None: "
        msg += "request_id=%s, workload_id=%s, coll_scope=%s, coll_name=%s" % (request_id, workload_id, coll_scope, coll_name)
        raise exceptions.WrongParameterException(msg)
    if request_id is None and workload_id is not None:
        request_id = orm_requests.get_request_id(request_id, workload_id, session=session)

    coll_id = orm_collections.get_collection_id_by_scope_name(coll_scope, coll_name, request_id, relation_type, session=session)

    parameters = []
    for content in contents:
        if 'status' not in content or content['status'] is None:
            raise exceptions.WrongParameterException("Content status is required and should not be None: %s" % content)
        if content['status'] in [ContentStatus.Available, ContentStatus.Available.value]:
            content_keys = ['scope', 'name', 'min_id', 'max_id', 'status', 'path']
        else:
            content_keys = ['scope', 'name', 'min_id', 'max_id', 'status']

        parameter = {}
        for key in content_keys:
            if content[key] is None:
                raise exceptions.WrongParameterException("Content %s should not be None" % key)
            parameter[key] = content[key]
        if isinstance(parameter['status'], ContentStatus):
            parameter['status'] = parameter['status'].value
        parameter['coll_id'] = coll_id
        parameters.append(parameter)
    orm_contents.update_contents(parameters, session=session)


@read_session
def get_match_contents(coll_scope, coll_name, scope, name, min_id=None, max_id=None,
                       request_id=None, workload_id=None, relation_type=None,
                       only_return_best_match=False, session=None):
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

    if (request_id is None and workload_id is None) or coll_scope is None or coll_name is None:
        msg = "Only one of (request_id, workload_id) can be None. All other parameters should not be None: "
        msg += "request_id=%s, workload_id=%s, coll_scope=%s, coll_name=%s" % (request_id, workload_id, coll_scope, coll_name)
        raise exceptions.WrongParameterException(msg)

    coll_id = orm_collections.get_collection_id_by_scope_name(coll_scope, coll_name, request_id, relation_type, session=session)
    contents = orm_contents.get_match_contents(coll_id=coll_id, scope=scope, name=name, min_id=min_id, max_id=max_id, session=session)

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
