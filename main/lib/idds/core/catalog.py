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
from idds.common.constants import CollectionRelationType, ContentStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm import (requests as orm_requests,
                      collections as orm_collections,
                      contents as orm_contents)


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
    collections = orm_collections.get_collections_by_request_transform_id(request_id=request_id,
                                                                          session=session)
    rets = {}
    for collection in collections:
        request_id = collection['request_id']
        if request_id not in rets:
            rets[request_id] = {}
        transform_id = collection['transform_id']
        if transform_id not in rets[request_id]:
            rets[request_id][transform_id] = []
        rets[request_id][transform_id].append(collection)
    return rets


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

    collections = orm_collections.get_collections(scope=scope, name=name, request_id=request_id,
                                                  workload_id=workload_id, session=session)
    rets = {}
    for collection in collections:
        request_id = collection['request_id']
        if request_id not in rets:
            rets[request_id] = {}
        transform_id = collection['transform_id']
        if transform_id not in rets[request_id]:
            rets[request_id][transform_id] = []
        rets[request_id][transform_id].append(collection)
    return rets


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
                    relation_type = collection['relation_type']
                    scope_name = '%s:%s' % (scope, name)
                    contents = orm_contents.get_contents(coll_id=coll_id)
                    rets[request_id][transform_id][scope_name] = {'collection': collection,
                                                                  'relation_type': relation_type,
                                                                  'contents': contents}
    return rets


@transactional_session
def register_output_contents(coll_scope, coll_name, contents, request_id=None, workload_id=None, session=None):
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

    coll_id = orm_collections.get_collection_id_by_scope_name(coll_scope, coll_name, request_id, workload_id, session=session)

    content_keys = ['scope', 'name', 'min_id', 'max_id', 'status', 'path']
    parameters = []
    for content in contents:
        parameter = {}
        for key in content_keys:
            if key != 'path' and content[key] is None:
                raise exceptions.WrongParameterException("Content %s should not be None" % key)
            parameter[key] = content[key]
        if isinstance(parameter['status'], ContentStatus):
            parameter['status'] = parameter['status'].value
        parameter['coll_id'] = coll_id
        parameters.append(parameter)
    orm_contents.update_contents(parameters, session=session)


@read_session
def get_match_contents(coll_scope, coll_name, scope, name, min_id=None, max_id=None,
                       request_id=None, workload_id=None, only_return_best_match=False, session=None):
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

    coll_id = orm_collections.get_collection_id_by_scope_name(coll_scope, coll_name, request_id, workload_id)
    contents = orm_contents.get_match_contents(coll_id=coll_id, scope=scope, name=name, min_id=min_id, max_id=max_id)

    if not only_return_best_match:
        return contents

    if len(contents) == 1:
        return contents

    content = None
    for row in contents:
        if (not content) or (content['max_id'] - content['min_id'] > row['max_id'] - row['min_id']):
            content = row
    return [content]
