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

from idds.core import catalog


def get_collections(scope, name, request_id=None, workload_id=None):
    """
    Get collections by scope, name, request_id and workload id.

    :param scope: scope of the collection.
    :param name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.

    :returns: dict of collections
    """
    return catalog.get_collections(scope=scope, name=name, request_id=request_id,
                                   workload_id=workload_id)


def get_contents(coll_scope=None, coll_name=None, request_id=None, workload_id=None, relation_type=None):
    """
    Get contents with collection scope, collection name, request id, workload id and relation type.

    :param coll_scope: scope of the collection.
    :param coll_name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param relation_type: The relation type between the collection and transform: input, outpu, logs and etc.

    :returns: dict of contents
    """
    return catalog.get_contents(coll_scope=coll_scope, coll_name=coll_name, request_id=request_id,
                                workload_id=workload_id, relation_type=relation_type)


def register_output_contents(coll_scope, coll_name, contents, request_id=None, workload_id=None):
    """
    register contents with collection scope, collection name, request id, workload id and contents.

    :param coll_scope: scope of the collection.
    :param coll_name: name the the collection.
    :param request_id: the request id.
    :param workload_id: The workload_id of the request.
    :param contents: list of contents [{'scope': <scope>, 'name': <name>, 'min_id': min_id, 'max_id': max_id, 'path': <path>}].
    """
    catalog.register_output_contents(coll_scope=coll_scope, coll_name=coll_name, contents=contents,
                                     request_id=request_id, workload_id=workload_id)


def get_match_contents(coll_scope, coll_name, scope, name, min_id=None, max_id=None,
                       request_id=None, workload_id=None, only_return_best_match=False):
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

    :returns: list of contents
    """
    return catalog.get_match_contents(coll_scope=coll_scope, coll_name=coll_name, scope=scope, name=name, min_id=min_id,
                                      max_id=max_id, request_id=request_id, workload_id=workload_id,
                                      only_return_best_match=only_return_best_match)
