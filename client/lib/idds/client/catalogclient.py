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
Request Rest client to access IDDS catalog system.
"""

import os
from enum import Enum

from idds.client.base import BaseRestClient


class CatalogClient(BaseRestClient):

    """Catalog Rest client"""

    CATALOG_BASEURL = 'catalog'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(CatalogClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def get_collections(self, scope=None, name=None, request_id=None, workload_id=None, relation_type=None):
        """
        Get collections from the Head service.

        :param scope: the collection scope.
        :param name: the collection name, can be wildcard.
        :param request_id: the request id.
        :param workload_id: the workload id.
        :param relation_type: The relation_type of the request (input/output/log).
        :raise exceptions if it's not got successfully.
        """
        path = os.path.join(self.CATALOG_BASEURL, 'collections')
        if scope is None:
            scope = 'null'
        if name is None:
            name = 'null'
        if request_id is None:
            request_id = 'null'
        if workload_id is None:
            workload_id = 'null'
        if relation_type is None:
            relation_type = 'null'
        elif isinstance(relation_type, Enum):
            relation_type = relation_type.value

        url = self.build_url(self.host, path=os.path.join(path, scope, name, str(request_id), str(workload_id),
                                                          str(relation_type)))

        collections = self.get_request_response(url, type='GET')
        return collections

    def get_contents(self, coll_scope=None, coll_name=None, request_id=None, workload_id=None,
                     relation_type=None, status=None):
        """
        Get contents from the Head service.

        :param coll_scope: the collection scope.
        :param coll_name: the collection name, can be wildcard.
        :param request_id: the request id.
        :param workload_id: the workload id.
        :param relation_type: the relation between the collection and the transform(input, output, log)
        :param status: The content status.

        :raise exceptions if it's not got successfully.
        """
        path = os.path.join(self.CATALOG_BASEURL, 'contents')
        if coll_scope is None:
            coll_scope = 'null'
        if coll_name is None:
            coll_name = 'null'
        if request_id is None:
            request_id = 'null'
        if workload_id is None:
            workload_id = 'null'
        if relation_type is None:
            relation_type = 'null'
        elif isinstance(relation_type, Enum):
            relation_type = relation_type.value
        if status is None:
            status = 'null'
        elif isinstance(status, Enum):
            status = status.value

        url = self.build_url(self.host, path=os.path.join(path, coll_scope, coll_name, str(request_id),
                                                          str(workload_id), str(relation_type), str(status)))

        contents = self.get_request_response(url, type='GET')
        return contents

    def get_match_contents(self, coll_scope=None, coll_name=None, scope=None, name=None, min_id=None, max_id=None, request_id=None, workload_id=None, only_return_best_match=None):
        """
        Get contents from the Head service.

        :param coll_scope: the collection scope.
        :param coll_name: the collection name.
        :param scope: the content scope.
        :param name: the content name.
        :param min_id: the content min_id.
        :param max_id: the content max_id.
        :param request_id: the request id.
        :param workload_id: the workload id.
        :param only_return_best_match: only return the best match content if True.

        :raise exceptions if it's not got successfully.
        """
        path = self.CATALOG_BASEURL
        if coll_scope is None:
            coll_scope = 'null'
        if coll_name is None:
            coll_name = 'null'
        if scope == 'null':
            scope = None
        if name == 'null':
            name = None
        if min_id == 'null':
            min_id = None
        if max_id == 'null':
            max_id = None
        if request_id == 'null':
            request_id = None
        if workload_id == 'null':
            workload_id = None
        if only_return_best_match == 'null':
            only_return_best_match = None

        url = self.build_url(self.host, path=os.path.join(path, coll_scope, coll_name, scope, name, str(min_id), str(max_id), str(request_id), str(workload_id), str(only_return_best_match)))

        contents = self.get_request_response(url, type='GET')

        return contents

    def register_contents(self, coll_scope, coll_name, request_id, workload_id, contents):
        """
        register contents..

        :param coll_scope: the collection scope.
        :param coll_name: the collection name, can be wildcard.
        :param request_id: the request id.
        :param workload_id: the workload id.
        :param contents: list of contents.

        :raise exceptions if errors.
        """
        path = self.CATALOG_BASEURL
        # url = self.build_url(self.host, path=path + '/')
        url = self.build_url(self.host, path=os.path.join(path, coll_scope, coll_name, str(request_id), str(workload_id)))

        if not isinstance(contents, (list, tuple)):
            contents = [contents]

        for i in range(len(contents)):
            for key in contents[i]:
                if contents[i][key] is not None and isinstance(contents[i][key], Enum):
                    contents[i][key] = contents[i][key].value

        r = self.get_request_response(url, type='POST', data=contents)
        return r

    def get_contents_output_ext(self, request_id=None, workload_id=None, transform_id=None, group_by_jedi_task_id=False):
        """
        Get output extension contents from the Head service.

        :param request_id: the request id.
        :param workload_id: the workload id.
        :param transform_id: the transform id.

        :raise exceptions if it's not got successfully.
        """
        path = os.path.join(self.CATALOG_BASEURL, 'contents_output_ext')
        if request_id is None:
            request_id = 'null'
        if workload_id is None:
            workload_id = 'null'
        if transform_id is None:
            transform_id = 'null'

        url = self.build_url(self.host, path=os.path.join(path, str(request_id), str(workload_id),
                                                          str(transform_id), str(group_by_jedi_task_id)))

        contents = self.get_request_response(url, type='GET')
        return contents
