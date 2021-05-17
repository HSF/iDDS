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
Request Rest client to access IDDS system.
"""

import os

from idds.client.base import BaseRestClient
# from idds.common.constants import RequestType, RequestStatus


class RequestClient(BaseRestClient):

    """Request Rest client"""

    REQUEST_BASEURL = 'request'

    def __init__(self, host=None, client_proxy=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(RequestClient, self).__init__(host=host, client_proxy=client_proxy, timeout=timeout)

    def add_request(self, **kwargs):
        """
        Add request to the Head service.

        :param kwargs: attributes of the request.

        :raise exceptions if it's not registerred successfully.
        """
        path = self.REQUEST_BASEURL
        # url = self.build_url(self.host, path=path + '/')
        url = self.build_url(self.host, path=path)

        data = kwargs

        # if 'request_type' in data and data['request_type'] and isinstance(data['request_type'], RequestType):
        #     data['request_type'] = data['request_type'].value
        # if 'status' in data and data['status'] and isinstance(data['status'], RequestStatus):
        #     data['status'] = data['status'].value

        r = self.get_request_response(url, type='POST', data=data)
        return r['request_id']

    def update_request(self, request_id, parameters):
        """
        Update Request to the Head service.

        :param request_id: the request.
        :param kwargs: other attributes of the request.

        :raise exceptions if it's not updated successfully.
        """
        path = self.REQUEST_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(request_id)))

        data = parameters
        # data['request_id'] = request_id
        # if 'request_type' in data and data['request_type'] is not None and isinstance(data['request_type'], RequestType):
        #     data['request_type'] = data['request_type'].value
        # if 'status' in data and data['status'] is not None and isinstance(data['status'], RequestStatus):
        #     data['status'] = data['status'].value

        # print('data: %s' % str(data))
        r = self.get_request_response(url, type='PUT', data=data)
        return r

    def get_requests(self, request_id=None, workload_id=None, with_detail=False, with_metadata=False):
        """
        Get request from the Head service.

        :param request_id: the request id.
        :param workload_id: the workload id.

        :raise exceptions if it's not got successfully.
        """
        path = self.REQUEST_BASEURL
        if request_id is None:
            request_id = 'null'
        if workload_id is None:
            workload_id = 'null'
        url = self.build_url(self.host, path=os.path.join(path, str(request_id), str(workload_id), str(with_detail), str(with_metadata)))

        requests = self.get_request_response(url, type='GET')

        # for request in requests:
        #     if request['request_type'] is not None:
        #         request['request_type'] = RequestType(request['request_type'])
        #     if request['status'] is not None:
        #         request['status'] = RequestStatus(request['status'])

        return requests
