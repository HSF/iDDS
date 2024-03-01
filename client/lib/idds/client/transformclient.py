#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


"""
Request Rest client to access IDDS system.
"""

import os

from idds.client.base import BaseRestClient
# from idds.common.constants import RequestType, RequestStatus


class TransformClient(BaseRestClient):

    """Transform Rest client"""

    TRANSFORM_BASEURL = 'transform'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(TransformClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def add_transform(self, request_id, **kwargs):
        """
        Add transform to the Head service.

        :param kwargs: attributes of the request.

        :raise exceptions if it's not registerred successfully.
        """
        path = self.TRANSFORM_BASEURL
        # url = self.build_url(self.host, path=path + '/')
        url = self.build_url(self.host, path=os.path.join(path, str(request_id)))

        data = kwargs

        # if 'request_type' in data and data['request_type'] and isinstance(data['request_type'], RequestType):
        #     data['request_type'] = data['request_type'].value
        # if 'status' in data and data['status'] and isinstance(data['status'], RequestStatus):
        #     data['status'] = data['status'].value

        r = self.get_request_response(url, type='POST', data=data)
        return r['transform_id']

    def get_transforms(self, request_id=None):
        """
        Get transforms from the Head service.

        :param request_id: the request id.

        :raise exceptions if it's not got successfully.
        """
        path = self.TRANSFORM_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(request_id)))
        tfs = self.get_request_response(url, type='GET')
        return tfs

    def get_transform(self, request_id=None, transform_id=None):
        """
        Get transforms from the Head service.

        :param request_id: the request id.
        :param transform_id: the transform id.

        :raise exceptions if it's not got successfully.
        """
        path = self.TRANSFORM_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(request_id), str(transform_id)))
        tf = self.get_request_response(url, type='GET')
        return tf
