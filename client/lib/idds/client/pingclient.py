#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022


"""
Ping Rest client to access IDDS system.
"""

from idds.client.base import BaseRestClient
# from idds.common.constants import RequestType, RequestStatus


class PingClient(BaseRestClient):

    """Message Rest client"""

    PING_BASEURL = 'ping'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(PingClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def ping(self):
        """
        Ping the Head service.

        :raise exceptions if it's not got successfully.
        """
        path = self.PING_BASEURL
        url = self.build_url(self.host, path=path)

        msgs = self.get_request_response(url, type='GET')

        return msgs
