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


class MessageClient(BaseRestClient):

    """Message Rest client"""

    MESSAGE_BASEURL = 'message'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(MessageClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def send_messages(self, request_id=None, workload_id=None, transform_id=None, internal_id=None, msgs=None):
        """
        Send messages to the Head service.

        :param request_id: the request id.
        :param workload_id: the workload id.
        :param transform_id: the transform id.
        :param internal_id: the internal id.
        :param msg: the list of message contents.

        :raise exceptions if it's not got successfully.
        """
        path = self.MESSAGE_BASEURL
        if request_id is None:
            request_id = 'null'
        if workload_id is None:
            workload_id = 'null'
        if transform_id is None:
            transform_id = 'null'
        if internal_id is None:
            internal_id = 'null'
        url = self.build_url(self.host, path=os.path.join(path, str(request_id), str(workload_id), str(transform_id), str(internal_id)))

        if msgs is None:
            raise Exception("Message is None")

        if type(msgs) not in (list, tuple):
            msgs = [msgs]
        self.get_request_response(url, type='POST', data=msgs)

        return None

    def get_messages(self, request_id=None, workload_id=None, transform_id=None, internal_id=None):
        """
        Get message from the Head service.

        :param request_id: the request id.
        :param workload_id: the workload id.
        :param transform_id: the transform id.
        :param internal_id: the internal id.

        :raise exceptions if it's not got successfully.
        """
        path = self.MESSAGE_BASEURL
        if request_id is None:
            request_id = 'null'
        if workload_id is None:
            workload_id = 'null'

        url = self.build_url(self.host, path=os.path.join(path, str(request_id), str(workload_id), str(transform_id), str(internal_id)))

        msgs = self.get_request_response(url, type='GET')

        return msgs
