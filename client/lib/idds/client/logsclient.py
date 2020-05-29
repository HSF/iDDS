#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


"""
Cacher Rest client to access IDDS system.
"""

import os
import requests

from idds.common import exceptions
from idds.client.base import BaseRestClient


class LogsClient(BaseRestClient):

    """logs Rest client"""

    LOGS_BASEURL = 'logs'

    def __init__(self, host=None, client_proxy=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(LogsClient, self).__init__(host=host, client_proxy=client_proxy, timeout=timeout)

    def download_logs(self, workload_id=None, request_id=None, dest_dir='./', filename=None):
        """
        Donwload log files.

        :param workload_id: The workload id.
        :param request_id: The request id.

        :raise exceptions if it's not downloaded successfully.
        """
        def_filename = 'logs'
        if workload_id:
            def_filename = def_filename + "_" + str(workload_id)
        if request_id:
            def_filename = def_filename + "_" + str(request_id)
        def_filename = def_filename + ".tar.gz"

        if not workload_id:
            workload_id = 'null'
        if not request_id:
            request_id = 'null'

        if workload_id == 'null' and request_id == 'null':
            raise exceptions.IDDSException("One of workload_id and request_id should not be None or empty")

        path = os.path.join(self.LOGS_BASEURL, str(workload_id), str(request_id))
        url = self.build_url(self.host, path=path)

        """
        ret = self.get_request_response(url, type='GET')
        content = ret['content']
        bytes = ret['bytes']
        with open(filename, 'w') as fp:
            fp.write(content)
        """
        response = requests.get(url, verify=False)
        # print(response)
        # print(response.text)
        # print(response.content)
        # print(response.headers)

        if not filename:
            if response.headers and 'Content-Disposition' in response.headers:
                cont_desc = response.headers['Content-Disposition']
                for cont in cont_desc.split(";"):
                    cont = cont.strip()
                    if 'filename' in cont:
                        filename = cont.replace("filename=", "")
        if not filename:
            filename = def_filename
        filename = os.path.join(dest_dir, filename)

        if response and response.text:
            with open(filename, 'wb') as fp:
                fp.write(response.content)
            return filename
        return None
