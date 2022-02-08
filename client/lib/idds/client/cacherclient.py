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

from idds.client.base import BaseRestClient


class CacherClient(BaseRestClient):

    """cacher Rest client"""

    CACHER_BASEURL = 'cacher'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(CacherClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def upload(self, filename):
        """
        Upload to the Cacher service.

        :param filename: The name of the file.

        :raise exceptions if it's not uploaded successfully.
        """
        path = os.path.join(self.CACHER_BASEURL, os.path.basename(filename))
        url = self.build_url(self.host, path=path)

        with open(filename, 'rb') as fp:
            content = fp.read()

        # response = requests.post(
        #     '{}/files/newdata.csv'.format(API_URL), headers=headers, data=content

        # data = {'content': content, 'bytes': os.path.getsize(filename)}
        # data = {'content': base64.b64decode(content), 'bytes': os.path.getsize(filename)}
        # print(data)
        # data = {'bytes': os.path.getsize(filename)}
        # import json
        # print(json.dumps(data))
        # response = self.get_request_response(url, type='POST', data=data)
        requests.post(url, data=content, verify=False)
        return url

    def download(self, filename):
        """
        Donwload from the Cacher service.

        :param filename: The name of the file.

        :raise exceptions if it's not uploaded successfully.
        """
        path = os.path.join(self.CACHER_BASEURL, os.path.basename(filename))
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
        if response and response.text:
            with open(filename, 'wb') as fp:
                fp.write(response.content)
            return filename
        return None
