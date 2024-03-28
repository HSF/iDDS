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
Metainfo Rest client to access IDDS system.
"""

import os

from idds.client.base import BaseRestClient


class MetaInfoClient(BaseRestClient):

    """MetaInfo Rest client"""

    METAINFO_BASEURL = 'metainfo'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(MetaInfoClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def get_metainfo(self, name):
        """
        Get meta info

        :param name: name to select different meta info

        :raise exceptions if it's not got successfully.
        """
        path = self.METAINFO_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(name)))

        meta_info = self.get_request_response(url, type='GET')
        return meta_info
