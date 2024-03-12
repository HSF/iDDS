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
Main client class for IDDS Rest callings.
"""


import os
import warnings

from idds.common import exceptions
from idds.common.utils import get_proxy_path
from idds.client.requestclient import RequestClient
from idds.client.transformclient import TransformClient
from idds.client.catalogclient import CatalogClient
from idds.client.cacherclient import CacherClient
from idds.client.hpoclient import HPOClient
from idds.client.logsclient import LogsClient
from idds.client.messageclient import MessageClient
from idds.client.pingclient import PingClient
from idds.client.authclient import AuthClient


warnings.filterwarnings("ignore")


class Client(RequestClient, TransformClient, CatalogClient, CacherClient, HPOClient, LogsClient, MessageClient, PingClient, AuthClient):

    """Main client class for IDDS rest callings."""

    def __init__(self, host=None, timeout=600, auth=None, client_proxy=None):
        """
        Constructor for the IDDS main client class.

        :param host: the host of the IDDS system.
        :param timeout: the timeout of the request (in seconds).
        """

        # if client_proxy is None:
        #     client_proxy = self.get_user_proxy()
        super(Client, self).__init__(host=host, auth=auth, timeout=timeout)

    def get_user_proxy(sellf):
        """
        Get the user proxy.

        :returns: the path of the user proxy.
        """

        client_proxy = get_proxy_path()

        if not client_proxy or not os.path.exists(client_proxy):
            raise exceptions.RestException("Cannot find a valid x509 proxy.")

        return client_proxy
