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
Auth Rest client to access IDDS system.
"""

import os

from idds.client.base import BaseRestClient
# from idds.common.constants import RequestType, RequestStatus


class AuthClient(BaseRestClient):

    """Authentication Rest client"""

    AUTH_BASEURL = 'auth'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(AuthClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def get_oidc_sign_url(self, vo):
        """
        Get url from the Head service for users to sign in.

        :param vo: the virtual organization.

        :raise exceptions if it's not got successfully.
        """
        path = self.AUTH_BASEURL + "/url"
        url = self.build_url(self.host, path=os.path.join(path, str(vo), str(self.auth_type)))

        sign_url = self.get_request_response(url, type='GET', auth_setup_step=True)

        return sign_url

    def get_id_token(self, vo, device_code, interval=5, expires_in=60):
        """
        Get token from the Head service.

        :param vo: the virtual organization.
        :param device_code: the device code.
        :param interval: the interval to poll the token.
        :param expires_in: the time in seconds to expire for polling.

        :raise exceptions if it's not got successfully.
        """
        path = self.AUTH_BASEURL + "/token"
        url = self.build_url(self.host, path=os.path.join(path, str(vo), str(device_code), str(interval), str(expires_in)))

        token = self.get_request_response(url, type='GET', auth_setup_step=True)

        return token

    def refresh_id_token(self, vo, refresh_token):
        """
        Refresh token from the Head service.

        :param vo: the virtual organization.
        :param refresh_token: the token from refreshing.

        :raise exceptions if it's not got successfully.
        """
        path = self.AUTH_BASEURL + "/token"
        url = self.build_url(self.host, path=os.path.join(path, str(vo)))

        data = {'refresh_token': refresh_token}
        token = self.get_request_response(url, type='POST', data=data)

        return token
