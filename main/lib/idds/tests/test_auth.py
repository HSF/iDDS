#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021 - 2024


"""
Test authentication.
"""

try:
    from urllib import urlencode        # noqa F401
except ImportError:
    from urllib.parse import urlencode  # noqa F401
    raw_input = input

import datetime
import sys
import time

import unittest2 as unittest
# from nose.tools import assert_equal
from idds.common.utils import setup_logging
from idds.core.authentication import OIDCAuthentication


setup_logging(__name__)


class TestAuthentication(unittest.TestCase):

    def test_oidc_authentication(self):
        vo = 'panda_dev'

        oidc = OIDCAuthentication()
        allow_vos = oidc.get_allow_vos()
        # print(allow_vos)
        assert(vo in allow_vos)
        auth_config = oidc.get_auth_config(vo)
        # print(auth_config)
        assert('vo' in auth_config)
        assert(auth_config['vo'] == vo)

        endpoint_config = oidc.get_endpoint_config(auth_config)
        # print(endpoint_config)
        assert('token_endpoint' in endpoint_config)

        status, sign_url = oidc.get_oidc_sign_url(vo)
        # print(sign_url)
        assert('user_code' in sign_url)
        print(("Please go to {0} and sign in. "
               "Waiting until authentication is completed").format(sign_url['verification_uri_complete']))

        print('Ready to get ID token?')
        while True:
            sys.stdout.write("[y/n] \n")
            choice = raw_input().lower()
            if choice == 'y':
                break
            elif choice == 'n':
                print('aborted')
                return

        if 'interval' in sign_url:
            interval = sign_url['interval']
        else:
            interval = 5

        if 'expires_in' in sign_url:
            expires_in = sign_url['expires_in']
        else:
            expires_in = 60

        token = None
        start_time = datetime.datetime.utcnow()
        while datetime.datetime.utcnow() - start_time < datetime.timedelta(seconds=expires_in):
            try:
                status, output = oidc.get_id_token(vo, sign_url['device_code'])
                if status:
                    # print(output)
                    token = output
                    break
                else:
                    if type(output) in [dict] and 'error' in output and output['error'] == 'authorization_pending':
                        time.sleep(interval)
                    else:
                        print(output)
                        break
            except Exception as error:
                print(error)
                break

        if not token:
            print("Failed to get a token")
        else:
            print(token)
            assert('id_token' in token)

            status, new_token = oidc.refresh_id_token(vo, token['refresh_token'])
            # print(new_token)
            assert('id_token' in new_token)

            print("verifying the token")
            status, decoded_token, username = oidc.verify_id_token(vo, token['id_token'])
            if not status:
                print("Failed to verify the token: %s" % decoded_token)
            else:
                print(username)
                print(decoded_token)
