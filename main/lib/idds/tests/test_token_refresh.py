import os
import sys
import logging
import json
import base64
import datetime

from pandaclient import Client


logging.basicConfig(stream=sys.stderr,
                    level=logging.DEBUG,
                    format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')

logger = logging.getLogger('my_logger')


# open_id = OpenIdConnect_Utils(None, log_stream=None, verbose=verbose)
oidc = Client._Curl().get_oidc(logger)
oidc.verbose = True
print(oidc)


def get_id_token():
    # oidc.cleanup()
    # this one should automatically refresh token
    status, output = oidc.run_device_authorization_flow()
    print(status)
    print(output)
    return output


def show_token_info(id_token):
    enc = id_token.split('.')[1]
    enc += '=' * (-len(enc) % 4)
    dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
    print(dec)
    exp_time = datetime.datetime.utcfromtimestamp(dec['exp'])
    print(exp_time)


def get_refresh_token():
    # manually refresh token
    refresh_token = None
    token_file = oidc.get_token_path()
    if os.path.exists(token_file):
        with open(token_file) as f:
            data = json.load(f)
            if 'refresh_token' in data:
                refresh_token = data['refresh_token']
    return refresh_token


def get_config():
    auth_config = None
    endpoint_config = None
    # get auth config
    s, o = oidc.fetch_page(oidc.auth_config_url)
    if s:
        auth_config = o
        # get endpoint config
        s_1, o_1 = oidc.fetch_page(auth_config['oidc_config_url'])
        if s_1:
            endpoint_config = o_1
    return auth_config, endpoint_config


auth_config, endpoint_config = get_config()
# print(endpoint_config)
refresh_token = get_refresh_token()

if endpoint_config and auth_config and refresh_token:
    status, output = oidc.refresh_token(endpoint_config['token_endpoint'], auth_config['client_id'],
                                        auth_config['client_secret'], refresh_token)
    print(status)
    print(output)
    show_token_info(output)
else:
    output = get_id_token()
    print(output)
    show_token_info(output)
