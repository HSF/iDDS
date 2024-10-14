
import sys
import logging
import jwt
import json
import base64
import datetime

from idds.common.authentication import OIDCAuthenticationUtils

# from pandaclient.openidc_utils import OpenIdConnect_Utils
from pandaclient import Client


logging.basicConfig(stream=sys.stderr,
                    level=logging.DEBUG,
                    format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')

logger = logging.getLogger('my_logger')

token_file = '~/.idds/.token'

oidc_util = OIDCAuthenticationUtils()
status, token = oidc_util.load_token(token_file)
print(status)
print(token)

id_token = token['id_token']
enc = id_token.split('.')[1]
enc += '=' * (-len(enc) % 4)
dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
print(dec)
exp_time = datetime.datetime.utcfromtimestamp(dec['exp'])
print(exp_time)

refresh_token = token['refresh_token']
dec_refresh_token = jwt.decode(refresh_token, options={"verify_signature": False})
print(dec_refresh_token)


# open_id = OpenIdConnect_Utils(None, log_stream=None, verbose=verbose)
oidc = Client._Curl().get_oidc(logger)
print(oidc)

oidc.cleanup()
status, output = oidc.run_device_authorization_flow()
print(status)
print(output)
