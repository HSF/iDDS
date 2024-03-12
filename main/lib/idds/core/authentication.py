#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@@cern.ch>, 2024

import base64
import json
import jwt

# from cryptography import x509
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from idds.common import authentication


def decode_value(val):
    if isinstance(val, str):
        val = val.encode()
    decoded = base64.urlsafe_b64decode(val + b'==')
    return int.from_bytes(decoded, 'big')


class OIDCAuthentication(authentication.OIDCAuthentication):
    def __init__(self, timeout=None):
        super(OIDCAuthentication, self).__init__(timeout=timeout)

    def get_public_key(self, token, jwks_uri, no_verify=False):
        headers = jwt.get_unverified_header(token)
        if headers is None or 'kid' not in headers:
            raise jwt.exceptions.InvalidTokenError('cannot extract kid from headers')
        kid = headers['kid']

        jwks = self.get_cache_value(jwks_uri)
        if not jwks:
            jwks_content = self.get_http_content(jwks_uri, no_verify=no_verify)
            jwks = json.loads(jwks_content)
            self.set_cache_value(jwks_uri, jwks)

        jwk = None
        for j in jwks.get('keys', []):
            if j.get('kid') == kid:
                jwk = j
        if jwk is None:
            raise jwt.exceptions.InvalidTokenError('JWK not found for kid={0}: {1}'.format(kid, str(jwks)))

        public_num = RSAPublicNumbers(n=decode_value(jwk['n']), e=decode_value(jwk['e']))
        public_key = public_num.public_key(default_backend())
        pem = public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo)
        return pem

    def verify_id_token(self, vo, token):
        try:
            auth_config, endpoint_config = self.get_auth_endpoint_config(vo)

            # check audience
            decoded_token = jwt.decode(token, verify=False, options={"verify_signature": False})
            audience = decoded_token['aud']
            if audience not in [auth_config['audience'], auth_config['client_id']]:
                # discovery_endpoint = auth_config['oidc_config_url']
                return False, "The audience %s of the token doesn't match vo configuration(client_id: %s)." % (audience, auth_config['client_id']), None

            public_key = self.get_public_key(token, endpoint_config['jwks_uri'], no_verify=auth_config['no_verify'])
            # decode token only with RS256
            if 'iss' in decoded_token and decoded_token['iss'] and decoded_token['iss'] != endpoint_config['issuer'] and endpoint_config['issuer'].startswith(decoded_token['iss']):
                # iss is missing the last '/' in access tokens
                issuer = decoded_token['iss']
            else:
                issuer = endpoint_config['issuer']

            decoded = jwt.decode(token, public_key, verify=True, algorithms='RS256',
                                 audience=audience, issuer=issuer)
            decoded['vo'] = vo
            if 'name' in decoded:
                username = decoded['name']
            else:
                username = None
            return True, decoded, username
        except Exception as error:
            return False, 'Failed to verify oidc token: ' + str(error), None


class OIDCAuthenticationUtils(authentication.OIDCAuthenticationUtils):
    def __init__(self):
        super(OIDCAuthenticationUtils, self).__init__()


class X509Authentication(authentication.X509Authentication):
    def __init__(self, timeout=None):
        super(X509Authentication, self).__init__(timeout=timeout)


def get_user_name_from_dn1(dn):
    return authentication.get_user_name_from_dn1(dn)


def get_user_name_from_dn2(dn):
    return authentication.get_user_name_from_dn2(dn)


def get_user_name_from_dn(dn):
    dn = get_user_name_from_dn1(dn)
    dn = get_user_name_from_dn2(dn)
    return dn


def authenticate_x509(vo, dn, client_cert):
    return authentication.authenticate_x509(vo, dn, client_cert)


def authenticate_oidc(vo, token):
    oidc_auth = OIDCAuthentication()
    status, data, username = oidc_auth.verify_id_token(vo, token)
    if status:
        return status, data, username
    else:
        return status, data, username


def authenticate_is_super_user(username, dn=None):
    return authentication.authenticate_is_super_user(username=username, dn=dn)
