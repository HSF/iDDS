#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@@cern.ch>, 2021 - 2022

import datetime
import base64
import json
import jwt
import os
import re
import requests

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

try:
    # Python 2
    from urllib import urlencode
except ImportError:
    # Python 3
    from urllib.parse import urlencode
    raw_input = input

# from cryptography import x509
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE


def decode_value(val):
    if isinstance(val, str):
        val = val.encode()
    decoded = base64.urlsafe_b64decode(val + b'==')
    return int.from_bytes(decoded, 'big')


class BaseAuthentication(object):
    def __init__(self, timeout=None):
        self.timeout = timeout
        self.config = self.load_auth_server_config()
        self.max_expires_in = 60
        if self.config and self.config.has_section('common'):
            if self.config.has_option('common', 'max_expires_in'):
                self.max_expires_in = self.config.getint('common', 'max_expires_in')

    def load_auth_server_config(self):
        config = ConfigParser.SafeConfigParser()
        if os.environ.get('IDDS_AUTH_CONFIG', None):
            configfile = os.environ['IDDS_AUTH_CONFIG']
            if config.read(configfile) == [configfile]:
                return config

        configfiles = ['%s/etc/idds/auth/auth.cfg' % os.environ.get('IDDS_HOME', ''),
                       '/etc/idds/auth/auth.cfg', '/opt/idds/etc/idds/auth/auth.cfg',
                       '%s/etc/idds/auth/auth.cfg' % os.environ.get('VIRTUAL_ENV', '')]
        for configfile in configfiles:
            if config.read(configfile) == [configfile]:
                return config
        return config

    def get_allow_vos(self):
        section = 'common'
        allow_vos = []
        if self.config and self.config.has_section(section):
            if self.config.has_option(section, 'allow_vos'):
                allow_vos_temp = self.config.get(section, 'allow_vos')
                allow_vos_temp = allow_vos_temp.split(',')
                for t in allow_vos_temp:
                    t = t.strip()
                    allow_vos.append(t)
        return allow_vos


class OIDCAuthentication(BaseAuthentication):
    def __init__(self, timeout=None):
        super(OIDCAuthentication, self).__init__(timeout=timeout)

    def get_auth_config(self, vo):
        ret = {'vo': vo, 'oidc_config_url': None, 'client_id': None,
               'client_secret': None, 'audience': None}

        if self.config and self.config.has_section(vo):
            for name in ['oidc_config_url', 'client_id', 'client_secret', 'vo', 'audience']:
                if self.config.has_option(vo, name):
                    ret[name] = self.config.get(vo, name)
        return ret

    def get_http_content(self, url):
        try:
            r = requests.get(url, allow_redirects=True)
            return r.content
        except Exception as error:
            return False, 'Failed to get http content for %s: %s' (str(url), str(error))

    def get_endpoint_config(self, auth_config):
        content = self.get_http_content(auth_config['oidc_config_url'])
        endpoint_config = json.loads(content)
        # ret = {'token_endpoint': , 'device_authorization_endpoint': None}
        return endpoint_config

    def get_oidc_sign_url(self, vo):
        try:
            allow_vos = self.get_allow_vos()
            if vo not in allow_vos:
                return False, "VO %s is not allowed." % vo

            auth_config = self.get_auth_config(vo)
            endpoint_config = self.get_endpoint_config(auth_config)

            data = {'client_id': auth_config['client_id'],
                    'scope': "openid profile email offline_access",
                    'audience': auth_config['audience']}

            headers = {'content-type': 'application/x-www-form-urlencoded'}

            result = requests.session().post(endpoint_config['device_authorization_endpoint'],
                                             # data=json.dumps(data),
                                             urlencode(data).encode(),
                                             timeout=self.timeout,
                                             headers=headers)

            if result is not None:
                if result.status_code == HTTP_STATUS_CODE.OK and result.text:
                    return True, json.loads(result.text)
                else:
                    return False, "Failed to get oidc sign in URL (status: %s, text: %s)" % (result.status_code, result.text)
            else:
                return False, "Failed to get oidc sign in URL. Response is None."
        except requests.exceptions.ConnectionError as error:
            return False, 'Failed to get oidc sign in URL. ConnectionError: ' + str(error)
        except Exception as error:
            return False, 'Failed to get oidc sign in URL: ' + str(error)

    def get_id_token(self, vo, device_code, interval=5, expires_in=60):
        try:
            allow_vos = self.get_allow_vos()
            if vo not in allow_vos:
                return False, "VO %s is not allowed." % vo

            auth_config = self.get_auth_config(vo)
            endpoint_config = self.get_endpoint_config(auth_config)

            data = {'client_id': auth_config['client_id'],
                    'client_secret': auth_config['client_secret'],
                    'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                    'device_code': device_code}

            headers = {'content-type': 'application/x-www-form-urlencoded'}

            if not interval:
                interval = 5
            interval = int(interval)

            if not expires_in:
                expires_in = 60
            expires_in = int(expires_in)
            if expires_in > self.max_expires_in:
                expires_in = self.max_expires_in

            result = requests.session().post(endpoint_config['token_endpoint'],
                                             # data=json.dumps(data),
                                             urlencode(data).encode(),
                                             timeout=self.timeout,
                                             headers=headers)
            if result is not None:
                if result.status_code == HTTP_STATUS_CODE.OK and result.text:
                    return True, json.loads(result.text)
                else:
                    return False, json.loads(result.text)
            else:
                return False, None
        except Exception as error:
            return False, 'Failed to get oidc token: ' + str(error)

    def refresh_id_token(self, vo, refresh_token):
        try:
            allow_vos = self.get_allow_vos()
            if vo not in allow_vos:
                return False, "VO %s is not allowed." % vo

            auth_config = self.get_auth_config(vo)
            endpoint_config = self.get_endpoint_config(auth_config)

            data = {'client_id': auth_config['client_id'],
                    'client_secret': auth_config['client_secret'],
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token}

            headers = {'content-type': 'application/x-www-form-urlencoded'}

            result = requests.session().post(endpoint_config['token_endpoint'],
                                             # data=json.dumps(data),
                                             urlencode(data).encode(),
                                             timeout=self.timeout,
                                             headers=headers)

            if result is not None:
                if result.status_code == HTTP_STATUS_CODE.OK and result.text:
                    return True, json.loads(result.text)
                else:
                    return False, "Failed to refresh oidc token (status: %s, text: %s)" % (result.status_code, result.text)
            else:
                return False, "Failed to refresh oidc token. Response is None."
        except requests.exceptions.ConnectionError as error:
            return False, 'Failed to refresh oidc token. ConnectionError: ' + str(error)
        except Exception as error:
            return False, 'Failed to refresh oidc token: ' + str(error)

    def get_public_key(self, token, jwks_uri):
        headers = jwt.get_unverified_header(token)
        if headers is None or 'kid' not in headers:
            raise jwt.exceptions.InvalidTokenError('cannot extract kid from headers')
        kid = headers['kid']

        jwks_content = self.get_http_content(jwks_uri)
        jwks = json.loads(jwks_content)
        jwk = None
        for j in jwks.get('keys', []):
            if j.get('kid') == kid:
                jwk = j
        if jwk is None:
            raise jwt.exceptions.InvalidTokenError('JWK not found for kid={0}'.format(kid, str(jwks)))

        public_num = RSAPublicNumbers(n=decode_value(jwk['n']), e=decode_value(jwk['e']))
        public_key = public_num.public_key(default_backend())
        pem = public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo)
        return pem

    def verify_id_token(self, vo, token):
        try:
            allow_vos = self.get_allow_vos()
            if vo not in allow_vos:
                return False, "VO %s is not allowed." % vo, None

            auth_config = self.get_auth_config(vo)
            endpoint_config = self.get_endpoint_config(auth_config)

            # check audience
            decoded_token = jwt.decode(token, verify=False, options={"verify_signature": False})
            audience = decoded_token['aud']
            if auth_config['client_id'] != audience:
                # discovery_endpoint = auth_config['oidc_config_url']
                return False, "The audience of the token doesn't match vo configuration.", None

            public_key = self.get_public_key(token, endpoint_config['jwks_uri'])
            # decode token only with RS256
            decoded = jwt.decode(token, public_key, verify=True, algorithms='RS256',
                                 audience=audience, issuer=endpoint_config['issuer'])
            decoded['vo'] = vo
            if 'name' in decoded:
                username = decoded['name']
            else:
                username = None
            return True, decoded, username
        except Exception as error:
            return False, 'Failed to verify oidc token: ' + str(error), None


class OIDCAuthenticationUtils(object):
    def __init__(self):
        pass

    def save_token(self, path, token):
        try:
            with open(path, 'w') as f:
                f.write(json.dumps(token))
            return True, None
        except Exception as error:
            return False, "Failed to save token: %s" % str(error)

    def load_token(self, path):
        try:
            with open(path) as f:
                data = json.load(f)
            return True, data
        except Exception as error:
            return False, "Failed to load token: %s" % str(error)

    def is_token_expired(self, token):
        try:
            enc = token['id_token'].split('.')[1]
            enc += '=' * (-len(enc) % 4)
            dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
            exp_time = datetime.datetime.utcfromtimestamp(dec['exp'])
            # delta = exp_time - datetime.datetime.utcnow()
            if exp_time < datetime.datetime.utcnow():
                return True, None
            else:
                return False, None
        except Exception as error:
            return True, "Failed to parse token: %s" % str(error)

    def clean_token(self, path):
        try:
            os.remove(path)
            return True, None
        except Exception as error:
            return False, "Failed to clean token: %s" % str(error)

    def get_token_info(self, token):
        try:
            enc = token['id_token'].split('.')[1]
            enc += '=' * (-len(enc) % 4)
            dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
            exp_time = datetime.datetime.utcfromtimestamp(dec['exp'])
            delta = exp_time - datetime.datetime.utcnow()
            minutes = delta.total_seconds() / 60

            info = dec
            info['expire'] = exp_time
            info['expire_time'] = 'Token will expire in %s minutes' % minutes
            info['expire_at'] = 'Token will expire at {0} UTC'.format(exp_time.strftime("%Y-%m-%d %H:%M:%S"))
            return True, info
        except Exception as error:
            return True, "Failed to parse token: %s" % str(error)


class X509Authentication(BaseAuthentication):
    def __init__(self, timeout=None):
        super(X509Authentication, self).__init__(timeout=timeout)

    def get_ban_user_list(self):
        section = "Users"
        option = "ban_users"
        if self.config and self.config.has_section(section):
            if self.config.has_option(section, option):
                users = self.config.get(section, option)
                users = users.split(",")
                return users
        return []

    def get_allow_user_list(self):
        section = "Users"
        option = "allow_users"
        if self.config and self.config.has_section(section):
            if self.config.has_option(section, option):
                users = self.config.get(section, option)
                users = users.split(",")
                return users
        return []


# "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=wguan/CN=667815/CN=Wen Guan/CN=1883443395"
def get_user_name_from_dn1(dn):
    try:
        up = re.compile('/(DC|O|OU|C|L)=[^\/]+')        # noqa W605
        username = up.sub('', dn)
        up2 = re.compile('/CN=[0-9]+')
        username = up2.sub('', username)
        up3 = re.compile(' [0-9]+')
        username = up3.sub('', username)
        up4 = re.compile('_[0-9]+')
        username = up4.sub('', username)
        username = username.replace('/CN=proxy', '')
        username = username.replace('/CN=limited proxy', '')
        username = username.replace('limited proxy', '')
        username = re.sub('/CN=Robot:[^/]+', '', username)
        username = re.sub('/CN=nickname:[^/]+', '', username)
        pat = re.compile('.*/CN=([^\/]+)/CN=([^\/]+)')         # noqa W605
        mat = pat.match(username)
        if mat:
            username = mat.group(2)
        else:
            username = username.replace('/CN=', '')
        if username.lower().find('/email') > 0:
            username = username[:username.lower().find('/email')]
        pat = re.compile('.*(limited.*proxy).*')
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace('(', '')
        username = username.replace(')', '')
        username = username.replace("'", '')
        return username
    except Exception:
        return dn


# 'CN=203633261,CN=Wen Guan,CN=667815,CN=wguan,OU=Users,OU=Organic Units,DC=cern,DC=ch'
def get_user_name_from_dn2(dn):
    try:
        up = re.compile(',(DC|O|OU|C|L)=[^\,]+')        # noqa W605
        username = up.sub('', dn)
        up2 = re.compile(',CN=[0-9]+')
        username = up2.sub('', username)
        up3 = re.compile(' [0-9]+')
        username = up3.sub('', username)
        up4 = re.compile('_[0-9]+')
        username = up4.sub('', username)
        username = username.replace(',CN=proxy', '')
        username = username.replace(',CN=limited proxy', '')
        username = username.replace('limited proxy', '')
        username = re.sub(',CN=Robot:[^/]+', '', username)
        username = re.sub(',CN=nickname:[^/]+', '', username)
        pat = re.compile('.*,CN=([^\,]+),CN=([^\,]+)')         # noqa W605
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        else:
            username = username.replace(',CN=', '')
        if username.lower().find(',email') > 0:
            username = username[:username.lower().find(',email')]
        pat = re.compile('.*(limited.*proxy).*')
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace('(', '')
        username = username.replace(')', '')
        username = username.replace("'", '')
        return username
    except Exception:
        return dn


def get_user_name_from_dn(dn):
    dn = get_user_name_from_dn1(dn)
    dn = get_user_name_from_dn2(dn)
    return dn


def authenticate_x509(vo, dn, client_cert):
    if not dn:
        return False, "User DN cannot be found.", None
    if not client_cert:
        return False, "Client certificate proxy cannot be found.", None

    # certDecoded = x509.load_pem_x509_certificate(str.encode(client_cert), default_backend())
    # print(certDecoded.issuer)
    # for ext in certDecoded.extensions:
    #     print(ext)
    allow_user_list = X509Authentication().get_allow_user_list()
    matched = False
    for allow_user in allow_user_list:
        # pat = re.compile(allow_user)
        # mat = pat.match(dn)
        mat = dn.find(allow_user)
        if mat > -1:
            matched = True
            break

    if not matched:
        return False, "User %s is not allowed" % str(dn), None

    if matched:
        # username = get_user_name_from_dn(dn)
        ban_user_list = X509Authentication().get_ban_user_list()
        for ban_user in ban_user_list:
            pat = re.compile(ban_user)
            mat = pat.match(dn)
            if mat:
                return False, "User %s is banned" % str(dn), None
    username = get_user_name_from_dn(dn)
    return True, None, username


def authenticate_oidc(vo, token):
    oidc_auth = OIDCAuthentication()
    status, data, username = oidc_auth.verify_id_token(vo, token)
    if status:
        return status, data, username
    else:
        return status, data, username
