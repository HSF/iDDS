#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@@cern.ch>, 2021 - 2023

import datetime
import base64
import json
import os
import re
import requests
import time


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

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE


def decode_value(val):
    if isinstance(val, str):
        val = val.encode()
    decoded = base64.urlsafe_b64decode(val + b'==')
    return int.from_bytes(decoded, 'big')


def should_verify(no_verify=False, ssl_verify=None):
    if no_verify:
        return False
    if os.environ.get('IDDS_AUTH_NO_VERIFY', None):
        return False

    if os.environ.get('IDDS_AUTH_SSL_VERIFY', None):
        return os.environ.get('IDDS_AUTH_SSL_VERIFY', None)

    if ssl_verify:
        return ssl_verify

    return True


class Singleton(object):
    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
            class_._instance._initialized = False
        return class_._instance


class BaseAuthentication(Singleton):
    def __init__(self, timeout=None):
        self.timeout = timeout
        self.config = self.load_auth_server_config()
        self.max_expires_in = 60

        self.cache = {}
        self.cache_time = 3600 * 6

        if self.config and self.config.has_section('common'):
            if self.config.has_option('common', 'max_expires_in'):
                self.max_expires_in = self.config.getint('common', 'max_expires_in')

        if self.config and self.config.has_section('common'):
            if self.config.has_option('common', 'cache_time'):
                self.cache_time = self.config.getint('common', 'cache_time')

    def get_cache_value(self, key):
        if key in self.cache and self.cache[key]['time'] + self.cache_time > time.time():
            return self.cache[key]['value']
        return None

    def set_cache_value(self, key, value):
        cache_keys = list(self.cache.keys())
        for k in cache_keys:
            if self.cache[k]['time'] + self.cache_time <= time.time():
                del self.cache[k]
        self.cache[key] = {'time': time.time(), 'value': value}

    def load_auth_server_config(self):
        config = ConfigParser.ConfigParser()
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

    def get_ssl_verify(self):
        section = 'common'
        ssl_verify = None
        if self.config and self.config.has_section(section):
            if self.config.has_option(section, 'ssl_verify'):
                ssl_verify = self.config.get(section, 'ssl_verify')
        return ssl_verify


class OIDCAuthentication(BaseAuthentication):
    def __init__(self, timeout=None):
        super(OIDCAuthentication, self).__init__(timeout=timeout)

    def get_auth_config(self, vo):
        ret = self.get_cache_value(vo)
        if ret:
            return ret

        ret = {'vo': vo, 'oidc_config_url': None, 'client_id': None,
               'client_secret': None, 'audience': None, 'no_verify': False}

        if self.config and self.config.has_section(vo):
            for name in ['oidc_config_url', 'client_id', 'client_secret', 'vo', 'audience']:
                if self.config.has_option(vo, name):
                    ret[name] = self.config.get(vo, name)
            for name in ['no_verify']:
                if self.config.has_option(vo, name):
                    ret[name] = self.config.getboolean(vo, name)
        return ret

    def get_http_content(self, url, no_verify=False):
        try:
            r = requests.get(url, allow_redirects=True, verify=should_verify(no_verify, self.get_ssl_verify()))
            return r.content
        except Exception as error:
            return False, 'Failed to get http content for %s: %s' % (str(url), str(error))

    def get_endpoint_config(self, auth_config):
        content = self.get_http_content(auth_config['oidc_config_url'], no_verify=auth_config['no_verify'])
        endpoint_config = json.loads(content)
        # ret = {'token_endpoint': , 'device_authorization_endpoint': None}
        return endpoint_config

    def get_auth_endpoint_config(self, vo):
        auth_config = self.get_cache_value(vo)
        endpoint_config_key = vo + "_endpoint_config"
        endpoint_config = self.get_cache_value(endpoint_config_key)

        if not auth_config or not endpoint_config:
            allow_vos = self.get_allow_vos()
            if vo not in allow_vos:
                return False, "VO %s is not allowed." % vo

            auth_config = self.get_auth_config(vo)
            endpoint_config = self.get_endpoint_config(auth_config)

            self.set_cache_value(vo, auth_config)
            self.set_cache_value(endpoint_config_key, endpoint_config)
        return auth_config, endpoint_config

    def get_oidc_sign_url(self, vo):
        try:
            auth_config, endpoint_config = self.get_auth_endpoint_config(vo)

            data = {'client_id': auth_config['client_id'],
                    'scope': "openid profile email offline_access",
                    'audience': auth_config['audience']}

            headers = {'content-type': 'application/x-www-form-urlencoded'}

            result = requests.session().post(endpoint_config['device_authorization_endpoint'],
                                             # data=json.dumps(data),
                                             urlencode(data).encode(),
                                             timeout=self.timeout,
                                             verify=should_verify(auth_config['no_verify'], self.get_ssl_verify()),
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
            auth_config, endpoint_config = self.get_auth_endpoint_config(vo)

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
                                             verify=should_verify(auth_config['no_verify'], self.get_ssl_verify()),
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
            auth_config, endpoint_config = self.get_auth_endpoint_config(vo)

            data = {'client_id': auth_config['client_id'],
                    'client_secret': auth_config['client_secret'],
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token}

            headers = {'content-type': 'application/x-www-form-urlencoded'}

            result = requests.session().post(endpoint_config['token_endpoint'],
                                             # data=json.dumps(data),
                                             urlencode(data).encode(),
                                             timeout=self.timeout,
                                             verify=should_verify(auth_config['no_verify'], self.get_ssl_verify()),
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

    def get_public_key(self, token, jwks_uri, no_verify=False):
        raise exceptions.NotImplementedException("Method get_public_key is not implemented.")

    def verify_id_token(self, vo, token):
        raise exceptions.NotImplementedException("Method verify_id_token is not implemented.")

    def setup_oidc_client_token(self, issuer, client_id, client_secret, scope, audience):
        try:
            data = {'client_id': client_id,
                    'client_secret': client_secret,
                    'grant_type': 'client_credentials',
                    'scope': scope,
                    'audience': audience}

            headers = {'content-type': 'application/x-www-form-urlencoded'}

            endpoint = '{0}/token'.format(issuer)
            result = requests.session().post(endpoint,
                                             # data=json.dumps(data),
                                             # data=data,
                                             urlencode(data).encode(),
                                             timeout=self.timeout,
                                             verify=should_verify(ssl_verify=self.get_ssl_verify()),
                                             headers=headers)

            if result is not None:
                # print(result)
                # print(result.text)
                # print(result.status_code)

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
            # enc = token['id_token'].split('.')[1]
            enc = token.split('.')[1]
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

    def get_super_user_list(self):
        section = "Users"
        option = "super_users"
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
        username = re.sub('/CN=Robot[^/]+', '', username)
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
        up2 = re.compile('CN=[0-9]+,')
        username = up2.sub(',', username)
        up3 = re.compile(' [0-9]+')
        username = up3.sub('', username)
        up4 = re.compile('_[0-9]+')
        username = up4.sub('', username)
        username = username.replace(',CN=proxy', '')
        username = username.replace(',CN=limited proxy', '')
        username = username.replace('limited proxy', '')
        username = re.sub(',CN=Robot:[^/]+,', ',', username)
        username = re.sub(',CN=Robot:[^/]+', '', username)
        username = re.sub(',CN=Robot[^/]+,', ',', username)
        username = re.sub(',CN=Robot[^/]+', '', username)
        username = re.sub(',CN=nickname:[^/]+,', ',', username)
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


def authenticate_is_super_user(username, dn=None):
    super_user_list = X509Authentication().get_super_user_list()
    for super_user in super_user_list:
        if username == super_user:
            return True
        if dn and super_user in dn:
            return True
    return False
