#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

"""----------------------
   Web service app
----------------------"""

import flask
from flask import Flask, Response

from idds.common import exceptions
from idds.common.authentication import authenticate_x509, authenticate_oidc
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import get_rest_debug
# from idds.common.utils import get_rest_url_prefix
from idds.rest.v1 import requests
from idds.rest.v1 import catalog
from idds.rest.v1 import cacher
from idds.rest.v1 import hyperparameteropt
from idds.rest.v1 import logs
from idds.rest.v1 import monitor
from idds.rest.v1 import messages
from idds.rest.v1 import ping
from idds.rest.v1 import auth


class LoggingMiddleware(object):
    def __init__(self, app, logger, url_map):
        import logging
        self._app = app
        self._logger = logger
        self._url_map = url_map
        self._logger.setLevel(logging.DEBUG)

    def __call__(self, environ, resp):
        import pprint
        # errorlog = environ['wsgi.errors']
        # pprint.pprint(('REQUEST', environ), stream=errorlog)
        self._logger.info(pprint.pprint(('URLMAP', self._url_map)))
        self._logger.info(pprint.pprint(('REQUEST', environ)))

        def log_response(status, headers, *args):
            # pprint.pprint(('RESPONSE', status, headers), stream=errorlog)
            self._logger.info(('RESPONSE', status, headers))
            return resp(status, headers, *args)

        return self._app(environ, log_response)


def get_normal_blueprints():
    bps = []
    bps.append(requests.get_blueprint())
    bps.append(catalog.get_blueprint())
    bps.append(cacher.get_blueprint())
    bps.append(hyperparameteropt.get_blueprint())
    bps.append(logs.get_blueprint())
    # bps.append(monitor.get_blueprint())
    bps.append(messages.get_blueprint())
    bps.append(ping.get_blueprint())

    return bps


def get_auth_blueprints():
    bps = []
    bps.append(auth.get_blueprint())
    bps.append(monitor.get_blueprint())
    return bps


def generate_failed_auth_response(exc_msg=None):
    resp = Response(response=None, status=HTTP_STATUS_CODE.Unauthorized, content_type='application/json')
    resp.headers['ExceptionClass'] = exceptions.IDDSException.__name__
    resp.headers['ExceptionMessage'] = exc_msg
    return resp


def before_request_auth():
    print("envs")
    print(flask.request.environ)
    print("headers")
    print(flask.request.headers)
    auth_type = flask.request.headers.get('X-IDDS-Auth-Type', default='x509_proxy')
    vo = flask.request.headers.get('X-IDDS-Auth-VO', default=None)
    if auth_type in ['x509_proxy']:
        dn = flask.request.environ.get('SSL_CLIENT_S_DN', None)
        client_cert = flask.request.environ.get('SSL_CLIENT_CERT', None)
        is_authenticated, errors, username = authenticate_x509(vo, dn, client_cert)
        if not is_authenticated:
            return generate_failed_auth_response(errors)
        flask.request.environ['username'] = username
    elif auth_type in ['oidc']:
        token = flask.request.headers.get('X-IDDS-Auth-Token', default=None)
        is_authenticated, errors, username = authenticate_oidc(vo, token)
        if not is_authenticated:
            return generate_failed_auth_response(errors)
        flask.request.environ['username'] = username
    else:
        errors = "Authentication method %s is not supported" % auth_type
        return generate_failed_auth_response(errors)


def after_request(response):
    return response


def create_app(auth_type=None):
    # url_prefix = get_rest_url_prefix()
    application = Flask(__name__)

    bps = get_auth_blueprints()
    for bp in bps:
        # application.register_blueprint(bp, url_prefix=url_prefix)
        application.register_blueprint(bp)

    bps = get_normal_blueprints()
    for bp in bps:
        bp.before_request(before_request_auth)
        bp.after_request(after_request)
        # application.register_blueprint(bp, url_prefix=url_prefix)
        application.register_blueprint(bp)

    # application.before_request(before_request)
    # application.after_request(after_request)
    if get_rest_debug():
        application.wsgi_app = LoggingMiddleware(application.wsgi_app, application.logger, application.url_map)

    @application.errorhandler(404)
    @application.errorhandler(405)
    def _handle_api_error(ex):
        status = HTTP_STATUS_CODE.NotFound
        if hasattr(ex, 'code'):
            status = ex.code
        resp = Response(response=None, status=status)
        resp.headers['ExceptionClass'] = exceptions.IDDSException.__name__
        resp.headers['ExceptionMessage'] = 'The requested REST API is not defined: %s' % ex
        return resp

    return application
