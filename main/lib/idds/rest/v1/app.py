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

from flask import Flask, Response

from idds.common import exceptions
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


def get_blueprints():
    bps = []
    bps.append(requests.get_blueprint())
    bps.append(catalog.get_blueprint())
    bps.append(cacher.get_blueprint())
    bps.append(hyperparameteropt.get_blueprint())
    bps.append(logs.get_blueprint())
    bps.append(monitor.get_blueprint())
    bps.append(messages.get_blueprint())
    return bps


def create_app():
    # url_prefix = get_rest_url_prefix()
    bps = get_blueprints()
    application = Flask(__name__)
    for bp in bps:
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
