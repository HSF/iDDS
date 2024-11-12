#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


from flask import Response, request
from flask.views import MethodView

from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import json_dumps, get_logger


class IDDSController(MethodView):
    """ Default ESS Controller class. """

    def get_class_name(self):
        return self.__class__.__name__

    def setup_logger(self, logger=None):
        """
        Setup logger
        """
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(name=self.get_class_name(), filename='idds_rest.log')
        return self.logger

    def post(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def get(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def put(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def delete(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def get_request(self):
        return request

    def get_username(self):
        if 'username' in request.environ and request.environ['username']:
            return request.environ['username']
        return None

    def generate_message(self, exc_cls=None, exc_msg=None):
        if exc_cls is None and exc_msg is None:
            return None
        else:
            message = {}
            # if exc_cls is not None:
            #     message['ExceptionClass'] = exc_cls
            if exc_msg is not None:
                message['msg'] = str(exc_msg)
            return json_dumps(message)

    def generate_http_response(self, status_code, data=None, exc_cls=None, exc_msg=None):
        enable_json_outputs = self.get_request().args.get('json_outputs', None)
        if enable_json_outputs and enable_json_outputs.upper() == 'TRUE':
            error = None
            if exc_cls:
                error = {'ExceptionClass': exc_cls,
                         'ExceptionMessage': self.generate_message(exc_cls, exc_msg)}
            if status_code == HTTP_STATUS_CODE.OK:
                status_code = 0
            response = {'status': status_code,
                        'data': data,
                        'error': error}
            resp = Response(response=json_dumps(response, sort_keys=True, indent=4), status=HTTP_STATUS_CODE.OK, content_type='application/json')
        else:
            resp = Response(response=json_dumps(data, sort_keys=True, indent=4) if data is not None else data, status=status_code, content_type='application/json')
            if exc_cls:
                resp.headers['ExceptionClass'] = exc_cls
                resp.headers['ExceptionMessage'] = self.generate_message(exc_cls, exc_msg)
        return resp
