#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


from flask import Response, request
from flask.views import MethodView

from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import json_dumps


class IDDSController(MethodView):
    """ Default ESS Controller class. """

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

    def get_request(sel):
        return request

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
        resp = Response(response=json_dumps(data) if data is not None else data, status=status_code, content_type='application/json')
        if exc_cls:
            resp.headers['ExceptionClass'] = exc_cls
            resp.headers['ExceptionMessage'] = self.generate_message(exc_cls, exc_msg)
        return resp
