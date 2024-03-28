#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


from traceback import format_exc

from flask import Blueprint

from idds.common import exceptions
from idds.common.authentication import authenticate_is_super_user
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.constants import RequestStatus, RequestType
from idds.common.utils import json_loads
from idds.core.requests import get_request
from idds.core.transforms import add_transform, get_transform, get_transforms
from idds.rest.v1.controller import IDDSController


class Transform(IDDSController):
    """ Create a Transform. """

    def post(self, request_id):
        """ Create Transform.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        try:
            parameters = self.get_request().data and json_loads(self.get_request().data)
            if 'status' not in parameters:
                parameters['status'] = RequestStatus.New
            if 'priority' not in parameters or not parameters['priority']:
                parameters['priority'] = 0
            if 'token' not in parameters or not parameters['token']:
                return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Token is required')
            token = parameters['token']
            del parameters['token']
        except ValueError:
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            if not request_id or request_id in [None, 'None', 'none', 'NULL', 'null']:
                raise exceptions.IDDSException("Request (request_id: %s) is required" % request_id)

            request_id = int(request_id)
            req = get_request(request_id=request_id)
            if not req:
                raise exceptions.IDDSException("Request %s is not found" % request_id)

            if req['request_type'] not in [RequestType.iWorkflow, RequestType.iWorkflowLocal]:
                raise exceptions.IDDSException("Request type %s doesn't support this operations" % req['request_type'])

            workflow = req['request_metadata']['workflow']
            if workflow.token != token:
                raise exceptions.IDDSException("Token %s is not correct for request %s" % (token, request_id))

            username = self.get_username()
            if req['username'] and req['username'] != username and not authenticate_is_super_user(username):
                raise exceptions.AuthenticationNoPermission("User %s has no permission to update request %s" % (username, request_id))

            parameters['request_id'] = request_id
            transform_id = add_transform(**parameters)
        except exceptions.DuplicatedObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.Conflict, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'transform_id': transform_id})

    def get(self, request_id, transform_id=None):
        """ Get transforms with given id.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        try:
            if not request_id or request_id in [None, 'None', 'none', 'NULL', 'null']:
                raise exceptions.IDDSException("Request (request_id: %s) is required" % request_id)
            if not transform_id or transform_id in [None, 'None', 'none', 'NULL', 'null']:
                transform_id = None

            if not transform_id:
                tfs = get_transforms(request_id=request_id)
            else:
                tfs = get_transform(request_id=request_id, transform_id=transform_id)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=tfs)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('transfrom', __name__)

    transform_view = Transform.as_view('transform')
    bp.add_url_rule('/transform/<request_id>', view_func=transform_view, methods=['post', ])
    bp.add_url_rule('/transform/<request_id>', view_func=transform_view, methods=['get', ])
    bp.add_url_rule('/transform/<request_id>/<transform_id>', view_func=transform_view, methods=['get', ])

    return bp
