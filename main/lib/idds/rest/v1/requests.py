#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import datetime
import json
from traceback import format_exc

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.constants import RequestStatus
from idds.common.utils import date_to_str
from idds.api.requests import add_request, get_request, update_request
from idds.rest.v1.controller import IDDSController


class Requests(IDDSController):
    """ Create request """

    def get(self):
        """
        Get requests.

        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: A list containing requests.
        """

        try:
            request_id = self.get_request().args.get('request_id', None)
            workload_id = self.get_request().args.get('workload_id', None)
            if request_id is None and workload_id is None:
                self.generate_http_response(HTTP_STATUS_CODE.BadRequest,
                                            exc_cls=exceptions.BadRequest.__name__,
                                            exc_msg="request_id and workload_id are both None. One should not be None")
            reqs = get_request(request_id=request_id, workload_id=workload_id)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=reqs)


class Request(IDDSController):
    """ Create, Update, get and delete Request. """

    def post(self):
        """ Create Request.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        kwargs = {'scope': None, 'name': None, 'requester': None, 'request_type': None, 'transform_tag': None,
                  'status': RequestStatus.New, 'priority': 0, 'lifetime': 30, 'request_metadata': None}
        try:
            parameters = self.get_request().data and json.loads(self.get_request().data)
            if parameters:
                for key in kwargs:
                    if key in parameters:
                        kwargs[key] = parameters[key]
        except ValueError:
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            request_id = add_request(**kwargs)
        except exceptions.DuplicatedObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.Conflict, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'request_id': request_id})

    def put(self, request_id):
        """ Update Request properties with a given id.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            404 Not Found
            500 Internal Error
        """
        kwargs = {'request_type': None, 'transform_tag': None, 'status': RequestStatus.New, 'priority': 0, 'lifetime': 30, 'request_metadata': None}
        data = {}
        try:
            request = self.get_request()
            parameters = request.data and json.loads(request.data)
            if parameters:
                for key in kwargs:
                    if key in parameters:
                        data[key] = parameters[key]
            # data['status'] = RequestStatus.Extend
        except ValueError:
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            update_request(request_id, data)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'status': 0, 'message': 'update successfully'})

    def get(self, request_id, workload_id):
        """ Get details about a specific Request with given id.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        try:
            if request_id == 'null':
                request_id = None
            if workload_id == 'null':
                workload_id = None

            req = get_request(request_id=request_id, workload_id=workload_id)
            if req['request_type'] is not None:
                req['request_type'] = req['request_type'].value
            if req['status'] is not None:
                req['status'] = req['status'].value
            if req['locking'] is not None:
                req['locking'] = req['locking'].value

            for key in req:
                if req[key] and isinstance(req[key], datetime.datetime):
                    req[key] = date_to_str(req[key])
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=req)

    def post_test(self):
        import pprint
        pprint.pprint(self.get_request())
        pprint.pprint(self.get_request().endpoint)
        pprint.pprint(self.get_request().url_rule)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('request', __name__)

    request_view = Request.as_view('request')
    bp.add_url_rule('/request', view_func=request_view, methods=['post', ])
    bp.add_url_rule('/request/<request_id>', view_func=request_view, methods=['put', ])
    bp.add_url_rule('/request/<request_id>/<workload_id>', view_func=request_view, methods=['get', ])
    return bp
