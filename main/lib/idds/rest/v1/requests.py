#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


from traceback import format_exc

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.constants import RequestStatus
from idds.common.constants import (MessageType, MessageStatus,
                                   MessageSource, MessageDestination)
from idds.common.utils import json_loads
from idds.core.requests import add_request, get_requests
from idds.core.messages import add_message
from idds.rest.v1.controller import IDDSController

from idds.rest.v1.utils import convert_old_req_2_workflow_req


class Requests(IDDSController):
    """ Get request """

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
            # reqs = get_requests(request_id=request_id, workload_id=workload_id, to_json=True)
            reqs = get_requests(request_id=request_id, workload_id=workload_id)
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
        try:
            parameters = self.get_request().data and json_loads(self.get_request().data)
            if 'status' not in parameters:
                parameters['status'] = RequestStatus.New
            if 'priority' not in parameters:
                parameters['priority'] = 0
            # if 'lifetime' not in parameters:
            #     parameters['lifetime'] = 30
        except ValueError:
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            parameters = convert_old_req_2_workflow_req(parameters)
            request_id = add_request(**parameters)
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
        try:
            request = self.get_request()
            parameters = request.data and json_loads(request.data)
            # parameters['status'] = RequestStatus.Extend
        except ValueError:
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            # update_request(request_id, parameters)
            # msg = {'command': 'update_request', 'parameters': {'status': RequestStatus.ToSuspend}})
            msg = {'command': 'update_request', 'parameters': parameters}
            add_message(msg_type=MessageType.IDDSCommunication,
                        status=MessageStatus.New,
                        destination=MessageDestination.Clerk,
                        source=MessageSource.Rest,
                        request_id=request_id,
                        workload_id=None,
                        transform_id=None,
                        num_contents=1,
                        msg_content=msg)

        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'status': 0, 'message': 'update successfully'})

    def get(self, request_id, workload_id, with_detail, with_metadata=False):
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
            if with_detail and with_detail.lower() in ['true']:
                with_detail = True
            else:
                with_detail = False
            if with_metadata and with_metadata.lower() in ['true']:
                with_metadata = True
            else:
                with_metadata = False

            # reqs = get_requests(request_id=request_id, workload_id=workload_id, to_json=True)
            reqs = get_requests(request_id=request_id, workload_id=workload_id, with_detail=with_detail, with_metadata=with_metadata)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=reqs)

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
    bp.add_url_rule('/request/<request_id>/<workload_id>/<with_detail>', view_func=request_view, methods=['get', ])
    bp.add_url_rule('/request/<request_id>/<workload_id>/<with_detail>/<with_metadata>', view_func=request_view, methods=['get', ])
    return bp
