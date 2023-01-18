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
from idds.common.authentication import authenticate_is_super_user
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.constants import RequestStatus
from idds.common.constants import (MessageType, MessageStatus,
                                   MessageSource, MessageDestination,
                                   CommandType)
from idds.common.utils import json_loads
from idds.core.requests import (add_request, get_requests,
                                get_request, update_request,
                                get_request_ids_by_name)
from idds.core.messages import add_message
from idds.core.commands import add_command
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
            if 'username' not in parameters or not parameters['username']:
                if 'username' in self.get_request().environ and self.get_request().environ['username']:
                    parameters['username'] = self.get_request().environ['username']
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
            username = self.get_username()
            reqs = get_requests(request_id=request_id, with_request=True)
            for req in reqs:
                if req['username'] and req['username'] != username and not authenticate_is_super_user(username):
                    raise exceptions.AuthenticationNoPermission("User %s has no permission to update request %s" % (username, req['request_id']))
        except exceptions.AuthenticationNoPermission as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

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

    def get(self, request_id, workload_id, with_detail, with_metadata=False, with_transform=False, with_processing=False):
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
            if with_detail and (type(with_detail) in [bool] or type(with_detail) in [str] and with_detail.lower() in ['true']):
                with_detail = True
            else:
                with_detail = False
            if with_metadata and (type(with_metadata) in [bool] or type(with_metadata) in [str] and with_metadata.lower() in ['true']):
                with_metadata = True
            else:
                with_metadata = False
            if with_transform and (type(with_transform) in [bool] or type(with_transform) in [str] and with_transform.lower() in ['true']):
                with_transform = True
            else:
                with_transform = False
            if with_processing and (type(with_processing) in [bool] or type(with_processing) in [str] and with_processing.lower() in ['true']):
                with_processing = True
            else:
                with_processing = False

            if with_detail or with_transform or with_processing:
                with_request = False
            else:
                with_request = True

            # reqs = get_requests(request_id=request_id, workload_id=workload_id, to_json=True)
            reqs = get_requests(request_id=request_id, workload_id=workload_id,
                                with_request=with_request, with_detail=with_detail,
                                with_metadata=with_metadata, with_transform=with_transform,
                                with_processing=with_processing)
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


class RequestName(IDDSController):
    """ Get id from name. """

    def get(self, name):
        """ Get id from name.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: {name:id} dict.
        """
        try:
            rets = get_request_ids_by_name(name)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)


class RequestBuild(IDDSController):
    """ Create, Update, get and delete Request. """

    def post(self, request_id):
        """ update build request result.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        try:
            parameters = self.get_request().data and json_loads(self.get_request().data)
            if 'signature' not in parameters or 'workflow' not in parameters:
                raise exceptions.IDDSException("signature and workflow are required")

            # request_id = parameters['request_id']
            signature = parameters['signature']
            workflow = parameters['workflow']

            req = get_request(request_id=request_id)
            if not req:
                raise exceptions.IDDSException("Request %s is not found" % request_id)
            if req['status'] not in [RequestStatus.Building]:
                raise exceptions.IDDSException("Request (request_id: %s, status: %s) is not in Building status" % (request_id, req['status']))

            build_workflow = req['request_metadata']['build_workflow']
            works = build_workflow.get_all_works()
            build_work = works[0]
            if build_work.get_signature() != signature:
                raise exceptions.IDDSException("Request (request_id: %s) has a different signature(%s != %s)" % (request_id,
                                                                                                                 signature,
                                                                                                                 build_work.get_signature()))
            req['request_metadata']['workflow'] = workflow

            parameters = {'status': RequestStatus.Built,
                          'request_metadata': req['request_metadata']}
            update_request(request_id=req['request_id'], parameters=parameters, update_request_metadata=True)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'request_id': request_id})


class RequestAbort(IDDSController):
    """ Abort Request. """

    def put(self, request_id, workload_id=None, task_id=None):
        """ Abort the request.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            404 Not Found
            500 Internal Error
        """
        if request_id == 'null':
            request_id = None
        if workload_id == 'null':
            workload_id = None
        if task_id == 'null':
            task_id = None

        try:
            username = self.get_username()
            if task_id:
                reqs = get_requests(request_id=request_id, workload_id=workload_id, with_processing=True)
            else:
                reqs = get_requests(request_id=request_id, workload_id=workload_id, with_request=True)

            if not reqs:
                return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[(-1, {'status': -1, 'message': 'No match requests'})])
            matched_transform_id = None
            if task_id:
                for req in reqs:
                    if str(req['processing_workload_id']) == str(task_id):
                        matched_transform_id = req['transform_id']
                if matched_transform_id:
                    return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[(-1, {'status': -1, 'message': 'No match tasks'})])

            for req in reqs:
                if req['username'] and req['username'] != username and not authenticate_is_super_user(username):
                    msg = "User %s has no permission to update request %s" % (username, req['request_id'])
                    # raise exceptions.AuthenticationNoPermission(msg)
                    return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[(-1, {'status': -1, 'message': msg})])
        except exceptions.AuthenticationNoPermission as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        try:
            cmd_content = None
            if task_id and matched_transform_id:
                cmd_content = {'task_id': task_id,
                               'transform_id': matched_transform_id}

            add_command(request_id=request_id, cmd_type=CommandType.AbortRequest,
                        workload_id=workload_id, cmd_content=cmd_content,
                        username=username)

        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[(0, {'status': 0, 'message': 'Command registered successfully'})])


class RequestRetry(IDDSController):
    """ Retry Request. """

    def put(self, request_id, workload_id=None):
        """ Retry the request.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            404 Not Found
            500 Internal Error
        """

        if request_id == 'null':
            request_id = None
        if workload_id == 'null':
            workload_id = None

        try:
            username = self.get_username()
            reqs = get_requests(request_id=request_id, workload_id=workload_id, with_request=True)
            if not reqs:
                return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[(-1, {'status': -1, 'message': 'No match requests'})])

            for req in reqs:
                if req['username'] and req['username'] != username and not authenticate_is_super_user(username):
                    msg = "User %s has no permission to update request %s" % (username, req['request_id'])
                    # raise exceptions.AuthenticationNoPermission(msg)
                    return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[(-1, {'status': -1, 'message': msg})])
        except exceptions.AuthenticationNoPermission as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        try:
            add_command(request_id=request_id, cmd_type=CommandType.ResumeRequest,
                        workload_id=workload_id, cmd_content=None,
                        username=username)

        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[(0, {'status': 0, 'message': 'Command registered successfully'})])


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
    bp.add_url_rule('/request/<request_id>/<workload_id>/<with_detail>/<with_metadata>/<with_transform>', view_func=request_view, methods=['get', ])
    bp.add_url_rule('/request/<request_id>/<workload_id>/<with_detail>/<with_metadata>/<with_transform>/<with_processing>', view_func=request_view, methods=['get', ])

    request_name2id = RequestName.as_view('request_name')
    bp.add_url_rule('/request/name/<name>', view_func=request_name2id, methods=['get', ])

    request_build = RequestBuild.as_view('request_build')
    bp.add_url_rule('/request/build/<request_id>', view_func=request_build, methods=['post', ])

    request_abort = RequestAbort.as_view('request_abort')
    bp.add_url_rule('/request/abort/<request_id>/<workload_id>', view_func=request_abort, methods=['put', ])
    bp.add_url_rule('/request/abort/<request_id>/<workload_id>/task_id', view_func=request_abort, methods=['put', ])

    request_retry = RequestRetry.as_view('request_retry')
    bp.add_url_rule('/request/retry/<request_id>/<workload_id>', view_func=request_retry, methods=['put', ])

    return bp
