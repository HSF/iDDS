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
from idds.common.constants import (HTTP_STATUS_CODE, MessageType, MessageStatus,
                                   MessageSource, MessageDestination,
                                   CommandType, RequestStatus)
from idds.common.utils import json_loads
from idds.core.commands import add_command
from idds.core.requests import get_requests
from idds.core.messages import add_message, retrieve_messages
from idds.rest.v1.controller import IDDSController


class Message(IDDSController):
    """ Get message """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = self.setup_logger()

    def get(self, request_id, workload_id, transform_id, internal_id):
        """ Get messages with given id.
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
            if transform_id == 'null':
                transform_id = None
            if internal_id == 'null':
                internal_id = None

            if request_id is None:
                raise Exception("request_id should not be None")
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg=str(error))

        try:
            username = self.get_username()
            reqs = get_requests(request_id=request_id, workload_id=workload_id, with_request=True)
            for req in reqs:
                if req['username'] and req['username'] != username and not authenticate_is_super_user(username):
                    raise exceptions.AuthenticationNoPermission("User %s has no permission to get messages from request %s" % (username, req['request_id']))
        except exceptions.AuthenticationNoPermission as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        try:
            msgs = retrieve_messages(request_id=request_id, workload_id=workload_id, transform_id=transform_id, internal_id=internal_id)
            rets = []
            for msg in msgs:
                msg_content = msg['msg_content']
                if type(msg_content) in (list, tuple):
                    for msg_content_item in msg_content:
                        rets.append(msg_content_item)
                else:
                    rets.append(msg_content)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)

    def post(self, request_id, workload_id, transform_id, internal_id):
        """ Create Request.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        try:
            if request_id == 'null':
                request_id = None
            if workload_id == 'null':
                workload_id = None
            if transform_id == 'null':
                transform_id = None
            if internal_id == 'null':
                internal_id = None
            if request_id is None:
                raise Exception("request_id should not be None")
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg=str(error))

        try:
            username = self.get_username()
            reqs = get_requests(request_id=request_id, workload_id=workload_id, with_request=True)
            for req in reqs:
                if req['username'] and req['username'] != username and not authenticate_is_super_user(username):
                    raise exceptions.AuthenticationNoPermission("User %s has no permission to send messages to request %s" % (username, req['request_id']))
        except exceptions.AuthenticationNoPermission as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        try:
            msg = self.get_request().data and json_loads(self.get_request().data)
            if type(msg) in (list, tuple) and type(msg[0]) in [dict] and 'headers' and msg[0] and 'channel' in msg[0]['headers'] and msg[0]['headers']['channel'] == 'asyncresult':
                for msg_item in msg:
                    add_message(msg_type=MessageType.AsyncResult,
                                status=MessageStatus.New,
                                destination=MessageDestination.AsyncResult,
                                source=MessageSource.Rest,
                                request_id=request_id,
                                workload_id=workload_id,
                                transform_id=transform_id,
                                internal_id=internal_id,
                                num_contents=1,
                                msg_content=msg_item)
            elif 'command' in msg and msg['command'] in ['update_request', 'update_processing']:
                status = msg['parameters']['status']
                if status in [RequestStatus.ToCancel, RequestStatus.ToSuspend]:
                    add_command(request_id=request_id, cmd_type=CommandType.AbortRequest,
                                cmd_content=None)
                elif status in [RequestStatus.ToResume]:
                    add_command(request_id=request_id, cmd_type=CommandType.ResumeRequest,
                                cmd_content=None)
            else:
                add_message(msg_type=MessageType.IDDSCommunication,
                            status=MessageStatus.New,
                            destination=MessageDestination.Clerk,
                            source=MessageSource.Rest,
                            request_id=request_id,
                            workload_id=workload_id,
                            transform_id=None,
                            num_contents=1,
                            msg_content=msg)

        except exceptions.DuplicatedObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.Conflict, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'request_id': request_id})

    def post_test(self):
        import pprint
        pprint.pprint(self.get_request())
        pprint.pprint(self.get_request().endpoint)
        pprint.pprint(self.get_request().url_rule)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('message', __name__)

    view = Message.as_view('message')
    bp.add_url_rule('/message/<request_id>/<workload_id>/<transform_id>/<internal_id>', view_func=view, methods=['get', 'post'])
    return bp
