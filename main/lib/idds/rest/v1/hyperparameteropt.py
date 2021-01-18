#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

import json
from traceback import format_exc

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.constants import CollectionRelationType, ContentStatus
from idds.core import catalog
from idds.rest.v1.controller import IDDSController


class HyperParameterOpt(IDDSController):
    """  get and update hyper parameters. """

    def put(self, workload_id, request_id, id, loss):
        """ Update the loss for the hyper parameter.

        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            404 Not Found
            500 Internal Error
        """
        try:
            if workload_id == 'null':
                workload_id = None
            if request_id == 'null':
                request_id = None

            if workload_id is None and request_id is None:
                error = "One of workload_id and request_id should not be None or empty"
                return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        try:
            contents = catalog.get_contents(request_id=request_id, workload_id=workload_id,
                                            relation_type=CollectionRelationType.Output)

            if id:
                new_contents = []
                for content in contents:
                    if str(content['name']) == str(id):
                        new_contents.append(content)
                contents = new_contents
            content = contents[0]

            loss = float(loss)
            content_id = content['content_id']
            point = content['path']
            param, origin_loss = json.loads(point)
            params = {'path': json.dumps((param, loss)), 'substatus': ContentStatus.Available}
            catalog.update_content(content_id, params)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'status': 0, 'message': 'update successfully'})

    def get(self, workload_id, request_id, id=None, status=None, limit=None):
        """ Get hyper parameters.
        :param request_id: The id of the request.
        :param status: status of the hyper parameters. None for all statuses.
        :param limit: Limit number of hyperparameters.

        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: list of hyper parameters.
        """

        try:
            if workload_id == 'null':
                workload_id = None
            if request_id == 'null':
                request_id = None

            if status == 'null':
                status = None
            if limit == 'null':
                limit = None
            if id == 'null':
                id = None

            contents = catalog.get_contents(request_id=request_id, workload_id=workload_id,
                                            status=status, relation_type=CollectionRelationType.Output)

            if id:
                new_contents = []
                for content in contents:
                    if str(content['name']) == str(id):
                        new_contents.append(content)
                contents = new_contents

            if contents and limit and len(contents) > limit:
                contents = contents[:limit]

            hyperparameters = []
            for content in contents:
                point = content['path']
                parameter, loss = json.loads(point)
                param = {'id': content['name'],
                         'parameters': parameter,
                         'loss': loss}
                hyperparameters.append(param)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=hyperparameters)

    def post_test(self):
        import pprint
        pprint.pprint(self.get_request())
        pprint.pprint(self.get_request().endpoint)
        pprint.pprint(self.get_request().url_rule)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('hpo', __name__)

    hpo_view = HyperParameterOpt.as_view('hpo')
    bp.add_url_rule('/hpo/<workload_id>/<request_id>/<id>/<loss>', view_func=hpo_view, methods=['put', ])
    bp.add_url_rule('/hpo/<workload_id>/<request_id>/<id>/<status>/<limit>', view_func=hpo_view, methods=['get', ])
    return bp
