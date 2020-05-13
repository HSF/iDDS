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
from idds.common.constants import ContentType
from idds.core import catalog
from idds.rest.v1.controller import IDDSController


class HyperParameterOpt(IDDSController):
    """  get and update hyper parameters. """

    def put(self, request_id, id, loss):
        """ Update the loss for the hyper parameter.

        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            404 Not Found
            500 Internal Error
        """

        try:
            loss = float(loss)
            content = catalog.get_output_content_by_request_id_content_name(request_id, content_scope='hpo', content_name=str(id), content_type=ContentType.PseudoContent, min_id=0, max_id=0)
            content_id = content['content_id']
            point = content['path']
            param, origin_loss = json.loads(point)
            params = {'path': json.dumps((param, loss))}
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

    def get(self, request_id, status=None, limit=None):
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
            if status == 'null':
                status = None
            if limit == 'null':
                limit = None

            contents = catalog.get_output_contents_by_request_id_status(request_id, status, limit)
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
    bp.add_url_rule('/hpo/<request_id>/<id>/<loss>', view_func=hpo_view, methods=['put', ])
    bp.add_url_rule('/hpo/<request_id>/<status>/<limit>', view_func=hpo_view, methods=['get', ])
    return bp
