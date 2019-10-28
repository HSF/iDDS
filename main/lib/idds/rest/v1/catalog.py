#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import copy
import json

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import convert_nojsontype_to_value
from idds.api.catalog import get_collections, get_contents, register_output_contents, get_match_contents
from idds.rest.v1.controller import IDDSController


class Collections(IDDSController):
    """ Catalog """

    def get(self, scope, name, request_id, workload_id):
        """ Get collections by scope, name, request_id and workload_id.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        try:
            if scope == 'null':
                scope = None
            if name == 'null':
                name = None
            if request_id == 'null':
                request_id = None
            if workload_id == 'null':
                workload_id = None

            rets = get_collections(scope=scope, name=name, request_id=request_id, workload_id=workload_id)
            rets = convert_nojsontype_to_value(rets)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)


class Contents(IDDSController):
    """ Catalog """

    def get(self, coll_scope, coll_name, request_id, workload_id, relation_type):
        """ Get contents by coll_scope, coll_name, request_id, workload_id and relation_type.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: contents.
        """

        try:
            if coll_scope == 'null':
                coll_scope = None
            if coll_name == 'null':
                coll_name = None
            if request_id == 'null':
                request_id = None
            if workload_id == 'null':
                workload_id = None
            if relation_type == 'null':
                relation_type = None

            rets = get_contents(coll_scope=coll_scope, coll_name=coll_name, request_id=request_id,
                                workload_id=workload_id, relation_type=relation_type)
            rets = convert_nojsontype_to_value(rets)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)


class Catalog(IDDSController):
    """ Catalog """

    def get(self, coll_scope, coll_name, scope, name, min_id, max_id, request_id, workload_id, only_return_best_match):
        """ Get output contents by request id, workload id, coll_scope, coll_name, scope, name, min_id, max_id.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        try:
            if coll_scope == 'null':
                coll_scope = None
            if coll_name == 'null':
                coll_name = None
            if scope == 'null':
                scope = None
            if name == 'null':
                name = None
            if min_id == 'null':
                min_id = None
            if max_id == 'null':
                max_id = None
            if request_id == 'null':
                request_id = None
            if workload_id == 'null':
                workload_id = None
            if only_return_best_match == 'null':
                only_return_best_match = None
            rets = get_match_contents(coll_scope=coll_scope, coll_name=coll_name, scope=scope, name=name,
                                      min_id=min_id, max_id=max_id, request_id=request_id,
                                      workload_id=workload_id, only_return_best_match=only_return_best_match)
            rets = convert_nojsontype_to_value(rets)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)

    def post(self, coll_scope, coll_name, request_id, workload_id):
        """ register output contents.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        kwargs = {'scope': None, 'name': None, 'min_id': None, 'max_id': None,
                  'path': None, 'status': None}
        try:
            if coll_scope == 'null':
                coll_scope = None
            if coll_name == 'null':
                coll_name = None
            if request_id == 'null':
                request_id = None
            if workload_id == 'null':
                workload_id = None

            contents = []
            parameters = self.get_request().data and json.loads(self.get_request().data)
            if parameters:
                for parameter in parameters:
                    content = copy.deepcopy(kwargs)
                    for key in kwargs:
                        if key in parameter:
                            content[key] = parameter[key]
                    contents.append(content)
            register_output_contents(coll_scope=coll_scope, coll_name=coll_name, contents=contents,
                                     request_id=request_id, workload_id=workload_id)
        except ValueError:
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')
        except exceptions.DuplicatedObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.Conflict, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=None)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('catalog', __name__)

    catalog_view = Catalog.as_view('catalog')

    collections_view = Collections.as_view('collections')
    bp.add_url_rule('/catalog/collections/<scope>/<name>/<request_id>/<workload_id>',
                    view_func=collections_view, methods=['get', ])  # get collections

    contents_view = Contents.as_view('contents')
    bp.add_url_rule('/catalog/contents/<coll_scope>/<coll_name>/<request_id>/<workload_id>/<relation_type>',
                    view_func=contents_view, methods=['get', ])  # get contents

    catalog_view = Catalog.as_view('catalog')
    bp.add_url_rule('/catalog/<coll_scope>/<coll_name>/<scope>/<name>/<min_id>/<max_id>/<request_id>/<workload_id>/<only_return_best_match>',
                    view_func=catalog_view, methods=['get', ])  # get match contents
    bp.add_url_rule('/catalog/<coll_scope>/<coll_name>/<request_id>/<workload_id>',
                    view_func=catalog_view, methods=['post', ])  # register contents

    return bp
