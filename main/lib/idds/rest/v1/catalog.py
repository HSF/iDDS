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
import traceback

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE, CollectionRelationType
from idds.core.catalog import get_collections, get_contents, register_output_contents, get_match_contents
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
            if scope in ['null', 'None']:
                scope = None
            if name in ['null', 'None']:
                name = None
            if request_id in ['null', 'None']:
                request_id = None
            else:
                request_id = int(request_id)
            if workload_id in ['null', 'None']:
                workload_id = None
            else:
                workload_id = int(workload_id)

            rets = get_collections(scope=scope, name=name, request_id=request_id, workload_id=workload_id)
            for req_id in rets:
                for trans_id in rets[req_id]:
                    colls = rets[req_id][trans_id]
                    colls = [coll.to_dict_json() for coll in colls]
                    rets[req_id][trans_id] = colls
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
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
            if coll_scope in ['null', 'None']:
                coll_scope = None
            if coll_name in ['null', 'None']:
                coll_name = None
            if request_id in ['null', 'None']:
                request_id = None
            else:
                request_id = int(request_id)
            if workload_id in ['null', 'None']:
                workload_id = None
            else:
                workload_id = int(workload_id)
            if relation_type in ['null', 'None']:
                relation_type = None
            else:
                relation_type = int(relation_type)

            rets = get_contents(coll_scope=coll_scope, coll_name=coll_name, request_id=request_id,
                                workload_id=workload_id, relation_type=relation_type)
            for req_id in rets:
                for trans_id in rets[req_id]:
                    for scope_name in rets[req_id][trans_id]:
                        if 'collection' in rets[req_id][trans_id][scope_name]:
                            rets[req_id][trans_id][scope_name]['collection'] = rets[req_id][trans_id][scope_name]['collection'].to_dict_json()
                        if 'relation_type' in rets[req_id][trans_id][scope_name]:
                            relation_type = rets[req_id][trans_id][scope_name]['relation_type']
                            if isinstance(relation_type, CollectionRelationType):
                                relation_type = relation_type.value
                            rets[req_id][trans_id][scope_name]['relation_type'] = relation_type
                        if 'contents' in rets[req_id][trans_id][scope_name]:
                            contents = rets[req_id][trans_id][scope_name]['contents']
                            contents = [content.to_dict_json() for content in contents]
                            rets[req_id][trans_id][scope_name]['contents'] = contents
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
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
            if coll_scope in ['null', 'None']:
                coll_scope = None
            if coll_name in ['null', 'None']:
                coll_name = None
            if scope in ['null', 'None']:
                scope = None
            if name in ['null', 'None']:
                name = None
            if min_id in ['null', 'None']:
                min_id = None
            else:
                min_id = int(min_id)
            if max_id in ['null', 'None']:
                max_id = None
            else:
                max_id = int(max_id)
            if request_id in ['null', 'None']:
                request_id = None
            else:
                request_id = int(request_id)
            if workload_id in ['null', 'None']:
                workload_id = None
            else:
                workload_id = int(workload_id)
            if only_return_best_match in ['null', 'None']:
                only_return_best_match = None
            else:
                if only_return_best_match.lower() == 'true':
                    only_return_best_match = True
                else:
                    only_return_best_match = False

            rets = get_match_contents(coll_scope=coll_scope, coll_name=coll_name, scope=scope, name=name,
                                      min_id=min_id, max_id=max_id, request_id=request_id,
                                      workload_id=workload_id, only_return_best_match=only_return_best_match)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=[ret.to_dict_json() for ret in rets])

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
            if coll_scope in ['null', 'None']:
                coll_scope = None
            if coll_name in ['null', 'None']:
                coll_name = None
            if request_id in ['null', 'None']:
                request_id = None
            else:
                request_id = int(request_id)
            if workload_id in ['null', 'None']:
                workload_id = None
            else:
                workload_id = int(workload_id)

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
            print(error)
            print(traceback.format_exc())
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
