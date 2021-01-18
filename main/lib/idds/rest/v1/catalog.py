#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019-2020


import traceback

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.core.catalog import get_collections, get_contents
from idds.rest.v1.controller import IDDSController


class Collections(IDDSController):
    """ Catalog """

    def get(self, scope, name, request_id, workload_id, relation_type):
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
            if relation_type in ['null', 'None']:
                relation_type = None
            else:
                relation_type = int(relation_type)

            rets = get_collections(scope=scope, name=name, request_id=request_id, workload_id=workload_id,
                                   relation_type=relation_type, to_json=False)
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

    def get(self, coll_scope, coll_name, request_id, workload_id, relation_type, status):
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
            if status in ['null', 'None']:
                status = None
            else:
                status = int(status)

            rets = get_contents(coll_scope=coll_scope, coll_name=coll_name, request_id=request_id,
                                workload_id=workload_id, relation_type=relation_type, status=status, to_json=False)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('catalog', __name__)

    # catalog_view = Catalog.as_view('catalog')

    collections_view = Collections.as_view('collections')
    bp.add_url_rule('/catalog/collections/<scope>/<name>/<request_id>/<workload_id>/<relation_type>',
                    view_func=collections_view, methods=['get', ])  # get collections

    contents_view = Contents.as_view('contents')
    bp.add_url_rule('/catalog/contents/<coll_scope>/<coll_name>/<request_id>/<workload_id>/<relation_type>/<status>',
                    view_func=contents_view, methods=['get', ])  # get contents

    return bp
