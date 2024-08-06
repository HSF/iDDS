#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


import traceback

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE, CollectionRelationType
from idds.core.catalog import get_collections, get_contents, get_contents_ext, combine_contents_ext
from idds.rest.v1.controller import IDDSController


class Collections(IDDSController):
    """ Catalog """

    def get(self, request_id, transform_id, workload_id, scope, name, relation_type):
        """ Get collections by request_id, transform_id, workload_id, scope and name
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        try:
            if request_id in ['null', 'None']:
                request_id = None
            else:
                request_id = int(request_id)
            if transform_id in ['null', 'None']:
                transform_id = None
            else:
                transform_id = int(transform_id)
            if workload_id in ['null', 'None']:
                workload_id = None
            else:
                workload_id = int(workload_id)
            if scope in ['null', 'None']:
                scope = None
            if name in ['null', 'None']:
                name = None
            if relation_type in ['null', 'None']:
                relation_type = None
            else:
                relation_type = int(relation_type)

            rets = get_collections(request_id=request_id, transform_id=transform_id, workload_id=workload_id,
                                   scope=scope, name=name, relation_type=relation_type, to_json=False)
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

    def get(self, request_id, transform_id, workload_id, coll_scope, coll_name, relation_type, status):
        """ Get contents by request_id, transform_id, workload_id coll_scope, coll_name, relation_type and status.
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
            if transform_id in ['null', 'None']:
                transform_id = None
            else:
                transform_id = int(transform_id)
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

            rets = get_contents(request_id=request_id, transform_id=transform_id, workload_id=workload_id, coll_scope=coll_scope,
                                coll_name=coll_name, relation_type=relation_type, status=status, to_json=False)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)


class ContentsOutputExt(IDDSController):
    """ Catalog """

    def get(self, request_id, workload_id, transform_id, group_by_jedi_task_id=False):
        """ Get contents by request_id, workload_id and transform_id.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: contents.
        """

        try:
            if request_id in ['null', 'None']:
                request_id = None
            else:
                request_id = int(request_id)
            if workload_id in ['null', 'None']:
                workload_id = None
            else:
                workload_id = int(workload_id)
            if transform_id in ['null', 'None']:
                transform_id = None
            else:
                transform_id = int(transform_id)
            if group_by_jedi_task_id:
                if type(group_by_jedi_task_id) in [bool]:
                    pass
                else:
                    if type(group_by_jedi_task_id) in [str] and group_by_jedi_task_id.lower() in ['true']:
                        group_by_jedi_task_id = True
                    else:
                        group_by_jedi_task_id = False
            else:
                group_by_jedi_task_id = False

            if request_id is None:
                return self.generate_http_response(HTTP_STATUS_CODE.BadRequest,
                                                   exc_cls=exceptions.BadRequest.__name__,
                                                   exc_msg="request_id must not be None")

            else:
                contents = get_contents(request_id=request_id, workload_id=workload_id, transform_id=transform_id,
                                        relation_type=CollectionRelationType.Output)
                contents_ext = get_contents_ext(request_id=request_id, workload_id=workload_id, transform_id=transform_id)

                ret_contents = combine_contents_ext(contents, contents_ext, with_status_name=True)
                rets = {}
                for content in ret_contents:
                    if group_by_jedi_task_id:
                        jedi_task_id = content.get('jedi_task_id', 'None')
                        if jedi_task_id not in rets:
                            rets[jedi_task_id] = []
                        rets[jedi_task_id].append(content)
                    else:
                        transform_id = content.get('transform_id')
                        if transform_id not in rets:
                            rets[transform_id] = []
                        rets[transform_id].append(content)
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
    bp.add_url_rule('/catalog/collections/<request_id>/<transform_id>/<workload_id>/<scope>/<name>/<relation_type>',
                    view_func=collections_view, methods=['get', ])  # get collections

    contents_view = Contents.as_view('contents')
    bp.add_url_rule('/catalog/contents/<request_id>/<transform_id>/<workload_id>/<coll_scope>/<coll_name>/<relation_type>/<status>',
                    view_func=contents_view, methods=['get', ])  # get contents

    contents_ext_view = ContentsOutputExt.as_view('contents_output_ext')
    bp.add_url_rule('/catalog/contents_output_ext/<request_id>/<workload_id>/<transform_id>/<group_by_jedi_task_id>',
                    view_func=contents_ext_view, methods=['get', ])

    return bp
