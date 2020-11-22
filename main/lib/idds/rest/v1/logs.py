#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import os
from traceback import format_exc

from flask import Blueprint, send_from_directory

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import tar_zip_files, get_rest_cacher_dir
from idds.core import (transforms as core_transforms)
from idds.rest.v1.controller import IDDSController


class Logs(IDDSController):
    """  get(download) logs. """

    def get(self, workload_id, request_id):
        """ Get(download) logs.

        :param workload_id: the workload id.
        :param request_id: The id of the request.

        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: logs.
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
            transforms = core_transforms.get_transforms(request_id=request_id, workload_id=workload_id)
            workdirs = []
            for transform in transforms:
                work = transform['transform_metadata']['work']
                workdir = work.get_workdir()
                if workdir:
                    workdirs.append(workdir)
            if not workdirs:
                error = "No log files founded."
                return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

            cache_dir = get_rest_cacher_dir()
            if request_id and workload_id:
                output_filename = "request_%s.workload_%s.logs.tar.gz" % (request_id, workload_id)
            elif request_id:
                output_filename = "request_%s.logs.tar.gz" % (request_id)
            elif workload_id:
                output_filename = "workload_%s.logs.tar.gz" % (workload_id)
            else:
                output_filename = "%s.logs.tar.gz" % (os.path.basename(cache_dir))
            tar_zip_files(cache_dir, output_filename, workdirs)
            return send_from_directory(cache_dir, output_filename, as_attachment=True, mimetype='application/x-tgz')
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

    def post_test(self):
        import pprint
        pprint.pprint(self.get_request())
        pprint.pprint(self.get_request().endpoint)
        pprint.pprint(self.get_request().url_rule)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('logs', __name__)

    logs_view = Logs.as_view('logs')
    bp.add_url_rule('/logs/<workload_id>/<request_id>', view_func=logs_view, methods=['get', ])

    return bp
