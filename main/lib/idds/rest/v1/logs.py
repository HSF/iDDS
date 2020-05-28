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
from idds.common.utils import tar_zip_files
from idds.core import (requests as core_requests,
                       transforms as core_transforms,
                       processings as core_processings)
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
            if not request_id:
                request_ids = core_requests.get_request_ids_by_workload_id(workload_id)
                if not request_ids:
                    error = "Cannot find requests with this workloa_id: %s" % workload_id
                    return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)
                else:
                    if len(request_ids) > 1:
                        error = "More than one request with the same workload_id. request_id should be provided."
                        return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)
                    else:
                        request_id = request_ids[0]
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        try:
            transform_ids = core_transforms.get_transform_ids(request_id)
            files = []
            for transform_id in transform_ids:
                processings = core_processings.get_processings_by_transform_id(transform_id)
                for processing in processings:
                    processing_metadata = processing['processing_metadata']
                    if processing_metadata and 'job_logs_tar' in processing_metadata and processing_metadata['job_logs_tar']:
                        files.append(processing_metadata['job_logs_tar'])
            if not files:
                error = "No log files founded."
                return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

            cache_dir = os.path.dirname(files[0])
            output_filename = "%s.logs.tgz" % request_id
            tar_zip_files(cache_dir, output_filename, files)
            return send_from_directory(cache_dir, output_filename, as_attachment=True)
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
