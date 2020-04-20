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
from idds.common.utils import get_rest_cacher_dir
from idds.rest.v1.controller import IDDSController


class Cacher(IDDSController):
    """ upload/download file """

    def post(self, filename):
        """ upload file.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        if '/' in filename:
            return self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='subdirectory is not allowed')

        try:
            cacher_dir = get_rest_cacher_dir()
            """
            data = self.get_request().data and json.loads(self.get_request().data)
            content = data['content']
            # bytes = data['bytes']

            with open(os.path.join(cacher_dir, filename), 'w') as f:
                f.write(content)
            """
            data = self.get_request().data
            with open(os.path.join(cacher_dir, filename), 'wb') as f:
                f.write(data)
        except exceptions.DuplicatedObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.Conflict, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data={'filename': filename})

    def get(self, filename):
        """ donwload file.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        try:
            cacher_dir = get_rest_cacher_dir()
            # with open(os.path.join(cacher_dir, filename), 'r') as f:
            #     content = f.read()
            # data = {'content': content, 'bytes': os.path.getsize(os.path.join(cacher_dir, filename))}
            return send_from_directory(cacher_dir, filename, as_attachment=True)
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        # return self.generate_http_response(HTTP_STATUS_CODE.OK, data=data)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('cacher', __name__)

    cacher_view = Cacher.as_view('cacher')
    bp.add_url_rule('/cacher/<filename>', view_func=cacher_view, methods=['post', ])
    bp.add_url_rule('/cacher/<filename>', view_func=cacher_view, methods=['get', ])
    return bp
