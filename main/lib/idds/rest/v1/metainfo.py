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
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import get_asyncresult_config

from idds.rest.v1.controller import IDDSController


class MetaInfo(IDDSController):
    """ Get Meta info"""

    def get(self, name):
        try:
            rets = {}
            if name == 'asyncresult_config':
                asyncresult_config = get_asyncresult_config()
                rets = asyncresult_config

        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)


def get_blueprint():
    bp = Blueprint('metainfo', __name__)

    metainfo_view = MetaInfo.as_view('metainfo')
    bp.add_url_rule('/metainfo/<name>', view_func=metainfo_view, methods=['get', ])

    return bp
