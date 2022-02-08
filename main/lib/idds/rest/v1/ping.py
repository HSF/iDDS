#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021 - 2022


from flask import Blueprint

# from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.rest.v1.controller import IDDSController


class Ping(IDDSController):
    """ Ping the rest service """

    def get(self):
        """ Ping the rest service.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        rets = {'Status': 'OK'}
        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)

    def post_test(self):
        import pprint
        pprint.pprint(self.get_request())
        pprint.pprint(self.get_request().endpoint)
        pprint.pprint(self.get_request().url_rule)


"""----------------------
   Web service url maps
----------------------"""


def get_blueprint():
    bp = Blueprint('ping', __name__)

    view = Ping.as_view('ping')
    bp.add_url_rule('/ping', view_func=view, methods=['get'])
    return bp
