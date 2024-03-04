#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021

import json
import traceback

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.core.authentication import OIDCAuthentication
from idds.rest.v1.controller import IDDSController


class OIDCAuthenticationSignURL(IDDSController):
    """ OIDCAuthentication Sign URL"""

    def get(self, vo, auth_type='oidc'):
        """ Get sign url for user to approve.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary with sign url.
        """

        try:
            if auth_type == 'oidc':
                oidc = OIDCAuthentication()
                status, sign_url = oidc.get_oidc_sign_url(vo)
                if status:
                    rets = sign_url
                    return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)
                else:
                    raise exceptions.IDDSException("Failed to get oidc sign url: %s" % str(sign_url))
            else:
                raise exceptions.AuthenticationNotSupported("auth_type %s is not supported to call this function." % str(auth_type))
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)


class OIDCAuthenticationToken(IDDSController):
    """ OIDCAuthentication Token"""

    def get(self, vo, device_code, interval=5, expires_in=60):
        """ Get id token.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary with sign url.
        """

        try:
            oidc = OIDCAuthentication()
            status, id_token = oidc.get_id_token(vo, device_code, interval, expires_in)
            if status:
                return self.generate_http_response(HTTP_STATUS_CODE.OK, data=id_token)
            else:
                if 'error' in id_token and 'authorization_pending' in id_token['error']:
                    raise exceptions.AuthenticationPending(str(id_token))
                else:
                    raise exceptions.IDDSException("Failed to get oidc token: %s" % str(id_token))
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

    def post(self, vo):
        """ Refresh the token.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        try:
            parameters = self.get_request().data and json.loads(self.get_request().data)
            refresh_token = parameters['refresh_token']

            oidc = OIDCAuthentication()
            status, id_token = oidc.refresh_id_token(vo, refresh_token)
            if status:
                return self.generate_http_response(HTTP_STATUS_CODE.OK, data=id_token)
            else:
                raise exceptions.IDDSException("Failed to refresh oidc token: %s" % str(id_token))
        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(traceback.format_exc())
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
    bp = Blueprint('auth', __name__)

    url_view = OIDCAuthenticationSignURL.as_view('url')
    bp.add_url_rule('/auth/url/<vo>', view_func=url_view, methods=['get'])
    bp.add_url_rule('/auth/url/<vo>/<auth_type>', view_func=url_view, methods=['get'])

    token_view = OIDCAuthenticationToken.as_view('token')
    bp.add_url_rule('/auth/token/<vo>/<device_code>', view_func=token_view, methods=['get'])
    bp.add_url_rule('/auth/token/<vo>/<device_code>/<interval>', view_func=token_view, methods=['get'])
    bp.add_url_rule('/auth/token/<vo>/<device_code>/<interval>/<expires_in>', view_func=token_view, methods=['get'])
    bp.add_url_rule('/auth/token/<vo>', view_func=token_view, methods=['post'])
    return bp
