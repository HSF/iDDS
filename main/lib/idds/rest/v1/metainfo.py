#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


import datetime
from traceback import format_exc

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE, MetaStatus
from idds.common.utils import get_asyncresult_config, get_prompt_broker_config
from idds.core import meta as core_meta

from idds.rest.v1.controller import IDDSController


EJFAT_NAME_PREFIX = 'ejfat_'


def _get_active_ejfat_info(logger=None):
    """
    Retrieve active, non-expired ejfat_<run_id> meta items, keyed by run_id.

    Expired active items (created_at + lifetime seconds < now) are marked
    UnActive and saved back.
    """
    ejfat = {}
    now = datetime.datetime.utcnow()
    ejfat_items = core_meta.get_meta_items(name_prefix=EJFAT_NAME_PREFIX, status=MetaStatus.Active) or []
    for item in ejfat_items:
        meta_info = item.get('meta_info') or {}
        lifetime = meta_info.get('lifetime')
        created_at = item.get('created_at')
        if lifetime is not None and created_at is not None and now > created_at + datetime.timedelta(seconds=lifetime):
            core_meta.update_meta_item(name=item['name'], status=MetaStatus.UnActive)
            if logger:
                logger.info(f"Meta item {item['name']} expired (created_at={created_at}, lifetime={lifetime}), marked UnActive")
            continue

        run_id = item['name'][len(EJFAT_NAME_PREFIX):]
        ejfat[run_id] = {'instance_uri': meta_info.get('instance_uri'), 'lifetime': lifetime, 'created_at': created_at}
    return ejfat


class MetaInfo(IDDSController):
    """ Get Meta info"""

    def get(self, name):
        logger = self.get_logger()
        try:
            logger.info(f"Getting meta info for {name}")

            rets = {}
            if name == 'asyncresult_config':
                asyncresult_config = get_asyncresult_config()
                rets = asyncresult_config
            elif name == 'prompt_broker':
                prompt_broker_config = get_prompt_broker_config()
                rets = prompt_broker_config
                rets['ejfat'] = _get_active_ejfat_info(logger=logger)

            logger.info(f"Meta info for {name} retrieved successfully: {rets}")

        except exceptions.NoObject as error:
            return self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.IDDSException as error:
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            logger.error(f"Error getting meta info for {name}: {error}\n{format_exc()}")
            return self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        return self.generate_http_response(HTTP_STATUS_CODE.OK, data=rets)


def get_blueprint():
    bp = Blueprint('metainfo', __name__)

    metainfo_view = MetaInfo.as_view('metainfo')
    bp.add_url_rule('/metainfo/<name>', view_func=metainfo_view, methods=['get', ])

    return bp
