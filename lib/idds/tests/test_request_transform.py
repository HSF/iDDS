#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
Test Request.
"""
import datetime

import unittest2 as unittest
from uuid import uuid4 as uuid
from nose.tools import assert_equal, assert_raises

from idds.common import exceptions
from idds.common.constants import RequestType, RequestStatus, TransformType, TransformStatus
from idds.common.utils import check_database, has_config, setup_logging
from idds.core.requests import (add_request, get_request, delete_request)
from idds.core.transforms import (add_transform, get_transform, delete_transform)

setup_logging(__name__)


class TestTransform(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_request_transform_core(self):
        """ Transform (CORE): Test to create and delete a Transform """
        req_properties = {
            'scope': 'test_scope',
            'name': 'test_name_%s' % str(uuid()),
            'requester': 'panda',
            'request_type': RequestType.EventStreaming,
            'transform_tag': 's3218',
            'status': RequestStatus.New,
            'priority': 0,
            'lifetime': 30,
            'request_metadata': {'workload_id': 2019}
        }

        trans_properties = {
            'transform_type': TransformType.EventStreaming,
            'transform_tag': 's3128',
            'priority': 0,
            'status': TransformStatus.New,
            'retries': 0,
            'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
            'transform_metadata': {'input': {'coll_id': 123},
                                   'output': {'coll_id': 456},
                                   'log': {'coll_id': 789}}
        }

        request_id = add_request(**req_properties)

        trans_properties['request_id'] = request_id
        trans_id = add_transform(**trans_properties)
        transform = get_transform(transform_id=trans_id)
        for key in trans_properties:
            if key in ['request_id']:
                continue
            assert_equal(transform[key], trans_properties[key])
        delete_transform(trans_id)
        delete_request(request_id)

        with assert_raises(exceptions.NoObject):
            get_request(request_id=request_id)

        with assert_raises(exceptions.NoObject):
            get_transform(transform_id=trans_id)
