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

import random
import time
import unittest2 as unittest
from nose.tools import assert_equal

from idds.common.utils import check_database, has_config, setup_logging
from idds.orm.requests import (add_request, get_request, delete_requests)
from idds.orm.transforms import (add_transform, get_transform, get_transform_ids, delete_transform)
from idds.tests.common import get_request_properties, get_transform_properties

setup_logging(__name__)


class TestTransform(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_request_transform_orm(self):
        """ Transform (ORM): Test to create and delete a Transform """
        req_properties = get_request_properties()
        req_properties['workload_id'] = int(time.time()) + random.randint(1, 1000000)
        trans_properties = get_transform_properties()

        request_id = add_request(**req_properties)

        trans_properties['request_id'] = request_id
        trans_id = add_transform(**trans_properties)
        transform = get_transform(transform_id=trans_id)
        for key in trans_properties:
            if key in ['request_id']:
                continue
            assert_equal(transform[key], trans_properties[key])

        trans_ids1 = get_transform_ids(request_id=request_id)
        trans_ids2 = get_transform_ids(workload_id=req_properties['workload_id'])
        trans_ids3 = get_transform_ids(transform_id=trans_id)
        trans_ids4 = get_transform_ids(request_id=request_id, workload_id=req_properties['workload_id'])
        trans_ids5 = get_transform_ids(request_id=request_id, workload_id=req_properties['workload_id'], transform_id=trans_id)
        trans_ids6 = get_transform_ids(request_id=request_id, transform_id=trans_id)
        trans_ids7 = get_transform_ids(workload_id=req_properties['workload_id'], transform_id=trans_id)
        assert_equal(trans_ids1, trans_ids2)
        assert_equal(trans_ids2, trans_ids3)
        assert_equal(trans_ids3, trans_ids4)
        assert_equal(trans_ids4, trans_ids5)
        assert_equal(trans_ids5, trans_ids6)
        assert_equal(trans_ids6, trans_ids7)

        delete_transform(trans_id)
        delete_requests(request_id=request_id)

        req = get_request(request_id=request_id)
        assert_equal(req, None)

        trans = get_transform(transform_id=trans_id)
        assert_equal(trans, None)
