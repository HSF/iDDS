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

import unittest2 as unittest
from uuid import uuid4 as uuid
from nose.tools import assert_equal, assert_raises

from idds.common import exceptions
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import check_database, has_config, setup_logging
from idds.core.requests import (add_request, get_request, update_request,
                                delete_request)

setup_logging(__name__)


class TestRequest(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_request_core(self):
        """ Request (CORE): Test the creation, query, and cancel of a Request """
        properties = {
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
        request_id = add_request(**properties)
        # request = get_request(properties['scope'], properties['name'], requester=properties['requester'])
        # assert_equal(request_id, request.request_id)

        request = get_request(request_id=request_id)
        assert_equal(request_id, request['request_id'])

        for key in properties:
            if key in ['lifetime']:
                continue
            assert_equal(request[key], properties[key])

        request_id1 = add_request(**properties)
        delete_request(request_id1)

        update_request(request_id, parameters={'status': RequestStatus.Failed})
        request = get_request(request_id=request_id)
        assert_equal(request['status'], RequestStatus.Failed)

        with assert_raises(exceptions.NoObject):
            get_request(request_id=999999)

        delete_request(request_id)

        with assert_raises(exceptions.NoObject):
            get_request(request_id=request_id)
