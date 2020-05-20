#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


"""
Test active learning client.
"""

import os

from idds.client.client import Client
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host


def get_test_codes():
    dir_name = os.path.dirname(os.path.abspath(__file__))
    test_codes = os.path.join(dir_name, 'activelearning_test_codes/activelearning_test_codes.tgz')
    return test_codes


def get_req_properties():
    req_properties = {
        'scope': 'data15_13TeV',
        'name': 'data15_13TeV.00270949.physics_Main.merge.AOD.r7600_p2521_tid07734829_00',
        'requester': 'panda',
        'request_type': RequestType.ActiveLearning,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': '20525134', 'sandbox': 'https://', 'executable': 'hostname', 'arguments': '-s --input %IN', 'output_json': 'output.json'}
    }
    return req_properties


host = get_rest_host()
props = get_req_properties()
test_codes = get_test_codes()

client = Client(host=host)
test_codes_url = client.upload(test_codes)
props['request_metadata']['sandbox'] = test_codes_url
props['request_metadata']['executable'] = 'test.sh'
props['request_metadata']['arguments'] = '-1 -2 test'
# props['request_metadata']['result_parser'] = 'default'

request_id = client.add_request(**props)
print(request_id)
