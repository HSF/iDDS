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
        'name': 'data15_13TeV.00270949.pseudo.1',
        'requester': 'panda',
        'request_type': RequestType.HyperParameterOpt,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        # 'request_metadata': {'workload_id': '20525134', 'sandbox': None, 'executable': 'docker run --rm -it -v "$(pwd)":/payload gitlab-registry.cern.ch/zhangruihpc/endpointcontainer:latest /bin/bash -c "/bin/cat /payload/input_json.txt>/payload/output_json.txt"', 'arguments': '-s --input %IN', 'output_json': 'output.json'}  # noqa: E501
        'request_metadata': {'workload_id': '20525134', 'is_pseudo_input': True, 'sandbox': None, 'executable': 'docker', 'arguments': 'run --rm -it -v "$(pwd)":/payload gitlab-registry.cern.ch/zhangruihpc/endpointcontainer:latest /bin/bash -c "echo "--num_points %NUM_POINTS"; /bin/cat /payload/%IN>/payload/%OUT"', 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'output_json': 'output.json'}  # noqa: E501
    }
    return req_properties


host = get_rest_host()
props = get_req_properties()
test_codes = get_test_codes()

client = Client(host=host)
# props['request_metadata']['result_parser'] = 'default'

request_id = client.add_request(**props)
print(request_id)
