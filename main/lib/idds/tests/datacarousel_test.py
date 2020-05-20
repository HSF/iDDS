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
Test client.
"""


from idds.client.client import Client
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request


def get_req_properties():
    req_properties = {
        'scope': 'data16_13TeV',
        'name': 'data16_13TeV.00298862.physics_Main.daq.RAW',
        'requester': 'panda',
        'request_type': RequestType.StageIn,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': '20776840', 'src_rse': 'NDGF-T1_DATATAPE', 'rule_id': '236e4bf87e11490291e3259b14724e30'}
    }
    return req_properties


host = get_rest_host()
props = get_req_properties()
# props = get_example_real_tape_stagein_request()
# props = get_example_prodsys2_tape_stagein_request()
# props = get_example_active_learning_request()

client = Client(host=host)
request_id = client.add_request(**props)
print(request_id)
