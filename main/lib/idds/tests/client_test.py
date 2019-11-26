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


import time
from uuid import uuid4 as uuid

from idds.client.client import Client
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host


def get_req_properties():
    properties = {
        'scope': 'test_scope',
        'name': 'test_name_%s' % str(uuid()),
        'requester': 'panda',
        'request_type': RequestType.EventStreaming,
        'transform_tag': 's3218',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': int(time.time())}
    }
    return properties


host = get_rest_host()
props = get_req_properties()
client = Client(host=host)
request_id = client.add_request(**props)
print(request_id)
