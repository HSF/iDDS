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
Test hyper parameter optimization test  client.
"""

import sys

from idds.client.client import Client
from idds.common.utils import get_rest_host

if len(sys.argv) == 2:
    workload_id = sys.argv[1]
    request_id = None
elif len(sys.argv) == 3:
    workload_id = sys.argv[1]
    request_id = sys.argv[2]

# host = "https://aipanda181.cern.ch:443/idds"
host = get_rest_host()

client = Client(host=host)
# props['request_metadata']['result_parser'] = 'default'

params = client.get_hyperparameters(workload_id=workload_id, request_id=request_id)
print(params)
if not params:
    print("No parameters")
else:
    param_0 = client.get_hyperparameters(workload_id=workload_id, request_id=request_id, id=0)
    print(param_0)
    if not param_0:
        print("failed to get param 0")
    for param in params:
        id = param['id']
        if param['loss'] is None:
            print("updating %s" % id)
            ret = client.update_hyperparameter(workload_id=workload_id, request_id=request_id, id=id, loss=0.3)
            print(ret)
            break
