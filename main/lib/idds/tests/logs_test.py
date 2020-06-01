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
Test cacher.
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


host = get_rest_host()

client = Client(host=host)
filename = client.download_logs(workload_id=workload_id, request_id=request_id, dest_dir='/tmp')
print(filename)
