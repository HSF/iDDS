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

import os

from idds.client.client import Client
from idds.common.utils import get_rest_host


full_name = os.path.abspath(__file__)

host = get_rest_host()

client = Client(host=host)
client.upload(full_name)
client.download(os.path.join('/tmp', os.path.basename(full_name)))

full_name = '/bin/hostname'
client.upload(full_name)
client.download(os.path.join('/tmp', os.path.basename(full_name)))
