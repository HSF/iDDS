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

from idds.client.client import Client
from idds.common.constants import CollectionRelationType, ContentStatus
from idds.common.utils import get_rest_host


# host = "https://aipanda181.cern.ch:443/idds"
host = get_rest_host()

client = Client(host=host)
# props['request_metadata']['result_parser'] = 'default'

scope = 'data16_13TeV'
name = 'data16_13TeV.00298862.physics_Main.daq.RAW.idds.stagein'
request_id = 12
workload_id = 1601235010
relation_type = CollectionRelationType.Output  # Input, Log
status = ContentStatus.Available  # New, Processing, Available, ...

colls = client.get_collections(scope=scope, name=name, request_id=request_id, workload_id=workload_id, relation_type=relation_type)
print(colls)
# example outputs
# [{'relation_type': <CollectionRelationType.Output: 1>, 'next_poll_at': 'Sun, 27 Sep 2020 19:30:16 UTC', 'storage_id': None, 'scope': 'data16_13TeV', 'accessed_at': 'Sun, 27 Sep 2020 21:31:40 UTC', 'new_files': 0, 'name': 'data16_13TeV.00298862.physics_Main.daq.RAW.idds.stagein', 'expired_at': 'Tue, 27 Oct 2020 19:30:10 UTC', 'processed_files': 0, 'bytes': 0, 'coll_metadata': {'internal_id': 'db1bb0dc-00f7-11eb-a5d2-fa163eb98fd2'}, 'request_id': 12, 'processing_files': 0, 'status': <CollectionStatus.Open: 3>, 'coll_id': 16, 'processing_id': None, 'substatus': None, 'workload_id': 1601235010, 'retries': 0, 'locking': <CollectionLocking.Idle: 0>, 'coll_type': <CollectionType.Dataset: 1>, 'created_at': 'Sun, 27 Sep 2020 19:30:16 UTC', 'total_files': 0, 'transform_id': 9, 'updated_at': 'Sun, 27 Sep 2020 21:31:40 UTC'}]  # noqa: E501

contents = client.get_contents(coll_scope=scope, coll_name=name, request_id=request_id, workload_id=workload_id, relation_type=relation_type, status=status)
for content in contents:
    # print(content)
    pass

# example outputs
# {'substatus': <ContentStatus.Available: 2>, 'transform_id': 9, 'path': None, 'locking': <ContentLocking.Idle: 0>, 'map_id': 8895, 'created_at': 'Sun, 27 Sep 2020 19:30:40 UTC', 'bytes': 534484132, 'scope': 'data16_13TeV', 'updated_at': 'Sun, 27 Sep 2020 19:44:55 UTC', 'md5': None, 'name': 'data16_13TeV.00298862.physics_Main.daq.RAW._lb0538._SFO-5._0003.data', 'accessed_at': 'Sun, 27 Sep 2020 19:44:56 UTC', 'adler32': 'e8543989', 'content_id': 2361650, 'min_id': 0, 'expired_at': 'Tue, 27 Oct 2020 19:30:26 UTC', 'coll_id': 16, 'processing_id': None, 'max_id': 579, 'content_metadata': {'events': 579}, 'storage_id': None, 'request_id': 12, 'content_type': <ContentType.File: 0>, 'workload_id': 1601235010, 'retries': 0, 'status': <ContentStatus.Available: 2>}  # noqa: E501
