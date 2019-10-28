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
common funcs for tests
"""

import datetime
from uuid import uuid4 as uuid

from idds.common.constants import (RequestType, RequestStatus,
                                   TransformType, TransformStatus, CollectionType,
                                   CollectionRelationType, CollectionStatus,
                                   ContentType, ContentStatus,
                                   ProcessingStatus, GranularityType)


def get_request_properties():
    req_properties = {
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
    return req_properties


def get_transform_properties():
    trans_properties = {
        'transform_type': TransformType.EventStreaming,
        'transform_tag': 's3128',
        'priority': 0,
        'status': TransformStatus.New,
        'retries': 0,
        'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
        'transform_metadata': {'input': {'coll_id': 123},
                               'output': {'coll_id': 456},
                               'log': {'coll_id': 789}}
    }
    return trans_properties


def get_collection_properties():
    coll_properties = {
        'scope': 'test_scope',
        'name': 'test_name_%s' % str(uuid()),
        'coll_type': CollectionType.Dataset,
        'request_id': None,
        'transform_id': None,
        'relation_type': CollectionRelationType.Input,
        'coll_size': 0,
        'coll_status': CollectionStatus.New,
        'total_files': 0,
        'retries': 0,
        'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
        'coll_metadata': {'ddm_status': 'closed'}
    }
    return coll_properties


def get_content_properties():
    content_properties = {
        'coll_id': None,
        'scope': 'test_scope',
        'name': 'test_file_name_%s' % str(uuid()),
        'min_id': 0,
        'max_id': 100,
        'content_type': ContentType.File,
        'status': ContentStatus.New,
        'content_size': 1,
        'md5': None,
        'adler32': None,
        'processing_id': None,
        'storage_id': None,
        'retries': 0,
        'path': None,
        'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
        'collcontent_metadata': {'id': 123}
    }
    return content_properties


def get_processing_properties():
    proc_properties = {
        'transform_id': None,
        'status': ProcessingStatus.New,
        'submitter': 'panda',
        'granularity': 10,
        'granularity_type': GranularityType.File,
        'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
        'processing_metadata': {'task_id': 40191323}
    }
    return proc_properties
