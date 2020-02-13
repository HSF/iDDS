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
performance test to insert contents.
"""
import datetime
import time
import threading

from uuid import uuid4 as uuid

from idds.orm.base.session import transactional_session
from idds.common.constants import (TransformType, TransformStatus, CollectionType,
                                   CollectionRelationType, CollectionStatus,
                                   ContentType, ContentStatus)
from idds.orm.transforms import add_transform
from idds.orm.collections import add_collection
from idds.orm.contents import add_contents


def get_transform_prop():
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


def get_collection_prop():
    coll_properties = {
        'scope': 'test_scope',
        'name': 'test_name_%s' % str(uuid()),
        'coll_type': CollectionType.Dataset,
        'request_id': None,
        'transform_id': None,
        'relation_type': CollectionRelationType.Input,
        'coll_size': 0,
        'status': CollectionStatus.New,
        'total_files': 0,
        'retries': 0,
        'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
        'coll_metadata': {'ddm_status': 'closed'}
    }
    return coll_properties


def get_content_prop():
    content_properties = {
        'coll_id': None,
        'scope': 'test_scope',
        'name': 'test_file_name_%s' % str(uuid()),
        'min_id': 0,
        'max_id': 100,
        'content_type': ContentType.File,
        'status': ContentStatus.New,
        'bytes': 1,
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


@transactional_session
def test_insert_contents(coll_id, num_contents=1, session=None):
    # print("test_insert_contents, num_contents: %s" % num_contents)
    list_contents = []
    for i in range(num_contents):
        content_properties = get_content_prop()
        content_properties['coll_id'] = coll_id
        list_contents.append(content_properties)
    add_contents(list_contents, bulk_size=num_contents, session=session)


def test_thread(num_contents_per_thread, num_contents_per_session, coll_id):
    for i in range(num_contents_per_thread // num_contents_per_session):
        test_insert_contents(coll_id, num_contents_per_session)


def test(num_threads=1, total_contents=1, num_colls_per_session=1):
    trans_properties = get_transform_prop()
    trans_id = add_transform(**trans_properties)
    coll_properties = get_collection_prop()
    coll_properties['transform_id'] = trans_id
    coll_id = add_collection(**coll_properties)

    time_start = time.time()
    threads = [threading.Thread(target=test_thread, args=(total_contents // num_threads, num_colls_per_session, coll_id)) for i in range(num_threads)]
    [thread.start() for thread in threads]
    while len(threads) > 0:
        left_threads = []
        for thread in threads:
            if thread.is_alive():
                left_threads.append(thread)
        time.sleep(0.1)
        threads = left_threads
    time_end = time.time()
    print("num_threads=%s, total_contents=%s, num_colls_per_session=%s, time used: %s" % (num_threads, total_contents, num_colls_per_session, time_end - time_start))


if __name__ == '__main__':
    test(num_threads=1, total_contents=10, num_colls_per_session=5)
    test(num_threads=1, total_contents=1, num_colls_per_session=1)
    test(num_threads=1, total_contents=10000, num_colls_per_session=1000)
    test(num_threads=1, total_contents=100000, num_colls_per_session=1000)
    test(num_threads=1, total_contents=1000000, num_colls_per_session=1000)
    test(num_threads=10, total_contents=10000, num_colls_per_session=1000)
    test(num_threads=10, total_contents=100000, num_colls_per_session=1000)
    test(num_threads=10, total_contents=1000000, num_colls_per_session=1000)
    test(num_threads=20, total_contents=10000, num_colls_per_session=500)
    test(num_threads=20, total_contents=100000, num_colls_per_session=500)
    test(num_threads=20, total_contents=1000000, num_colls_per_session=500)
