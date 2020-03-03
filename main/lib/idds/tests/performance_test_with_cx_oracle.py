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
import json
import time
import threading
import cx_Oracle

from uuid import uuid4 as uuid

from idds.common.config import config_get
from idds.common.constants import (TransformType, TransformStatus, CollectionType,
                                   CollectionRelationType, CollectionStatus,
                                   ContentType, ContentStatus)
from idds.orm.transforms import add_transform
from idds.orm.collections import add_collection
# from idds.core.contents import add_content


def add_content(coll_id, scope, name, min_id, max_id, content_type=ContentType.File, status=ContentStatus.New,
                bytes=0, md5=None, adler32=None, processing_id=None, storage_id=None, retries=0,
                path=None, expired_at=None, collcontent_metadata=None, connection=None):
    insert_coll_sql = """insert into atlas_idds.collections_content(coll_id, scope, name, min_id, max_id, content_type,
                                                                   status, bytes, md5, adler32, processing_id,
                                                                   storage_id, retries, path, created_at, updated_at,
                                                                   expired_at, collcontent_metadata)
                         values(:coll_id, :scope, :name, :min_id, :max_id, :content_type, :status, :bytes,
                                :md5, :adler32, :processing_id, :storage_id, :retries, :path, :created_at, :updated_at,
                                :expired_at, :collcontent_metadata)
                      """
    if isinstance(content_type, ContentType):
        content_type = content_type.value
    if isinstance(status, ContentStatus):
        status = status.value
    if collcontent_metadata:
        collcontent_metadata = json.dumps(collcontent_metadata)

    cursor = connection.cursor()
    cursor.execute(insert_coll_sql, {'coll_id': coll_id, 'scope': scope, 'name': name, 'min_id': min_id, 'max_id': max_id,
                                     'content_type': content_type, 'status': status, 'bytes': bytes, 'md5': md5,
                                     'adler32': adler32, 'processing_id': processing_id, 'storage_id': storage_id,
                                     'retries': retries, 'path': path, 'created_at': datetime.datetime.utcnow(),
                                     'updated_at': datetime.datetime.utcnow(), 'expired_at': expired_at,
                                     'collcontent_metadata': collcontent_metadata})
    cursor.close()


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


def test_insert_contents(coll_id, num_contents=1, db_pool=None):
    connection = db_pool.acquire()
    for i in range(num_contents):
        content_properties = get_content_prop()
        content_properties['coll_id'] = coll_id
        add_content(**content_properties, connection=connection)
    connection.commit()
    db_pool.release(connection)


def test_thread(num_contents_per_thread, num_contents_per_session, coll_id, db_pool):
    for i in range(num_contents_per_thread // num_contents_per_session):
        test_insert_contents(coll_id, num_contents_per_session, db_pool)


def get_session_pool():
    sql_connection = config_get('database', 'default')
    sql_connection = sql_connection.replace("oracle://", "")
    user_pass, tns = sql_connection.split('@')
    user, passwd = user_pass.split(':')
    db_pool = cx_Oracle.SessionPool(user, passwd, tns, min=12, max=20, increment=1)
    return db_pool


def test(num_threads=1, total_contents=1, num_colls_per_session=1):
    db_pool = get_session_pool()

    trans_properties = get_transform_prop()
    trans_id = add_transform(**trans_properties)
    coll_properties = get_collection_prop()
    coll_properties['transform_id'] = trans_id
    coll_id = add_collection(**coll_properties)

    time_start = time.time()
    threads = [threading.Thread(target=test_thread, args=(total_contents // num_threads, num_colls_per_session, coll_id, db_pool)) for i in range(num_threads)]
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
    test(num_threads=1, total_contents=1, num_colls_per_session=1)
    test(num_threads=1, total_contents=10000, num_colls_per_session=100)
    test(num_threads=1, total_contents=100000, num_colls_per_session=100)
    test(num_threads=1, total_contents=1000000, num_colls_per_session=100)
    test(num_threads=10, total_contents=10000, num_colls_per_session=100)
    test(num_threads=10, total_contents=100000, num_colls_per_session=100)
    test(num_threads=10, total_contents=1000000, num_colls_per_session=100)
