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
Test Request.
"""
import datetime

import unittest2 as unittest
from uuid import uuid4 as uuid
from nose.tools import assert_equal, assert_raises

from idds.common import exceptions
from idds.common.constants import (TransformType, TransformStatus, CollectionType,
                                   CollectionRelationType, CollectionStatus,
                                   ContentType, ContentStatus)
from idds.common.utils import check_database, has_config, setup_logging
from idds.core.transforms import (add_transform, delete_transform,
                                  update_transform, get_transform)
from idds.core.collections import (add_collection, get_collection_id,
                                   get_collection, update_collection,
                                   delete_collection)
from idds.core.contents import (add_content, get_content, update_content,
                                get_content_id, delete_content)


setup_logging(__name__)


class TestTransformCollectionContent(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_transform_core(self):
        """ Transform (CORE): Test the creation, query, and cancel of a Transform """
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

        trans_id = add_transform(**trans_properties)
        transform = get_transform(transform_id=trans_id)
        for key in trans_properties:
            assert_equal(transform[key], trans_properties[key])

        update_transform(trans_id, {'status': TransformStatus.Failed})
        transform = get_transform(transform_id=trans_id)
        assert_equal(transform['status'], TransformStatus.Failed)

        delete_transform(trans_id)

        with assert_raises(exceptions.NoObject):
            get_transform(transform_id=trans_id)

    def test_create_and_check_for_transform_collection_content_core(self):
        """ Transform/Collection/Content (CORE): Test the creation, query, and cancel of a Transform/Collection/Content """
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

        trans_id = add_transform(**trans_properties)

        coll_properties['transform_id'] = trans_id
        coll_id = add_collection(**coll_properties)
        coll_id_1 = get_collection_id(transform_id=trans_id, relation_type=coll_properties['relation_type'])
        assert_equal(coll_id, coll_id_1)

        coll = get_collection(coll_id=coll_id)
        for key in coll_properties:
            assert_equal(coll[key], coll_properties[key])

        update_collection(coll_id, {'coll_status': CollectionStatus.Closed})
        coll = get_collection(coll_id=coll_id)
        assert_equal(coll['coll_status'], CollectionStatus.Closed)

        content_properties['coll_id'] = coll_id
        content_id = add_content(**content_properties)
        content_id_1 = get_content_id(coll_id=coll_id, scope=content_properties['scope'],
                                      name=content_properties['name'])
        assert_equal(content_id, content_id_1)

        content = get_content(content_id=content_id)
        update_content(content_id=content_id, parameters={'status': ContentStatus.Lost})
        content = get_content(content_id=content_id)
        assert_equal(content['status'], ContentStatus.Lost)

        delete_content(content_id=content_id)
        with assert_raises(exceptions.NoObject):
            get_content(content_id=content_id)

        delete_collection(coll_id=coll_id)
        with assert_raises(exceptions.NoObject):
            get_collection(coll_id=coll_id)

        delete_transform(transform_id=trans_id)
        with assert_raises(exceptions.NoObject):
            get_transform(transform_id=trans_id)
