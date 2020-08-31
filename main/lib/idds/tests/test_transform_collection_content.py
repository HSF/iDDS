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

import copy

import unittest2 as unittest
from nose.tools import assert_equal, assert_in

from idds.common.constants import (TransformStatus, CollectionStatus, ContentStatus, ContentType)
from idds.common.utils import check_database, has_config, setup_logging
from idds.orm.requests import add_request
from idds.orm.transforms import (add_transform, delete_transform,
                                 update_transform, get_transform)
from idds.orm.collections import (add_collection, get_collection_id,
                                  get_collection, update_collection,
                                  delete_collection, get_collections,
                                  get_collection_ids_by_transform_id)
from idds.orm.contents import (add_content, get_content, update_content,
                               delete_content, get_contents,
                               get_match_contents, update_contents)
from idds.tests.common import (get_request_properties, get_transform_properties,
                               get_collection_properties, get_content_properties)

setup_logging(__name__)


class TestTransformCollectionContent(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_transform_orm(self):
        """ Transform (ORM): Test the creation, query, and cancel of a Transform """
        trans_properties = get_transform_properties()

        trans_id = add_transform(**trans_properties)
        transform = get_transform(transform_id=trans_id)
        for key in trans_properties:
            assert_equal(transform[key], trans_properties[key])

        update_transform(trans_id, {'status': TransformStatus.Failed})
        transform = get_transform(transform_id=trans_id)
        assert_equal(transform['status'], TransformStatus.Failed)

        delete_transform(trans_id)

        t = get_transform(transform_id=trans_id)
        assert_equal(t, None)

    def test_create_and_check_for_transform_collection_content_orm(self):
        """ Transform/Collection/Content (ORM): Test the creation, query, and cancel of a Transform/Collection/Content """

        trans_properties = get_transform_properties()
        coll_properties = get_collection_properties()
        content_properties = get_content_properties()

        trans_id = add_transform(**trans_properties)

        coll_properties['transform_id'] = trans_id
        coll_id = add_collection(**coll_properties)
        coll_id_1 = get_collection_id(transform_id=trans_id, relation_type=coll_properties['relation_type'])
        assert_equal(coll_id, coll_id_1)

        coll = get_collection(coll_id=coll_id)
        for key in coll_properties:
            assert_equal(coll[key], coll_properties[key])

        update_collection(coll_id, {'status': CollectionStatus.Closed})
        coll = get_collection(coll_id=coll_id)
        assert_equal(coll['status'], CollectionStatus.Closed)

        content_properties['coll_id'] = coll_id
        content_id = add_content(**content_properties)
        content_1 = get_content(coll_id=coll_id, scope=content_properties['scope'],
                                name=content_properties['name'],
                                content_type=ContentType.File)
        content_id_1 = content_1['content_id']
        assert_equal(content_id, content_id_1)

        content = get_content(content_id=content_id)
        update_content(content_id=content_id, parameters={'status': ContentStatus.Lost})
        content = get_content(content_id=content_id)
        assert_equal(content['status'], ContentStatus.Lost)

        delete_content(content_id=content_id)
        c = get_content(content_id=content_id)
        assert_equal(c, None)

        delete_collection(coll_id=coll_id)
        coll = get_collection(coll_id=coll_id)
        assert_equal(coll, None)

        delete_transform(transform_id=trans_id)
        t = get_transform(transform_id=trans_id)
        assert_equal(t, None)

    def test_get_collections_orm(self):
        """ Collections (ORM): Test get collections """

        req_properties = get_request_properties()
        trans_properties = get_transform_properties()
        coll_properties = get_collection_properties()

        request_id = add_request(**req_properties)
        trans_properties['request_id'] = request_id

        trans_id = add_transform(**trans_properties)

        coll_properties['transform_id'] = trans_id
        origin_coll_id = add_collection(**coll_properties)
        coll_properties1 = copy.deepcopy(coll_properties)
        coll_properties1['name'] = coll_properties['name'] + '_1'
        origin_coll_id1 = add_collection(**coll_properties1)
        origin_coll_id_list = [origin_coll_id, origin_coll_id1]

        colls = get_collections(transform_id=trans_id)
        assert_equal(len(colls), 2)
        for coll in colls:
            assert_in(coll['coll_id'], origin_coll_id_list)
            for key in coll_properties:
                if key == 'name':
                    continue
                assert_equal(coll[key], coll_properties[key])

        coll_ids = get_collection_ids_by_transform_id(transform_id=trans_id)
        assert_equal(len(coll_ids), 2)
        for coll_id in coll_ids:
            assert_in(coll_id, origin_coll_id_list)

        colls = get_collections(scope=coll_properties['scope'],
                                name=coll_properties['name'],
                                transform_id=[trans_id])
        assert_equal(len(colls), 1)
        for coll in colls:
            assert_in(coll['coll_id'], origin_coll_id_list)
            for key in coll_properties:
                if key == 'name':
                    continue
                assert_equal(coll[key], coll_properties[key])

    def test_contents_orm(self):
        """ Contents (ORM): Test contents """

        req_properties = get_request_properties()
        trans_properties = get_transform_properties()
        coll_properties = get_collection_properties()
        content_properties = get_content_properties()

        request_id = add_request(**req_properties)
        trans_properties['request_id'] = request_id

        trans_id = add_transform(**trans_properties)

        coll_properties['transform_id'] = trans_id
        coll_id = add_collection(**coll_properties)

        content_properties['coll_id'] = coll_id
        origin_content_id = add_content(**content_properties)
        content_properties1 = copy.deepcopy(content_properties)
        content_properties1['min_id'] = 101
        content_properties1['max_id'] = 200
        origin_content_id1 = add_content(**content_properties1)
        content_properties2 = copy.deepcopy(content_properties)
        content_properties2['min_id'] = 0
        content_properties2['max_id'] = 200
        origin_content_id2 = add_content(**content_properties2)
        content_properties3 = copy.deepcopy(content_properties)
        content_properties3['name'] = content_properties3['name'] + '_1'
        origin_content_id3 = add_content(**content_properties3)
        origin_content_ids = [origin_content_id, origin_content_id1,
                              origin_content_id2, origin_content_id3]

        contents = get_contents(coll_id=coll_id)
        assert_equal(len(contents), 4)
        for content in contents:
            assert_in(content['content_id'], origin_content_ids)

        contents = get_contents(scope=content_properties['scope'],
                                name=content_properties['name'],
                                coll_id=coll_id)
        assert_equal(len(contents), 3)
        for content in contents:
            assert_in(content['content_id'], origin_content_ids)

        contents = get_contents(scope=content_properties3['scope'],
                                name=content_properties3['name'],
                                coll_id=coll_id)
        assert_equal(len(contents), 1)
        assert_equal(contents[0]['content_id'], origin_content_id3)

        contents = get_match_contents(coll_id=content_properties['coll_id'],
                                      scope=content_properties['scope'],
                                      name=content_properties['name'],
                                      min_id=content_properties['min_id'],
                                      max_id=content_properties['max_id'])
        assert_equal(len(contents), 2)
        for content in contents:
            assert_in(content['content_id'], [origin_content_id, origin_content_id2])

        to_updates = [{'path': 'test_path1', 'status': ContentStatus.Processing, 'content_id': origin_content_id},
                      {'path': 'test_path2', 'status': ContentStatus.Processing, 'content_id': origin_content_id1}]
        update_contents(to_updates)
        content = get_content(content_id=origin_content_id)
        assert_equal(content['status'], ContentStatus.Processing)
        assert_equal(content['path'], 'test_path1')
        content = get_content(content_id=origin_content_id1)
        assert_equal(content['status'], ContentStatus.Processing)
        assert_equal(content['path'], 'test_path2')
