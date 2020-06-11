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

from idds.client.client import Client
from idds.common.constants import (CollectionRelationType, ContentType, ContentStatus)
from idds.common.utils import (check_database, has_config, setup_logging,
                               check_rest_host, get_rest_host, check_user_proxy)
from idds.core.catalog import (get_collections, get_contents,
                               register_output_contents, get_match_contents)
from idds.orm.requests import add_request
from idds.orm.transforms import add_transform
from idds.orm.collections import add_collection
from idds.orm.contents import add_content, get_content
from idds.tests.common import (get_request_properties, get_transform_properties,
                               get_collection_properties, get_content_properties,
                               is_same_req_trans_colls, is_same_req_trans_coll_contents)

setup_logging(__name__)


class TestCatalog(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_catalog_core(self):
        """ Catalog (Core): Test catalog core functions """
        req_properties = get_request_properties()
        origin_request_id = add_request(**req_properties)

        trans_properties = get_transform_properties()
        trans_properties['request_id'] = origin_request_id
        origin_trans_id = add_transform(**trans_properties)

        coll_properties = get_collection_properties()
        coll_properties['transform_id'] = origin_trans_id
        coll_input_properties = copy.deepcopy(coll_properties)
        coll_input_properties['name'] = coll_input_properties['name'] + '_input'
        coll_output_properties = copy.deepcopy(coll_properties)
        coll_output_properties['relation_type'] = CollectionRelationType.Output
        coll_output_properties['name'] = coll_output_properties['name'] + '_output'
        coll_log_properties = copy.deepcopy(coll_properties)
        coll_log_properties['relation_type'] = CollectionRelationType.Log
        coll_log_properties['name'] = coll_log_properties['name'] + '_log'

        origin_coll_input_id = add_collection(**coll_input_properties)
        origin_coll_output_id = add_collection(**coll_output_properties)
        origin_coll_log_id = add_collection(**coll_log_properties)
        origin_coll_id_list = [origin_coll_input_id, origin_coll_output_id, origin_coll_log_id]

        req_trans_colls = get_collections(request_id=origin_request_id, workload_id=None)
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(origin_request_id, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 1)
        trans_id = list(req_trans_colls[req_id].keys())[0]
        assert_equal(trans_id, origin_trans_id)
        colls = req_trans_colls[req_id][trans_id]
        assert_equal(len(colls), 3)
        for coll in colls:
            assert_in(coll['coll_id'], origin_coll_id_list)
            for key in coll_input_properties:
                if key == 'relation_type':
                    if coll['coll_id'] == origin_coll_input_id:
                        assert_equal(coll[key], CollectionRelationType.Input)
                    elif coll['coll_id'] == origin_coll_output_id:
                        assert_equal(coll[key], CollectionRelationType.Output)
                    else:
                        assert_equal(coll['coll_id'], origin_coll_log_id)
                        assert_equal(coll[key], CollectionRelationType.Log)
                if key == 'name':
                    if coll['coll_id'] == origin_coll_input_id:
                        assert_equal(coll[key], coll_input_properties[key])
                    elif coll['coll_id'] == origin_coll_output_id:
                        assert_equal(coll[key], coll_output_properties[key])
                    else:
                        assert_equal(coll[key], coll_log_properties[key])

        req_trans_colls1 = get_collections(request_id=None, workload_id=req_properties['workload_id'])
        assert_equal(is_same_req_trans_colls(req_trans_colls, req_trans_colls1, allow_request_id_None=True), True)

        req_trans_colls1 = get_collections(request_id=origin_request_id, workload_id=req_properties['workload_id'])
        assert_equal(is_same_req_trans_colls(req_trans_colls, req_trans_colls1), True)

        req_trans_colls = get_collections(scope=coll_properties['scope'], name=coll_properties['name'] + "*", request_id=None, workload_id=None)
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(None, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 1)
        trans_id = list(req_trans_colls[req_id].keys())[0]
        assert_equal(trans_id, origin_trans_id)
        colls = req_trans_colls[req_id][trans_id]
        assert_equal(len(colls), 3)
        for coll in colls:
            assert_in(coll['coll_id'], origin_coll_id_list)
            for key in coll_input_properties:
                if key == 'relation_type':
                    if coll['coll_id'] == origin_coll_input_id:
                        assert_equal(coll[key], CollectionRelationType.Input)
                    elif coll['coll_id'] == origin_coll_output_id:
                        assert_equal(coll[key], CollectionRelationType.Output)
                    else:
                        assert_equal(coll['coll_id'], origin_coll_log_id)
                        assert_equal(coll[key], CollectionRelationType.Log)
                if key == 'name':
                    if coll['coll_id'] == origin_coll_input_id:
                        assert_equal(coll[key], coll_input_properties[key])
                    elif coll['coll_id'] == origin_coll_output_id:
                        assert_equal(coll[key], coll_output_properties[key])
                    else:
                        assert_equal(coll[key], coll_log_properties[key])

        req_trans_colls = get_collections(scope=coll_properties['scope'], name=coll_input_properties['name'], request_id=origin_request_id, workload_id=None)
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(origin_request_id, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 1)
        trans_id = list(req_trans_colls[req_id].keys())[0]
        assert_equal(trans_id, origin_trans_id)
        colls = req_trans_colls[req_id][trans_id]
        assert_equal(len(colls), 1)
        coll = colls[0]
        for key in coll_properties:
            if key == 'relation_type':
                assert_equal(coll[key], CollectionRelationType.Input)
            if key == 'name':
                assert_equal(coll[key], coll_input_properties[key])

        req_trans_colls = get_collections(scope=coll_properties['scope'], name=coll_output_properties['name'], request_id=None, workload_id=req_properties['workload_id'])
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(None, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 1)
        trans_id = list(req_trans_colls[req_id].keys())[0]
        assert_equal(trans_id, origin_trans_id)
        colls = req_trans_colls[req_id][trans_id]
        assert_equal(len(colls), 1)
        coll = colls[0]
        for key in coll_properties:
            if key == 'relation_type':
                assert_equal(coll[key], CollectionRelationType.Output)
            if key == 'name':
                assert_equal(coll[key], coll_output_properties[key])

        req_trans_colls = get_collections(scope=coll_properties['scope'], name=coll_log_properties['name'], request_id=origin_request_id, workload_id=req_properties['workload_id'])
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(origin_request_id, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 1)
        trans_id = list(req_trans_colls[req_id].keys())[0]
        assert_equal(trans_id, origin_trans_id)
        colls = req_trans_colls[req_id][trans_id]
        assert_equal(len(colls), 1)
        coll = colls[0]
        for key in coll_properties:
            if key == 'relation_type':
                assert_equal(coll[key], CollectionRelationType.Log)
            if key == 'name':
                assert_equal(coll[key], coll_log_properties[key])

        trans_properties1 = copy.deepcopy(trans_properties)
        origin_trans_id1 = add_transform(**trans_properties1)
        coll_properties1 = copy.deepcopy(coll_properties)
        coll_properties1['name'] = coll_properties1['name'] + '_1'
        coll_properties1['transform_id'] = origin_trans_id1
        coll_input_properties1 = copy.deepcopy(coll_properties1)
        coll_input_properties1['name'] = coll_input_properties1['name'] + '_input'
        coll_output_properties1 = copy.deepcopy(coll_properties1)
        coll_output_properties1['relation_type'] = CollectionRelationType.Output
        coll_output_properties1['name'] = coll_output_properties1['name'] + '_output'
        coll_log_properties1 = copy.deepcopy(coll_properties1)
        coll_log_properties1['relation_type'] = CollectionRelationType.Log
        coll_log_properties1['name'] = coll_log_properties1['name'] + '_log'

        origin_coll_input_id1 = add_collection(**coll_input_properties1)
        origin_coll_output_id1 = add_collection(**coll_output_properties1)
        origin_coll_log_id1 = add_collection(**coll_log_properties1)
        origin_coll_id_list1 = [origin_coll_input_id1, origin_coll_output_id1, origin_coll_log_id1]

        req_trans_colls = get_collections(request_id=origin_request_id, workload_id=None)
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(origin_request_id, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 2)
        for trans_id in req_trans_colls[req_id]:
            if trans_id == origin_trans_id:
                colls = req_trans_colls[req_id][trans_id]
                assert_equal(len(colls), 3)
                for coll in colls:
                    assert_in(coll['coll_id'], origin_coll_id_list)
                    for key in coll_properties:
                        if key == 'relation_type':
                            if coll['coll_id'] == origin_coll_input_id:
                                assert_equal(coll[key], CollectionRelationType.Input)
                            elif coll['coll_id'] == origin_coll_output_id:
                                assert_equal(coll[key], CollectionRelationType.Output)
                            else:
                                assert_equal(coll['coll_id'], origin_coll_log_id)
                                assert_equal(coll[key], CollectionRelationType.Log)
                        if key == 'name':
                            if coll['coll_id'] == origin_coll_input_id:
                                assert_equal(coll[key], coll_input_properties[key])
                            elif coll['coll_id'] == origin_coll_output_id:
                                assert_equal(coll[key], coll_output_properties[key])
                            else:
                                assert_equal(coll[key], coll_log_properties[key])
            if trans_id == origin_trans_id1:
                colls = req_trans_colls[req_id][trans_id]
                assert_equal(len(colls), 3)
                for coll in colls:
                    assert_in(coll['coll_id'], origin_coll_id_list1)
                    for key in coll_properties:
                        if key == 'relation_type':
                            if coll['coll_id'] == origin_coll_input_id1:
                                assert_equal(coll[key], CollectionRelationType.Input)
                            elif coll['coll_id'] == origin_coll_output_id1:
                                assert_equal(coll[key], CollectionRelationType.Output)
                            else:
                                assert_equal(coll['coll_id'], origin_coll_log_id1)
                                assert_equal(coll[key], CollectionRelationType.Log)
                        if key == 'name':
                            if coll['coll_id'] == origin_coll_input_id1:
                                assert_equal(coll[key], coll_input_properties1[key])
                            elif coll['coll_id'] == origin_coll_output_id1:
                                assert_equal(coll[key], coll_output_properties1[key])
                            else:
                                assert_equal(coll[key], coll_log_properties1[key])

        req_trans_colls1 = get_collections(request_id=None, workload_id=req_properties['workload_id'])
        assert_equal(is_same_req_trans_colls(req_trans_colls, req_trans_colls1, allow_request_id_None=True), True)

        req_trans_colls1 = get_collections(request_id=origin_request_id, workload_id=req_properties['workload_id'])
        assert_equal(is_same_req_trans_colls(req_trans_colls, req_trans_colls1), True)

        req_trans_colls = get_collections(scope=coll_properties['scope'], name=coll_properties['name'] + "*", request_id=None, workload_id=None)
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(None, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 2)
        for trans_id in req_trans_colls[req_id]:
            if trans_id == origin_trans_id:
                colls = req_trans_colls[req_id][trans_id]
                assert_equal(len(colls), 3)
                for coll in colls:
                    assert_in(coll['coll_id'], origin_coll_id_list)
                    for key in coll_properties:
                        if key == 'relation_type':
                            if coll['coll_id'] == origin_coll_input_id:
                                assert_equal(coll[key], CollectionRelationType.Input)
                            elif coll['coll_id'] == origin_coll_output_id:
                                assert_equal(coll[key], CollectionRelationType.Output)
                            else:
                                assert_equal(coll['coll_id'], origin_coll_log_id)
                                assert_equal(coll[key], CollectionRelationType.Log)
                        if key == 'name':
                            if coll['coll_id'] == origin_coll_input_id:
                                assert_equal(coll[key], coll_input_properties[key])
                            elif coll['coll_id'] == origin_coll_output_id:
                                assert_equal(coll[key], coll_output_properties[key])
                            else:
                                assert_equal(coll[key], coll_log_properties[key])
            if trans_id == origin_trans_id1:
                colls = req_trans_colls[req_id][trans_id]
                assert_equal(len(colls), 3)
                for coll in colls:
                    assert_in(coll['coll_id'], origin_coll_id_list1)
                    for key in coll_properties:
                        if key == 'relation_type':
                            if coll['coll_id'] == origin_coll_input_id1:
                                assert_equal(coll[key], CollectionRelationType.Input)
                            elif coll['coll_id'] == origin_coll_output_id1:
                                assert_equal(coll[key], CollectionRelationType.Output)
                            else:
                                assert_equal(coll['coll_id'], origin_coll_log_id1)
                                assert_equal(coll[key], CollectionRelationType.Log)
                        if key == 'name':
                            if coll['coll_id'] == origin_coll_input_id1:
                                assert_equal(coll[key], coll_input_properties1[key])
                            elif coll['coll_id'] == origin_coll_output_id1:
                                assert_equal(coll[key], coll_output_properties1[key])
                            else:
                                assert_equal(coll[key], coll_log_properties1[key])

        req_properties = get_request_properties()
        origin_request_id = add_request(**req_properties)

        trans_properties = get_transform_properties()
        trans_properties['request_id'] = origin_request_id
        origin_trans_id = add_transform(**trans_properties)

        coll_properties = get_collection_properties()
        coll_properties['transform_id'] = origin_trans_id
        coll_input_properties = copy.deepcopy(coll_properties)
        coll_input_properties['name'] = coll_input_properties['name'] + '_input'
        coll_output_properties = copy.deepcopy(coll_properties)
        coll_output_properties['relation_type'] = CollectionRelationType.Output
        coll_output_properties['name'] = coll_output_properties['name'] + '_output'

        origin_coll_input_id = add_collection(**coll_input_properties)
        origin_coll_output_id = add_collection(**coll_output_properties)

        content_input_properties = get_content_properties()
        content_input_properties['coll_id'] = origin_coll_input_id
        origin_content_input_id_0_100 = add_content(**content_input_properties)
        content_input_properties_100_200 = copy.deepcopy(content_input_properties)
        content_input_properties_100_200['min_id'] = 100
        content_input_properties_100_200['max_id'] = 200
        origin_content_input_id_100_200 = add_content(**content_input_properties_100_200)
        content_input_properties_name1 = copy.deepcopy(content_input_properties)
        content_input_properties_name1['name'] = content_input_properties_name1['name'] + '_1'
        content_input_properties_name1_id = add_content(**content_input_properties_name1)

        content_output_properties = get_content_properties()
        content_output_properties['content_type'] = ContentType.File
        content_output_properties['min_id'] = 0
        content_output_properties['max_id'] = 1000
        content_output_properties['coll_id'] = origin_coll_output_id
        origin_content_output_id_0_1000 = add_content(**content_output_properties)
        content_output_properties_0_100 = copy.deepcopy(content_output_properties)
        content_output_properties_0_100['min_id'] = 0
        content_output_properties_0_100['max_id'] = 100
        content_output_properties['content_type'] = ContentType.Event
        origin_content_output_id_0_100 = add_content(**content_output_properties_0_100)
        content_output_properties_100_200 = copy.deepcopy(content_output_properties)
        content_output_properties_100_200['min_id'] = 100
        content_output_properties_100_200['max_id'] = 200
        content_output_properties['content_type'] = ContentType.Event
        origin_content_output_id_100_200 = add_content(**content_output_properties_100_200)
        content_output_properties_name1 = copy.deepcopy(content_output_properties)
        content_output_properties_name1['name'] = content_output_properties_name1['name'] + '_1'
        content_output_properties_name1_id = add_content(**content_output_properties_name1)

        req_trans_coll_contents = get_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'] + "*",
                                               request_id=origin_request_id, workload_id=req_properties['workload_id'])
        coll_contents = req_trans_coll_contents[origin_request_id][origin_trans_id]
        coll_input_scope_name = '%s:%s' % (coll_input_properties['scope'], coll_input_properties['name'])
        coll_output_scope_name = '%s:%s' % (coll_output_properties['scope'], coll_output_properties['name'])
        coll_scope_names = [scope_name for scope_name in coll_contents]
        assert_equal(coll_scope_names, [coll_input_scope_name, coll_output_scope_name])
        input_contents = coll_contents[coll_input_scope_name]['contents']
        output_contents = coll_contents[coll_output_scope_name]['contents']
        assert_equal(len(input_contents), 3)
        assert_equal(len(output_contents), 4)
        input_content_ids = [input_content['content_id'] for input_content in input_contents]
        assert_equal(input_content_ids, [origin_content_input_id_0_100, origin_content_input_id_100_200, content_input_properties_name1_id])
        output_content_ids = [output_content['content_id'] for output_content in output_contents]
        output_content_ids.sort()
        assert_equal(output_content_ids, [origin_content_output_id_0_1000, origin_content_output_id_0_100,
                                          origin_content_output_id_100_200, content_output_properties_name1_id])

        req_trans_coll_contents = get_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'] + "*",
                                               request_id=origin_request_id, workload_id=req_properties['workload_id'],
                                               relation_type=CollectionRelationType.Input)
        coll_contents = req_trans_coll_contents[origin_request_id][origin_trans_id]
        coll_input_scope_name = '%s:%s' % (coll_input_properties['scope'], coll_input_properties['name'])
        coll_scope_names = list(coll_contents.keys())
        assert_equal(coll_scope_names, [coll_input_scope_name])

        req_trans_coll_contents = get_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'] + "*",
                                               request_id=origin_request_id, workload_id=req_properties['workload_id'],
                                               relation_type=CollectionRelationType.Output)
        coll_contents = req_trans_coll_contents[origin_request_id][origin_trans_id]
        coll_output_scope_name = '%s:%s' % (coll_output_properties['scope'], coll_output_properties['name'])
        coll_scope_names = list(coll_contents.keys())
        assert_equal(coll_scope_names, [coll_output_scope_name])

        contents = [{'scope': content_output_properties['scope'], 'name': content_output_properties['name'],
                     'min_id': content_output_properties['min_id'], 'max_id': content_output_properties['max_id'],
                     'status': ContentStatus.Available, 'path': '/abc/test_path'},
                    {'scope': content_output_properties_name1['scope'], 'name': content_output_properties_name1['name'],
                     'min_id': content_output_properties_name1['min_id'], 'max_id': content_output_properties_name1['max_id'],
                     'status': ContentStatus.Failed}]
        register_output_contents(coll_scope=coll_output_properties['scope'], coll_name=coll_output_properties['name'],
                                 contents=contents, request_id=origin_request_id, workload_id=None)
        content = get_content(content_id=origin_content_output_id_0_1000)
        assert_equal(content['status'], ContentStatus.Available)
        assert_equal(content['path'], '/abc/test_path')
        content = get_content(content_id=content_output_properties_name1_id)
        assert_equal(content['status'], ContentStatus.Failed)

        contents = get_match_contents(coll_scope=coll_output_properties['scope'], coll_name=coll_output_properties['name'],
                                      scope=content_output_properties['scope'], name=content_output_properties['name'],
                                      min_id=None, max_id=None, request_id=origin_request_id,
                                      workload_id=req_properties['workload_id'], only_return_best_match=False)
        assert_equal(len(contents), 3)
        content_ids = [content['content_id'] for content in contents]
        content_ids.sort()
        content_ids1 = [origin_content_output_id_0_1000, origin_content_output_id_0_100, origin_content_output_id_100_200]
        content_ids1.sort()
        assert_equal(content_ids, content_ids1)

        contents = get_match_contents(coll_scope=coll_output_properties['scope'], coll_name=coll_output_properties['name'],
                                      scope=content_output_properties['scope'], name=content_output_properties['name'],
                                      min_id=0, max_id=50, request_id=origin_request_id,
                                      workload_id=req_properties['workload_id'], only_return_best_match=False)
        assert_equal(len(contents), 2)
        content_ids = [content['content_id'] for content in contents]
        content_ids.sort()
        content_ids1 = [origin_content_output_id_0_1000, origin_content_output_id_0_100]
        content_ids1.sort()
        assert_equal(content_ids, content_ids1)

        contents = get_match_contents(coll_scope=coll_output_properties['scope'], coll_name=coll_output_properties['name'],
                                      scope=content_output_properties['scope'], name=content_output_properties['name'],
                                      min_id=0, max_id=50, request_id=origin_request_id,
                                      workload_id=req_properties['workload_id'], only_return_best_match=True)
        assert_equal(len(contents), 1)
        content_ids = [content['content_id'] for content in contents]
        assert_equal(content_ids, [origin_content_output_id_0_100])

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_user_proxy(), "No user proxy to access REST")
    @unittest.skipIf(not check_rest_host(), "REST host is not defined")
    def test_catalog_rest(self):
        """ Catalog (Rest): Test catalog rest functions """
        host = get_rest_host()
        client = Client(host=host)

        req_properties = get_request_properties()
        origin_request_id = add_request(**req_properties)

        trans_properties = get_transform_properties()
        trans_properties['request_id'] = origin_request_id
        origin_trans_id = add_transform(**trans_properties)

        coll_properties = get_collection_properties()
        coll_properties['transform_id'] = origin_trans_id
        coll_properties['relation_type'] = CollectionRelationType.Output
        origin_coll_id = add_collection(**coll_properties)

        req_trans_colls = client.get_collections(request_id=origin_request_id, workload_id=None)
        assert_equal(len(req_trans_colls.keys()), 1)
        req_id = list(req_trans_colls.keys())[0]
        assert_equal(origin_request_id, req_id)
        assert_equal(len(req_trans_colls[req_id].keys()), 1)
        trans_id = list(req_trans_colls[req_id].keys())[0]
        assert_equal(trans_id, origin_trans_id)
        colls = req_trans_colls[req_id][trans_id]
        assert_equal(len(colls), 1)
        assert_equal(colls[0]['coll_id'], origin_coll_id)

        req_trans_colls1 = client.get_collections(request_id=None, workload_id=req_properties['workload_id'])
        assert_equal(is_same_req_trans_colls(req_trans_colls, req_trans_colls1, allow_request_id_None=True), True)

        req_trans_colls1 = client.get_collections(scope=coll_properties['scope'], name=coll_properties['name'],
                                                  request_id=None, workload_id=None)
        assert_equal(is_same_req_trans_colls(req_trans_colls, req_trans_colls1, allow_request_id_None=True), True)

        content_output_properties = get_content_properties()
        content_output_properties['content_type'] = ContentType.File
        content_output_properties['min_id'] = 0
        content_output_properties['max_id'] = 1000
        content_output_properties['coll_id'] = origin_coll_id
        origin_content_output_id_0_1000 = add_content(**content_output_properties)
        content_output_properties_0_100 = copy.deepcopy(content_output_properties)
        content_output_properties_0_100['min_id'] = 0
        content_output_properties_0_100['max_id'] = 100
        content_output_properties['content_type'] = ContentType.Event
        origin_content_output_id_0_100 = add_content(**content_output_properties_0_100)
        content_output_properties_100_200 = copy.deepcopy(content_output_properties)
        content_output_properties_100_200['min_id'] = 100
        content_output_properties_100_200['max_id'] = 200
        content_output_properties['content_type'] = ContentType.Event
        origin_content_output_id_100_200 = add_content(**content_output_properties_100_200)
        content_output_properties_name1 = copy.deepcopy(content_output_properties)
        content_output_properties_name1['name'] = content_output_properties_name1['name'] + '_1'
        content_output_properties_name1_id = add_content(**content_output_properties_name1)

        req_trans_coll_contents = client.get_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'],
                                                      request_id=origin_request_id, workload_id=req_properties['workload_id'])
        coll_contents = req_trans_coll_contents[origin_request_id][origin_trans_id]
        coll_scope_name = '%s:%s' % (coll_properties['scope'], coll_properties['name'])
        coll_scope_names = [scope_name for scope_name in coll_contents]
        assert_equal(coll_scope_names, [coll_scope_name])
        contents = coll_contents[coll_scope_name]['contents']
        assert_equal(len(contents), 4)
        output_content_ids = [output_content['content_id'] for output_content in contents]
        output_content_ids.sort()
        assert_equal(output_content_ids, [origin_content_output_id_0_1000, origin_content_output_id_0_100,
                                          origin_content_output_id_100_200, content_output_properties_name1_id])

        req_trans_coll_contents1 = client.get_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'],
                                                       request_id=origin_request_id, workload_id=req_properties['workload_id'],
                                                       relation_type=CollectionRelationType.Output)
        assert_equal(is_same_req_trans_coll_contents(req_trans_coll_contents, req_trans_coll_contents1), True)

        contents = client.get_match_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'],
                                             scope=content_output_properties['scope'], name=content_output_properties['name'],
                                             min_id=None, max_id=None, request_id=origin_request_id,
                                             workload_id=req_properties['workload_id'], only_return_best_match=False)
        assert_equal(len(contents), 3)
        content_ids = [content['content_id'] for content in contents]
        content_ids.sort()
        content_ids1 = [origin_content_output_id_0_1000, origin_content_output_id_0_100, origin_content_output_id_100_200]
        content_ids1.sort()
        assert_equal(content_ids, content_ids1)

        contents = client.get_match_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'],
                                             scope=content_output_properties['scope'], name=content_output_properties['name'],
                                             min_id=0, max_id=50, request_id=origin_request_id,
                                             workload_id=req_properties['workload_id'], only_return_best_match=False)
        assert_equal(len(contents), 2)
        content_ids = [content['content_id'] for content in contents]
        content_ids.sort()
        content_ids1 = [origin_content_output_id_0_1000, origin_content_output_id_0_100]
        content_ids1.sort()
        assert_equal(content_ids, content_ids1)

        contents = client.get_match_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'],
                                             scope=content_output_properties['scope'], name=content_output_properties['name'],
                                             min_id=0, max_id=50, request_id=origin_request_id,
                                             workload_id=req_properties['workload_id'], only_return_best_match=True)
        assert_equal(len(contents), 1)
        content_ids = [content['content_id'] for content in contents]
        assert_equal(content_ids, [origin_content_output_id_0_100])

        contents = [{'scope': content_output_properties['scope'], 'name': content_output_properties['name'],
                     'min_id': content_output_properties['min_id'], 'max_id': content_output_properties['max_id'],
                     'status': ContentStatus.Available, 'path': '/abc/test_path'},
                    {'scope': content_output_properties_name1['scope'], 'name': content_output_properties_name1['name'],
                     'min_id': content_output_properties_name1['min_id'], 'max_id': content_output_properties_name1['max_id'],
                     'status': ContentStatus.Failed}]
        client.register_contents(coll_scope=coll_properties['scope'], coll_name=coll_properties['name'],
                                 contents=contents, request_id=origin_request_id,
                                 workload_id=req_properties['workload_id'])
        content = get_content(content_id=origin_content_output_id_0_1000)
        assert_equal(content['status'], ContentStatus.Available)
        assert_equal(content['path'], '/abc/test_path')
        content = get_content(content_id=content_output_properties_name1_id)
        assert_equal(content['status'], ContentStatus.Failed)
