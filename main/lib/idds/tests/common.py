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

import random
import time
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
        'workload_id': int(time.time()) + random.randint(0, 1000000),
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id1': int(time.time())}
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
        'transform_id': None,
        'relation_type': CollectionRelationType.Input,
        'bytes': 0,
        'status': CollectionStatus.New,
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
        'bytes': 1,
        'md5': None,
        'adler32': None,
        'processing_id': None,
        'storage_id': None,
        'retries': 0,
        'path': None,
        'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
        'content_metadata': {'id': 123}
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


def get_example_tape_stagein_request():
    req_properties = {
        'scope': 'test_scope',
        'name': 'test_name_%s' % str(uuid()),
        'requester': 'panda',
        'request_type': RequestType.StageIn,
        'transform_tag': RequestType.StageIn.value,
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': int(time.time()), 'src_rse': 'BNL-OSG2_DATATAPE', 'dest_rse': 'BNL-OSG2_DATADISK'}
    }
    return req_properties


def get_example_real_tape_stagein_request():
    req_properties = {
        'scope': 'data17_13TeV',
        'name': 'data17_13TeV.00341649.express_express.merge.RAW',
        'requester': 'panda',
        'request_type': RequestType.StageIn,
        'transform_tag': RequestType.StageIn.value,
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': int(time.time()), 'src_rse': 'BNL-OSG2_DATATAPE', 'dest_rse': 'BNL-OSG2_DATADISK'}
    }
    return req_properties


def get_example_prodsys2_tape_stagein_request_old():
    req_properties = {
        'scope': 'data16_13TeV',
        'name': 'data16_13TeV.00298773.physics_Main.daq.RAW',
        'requester': 'panda',
        'request_type': RequestType.StageIn,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': '20776834', 'src_rse': 'INFN-T1_DATATAPE', 'rule_id': 'ececf892223142b1b7c25b7593f50fe1'}
    }
    return req_properties


def get_example_prodsys2_tape_stagein_request():
    req_properties = {
        'scope': 'data16_13TeV',
        'name': 'data16_13TeV.00301915.physics_Main.daq.RAW',
        'requester': 'panda',
        'request_type': RequestType.StageIn,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': '20776936', 'src_rse': 'NDGF-T1_DATATAPE', 'rule_id': '1d522d631327478db5773b554f65b0ff'}
    }
    return req_properties


def get_example_active_learning_request():
    req_properties = {
        'scope': 'data15_13TeV',
        'name': 'data15_13TeV.00270949.physics_Main.merge.AOD.r7600_p2521_tid07734829_00',
        'requester': 'panda',
        'request_type': RequestType.ActiveLearning,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': '20525134', 'sandbox': 'https://', 'executable': 'hostname', 'arguments': '-s'}
    }
    return req_properties


def get_example_ess_request():
    req_properties = {
        'scope': 'test_scope',
        'name': 'test_name_%s' % str(uuid()),
        'requester': 'panda',
        'request_type': RequestType.EventStreaming,
        'transform_tag': 's3128',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': int(time.time()), 'events_per_range': 10}
    }
    return req_properties


# https://bigpanda.cern.ch/tasknew/19163542/
def get_real_example_ess_request():
    req_properties = {
        'scope': 'mc16_13TeV',
        'name': 'mc16_13TeV.451129.MGPy8EG_A14N23LO_VBF_RS_G_ZZ_llqq_kt1_m1500.merge.EVNT.e7758_e5984_tid19153490_00',
        'requester': 'panda',
        'request_type': RequestType.EventStreaming,
        'transform_tag': 's3126',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': 19163542, 'events_per_range': 10}
    }
    return req_properties


# https://bigpanda.cern.ch/task/19660169/
def get_example_derivation_request():
    req_properties = {
        'scope': 'mc16_13TeV',
        'name': 'mc16_13TeV.410287.PhPy8EG_A14_ttbar_hdamp258p75_allhad_mtt_1700_2000.merge.AOD.e6686_e5984_s3126_r10724_r10726_tid15801996_00',
        'requester': 'panda',
        'request_type': RequestType.Derivation,
        'transform_tag': 'p3978',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': 19660169, 'task_parameter': None}
    }
    return req_properties


def merge_dicts(list_dicts):
    ret = {}
    for a_dict in list_dicts:
        ret.update(a_dict)
    return ret


def is_same_req_trans_colls(req_trans_colls1, req_trans_colls2, allow_request_id_None=False):
    req_ids1 = list(req_trans_colls1.keys())
    req_ids2 = list(req_trans_colls2.keys())
    if not allow_request_id_None and not (req_ids1 == req_ids2):
        return False

    if not allow_request_id_None:
        for req_id in req_ids1:
            tran_ids1 = list(req_trans_colls1[req_id].keys())
            tran_ids2 = list(req_trans_colls2[req_id].keys())
            if not tran_ids1 == tran_ids2:
                return False

            for tran_id in tran_ids1:
                colls1 = req_trans_colls1[req_id][tran_id]
                colls2 = req_trans_colls2[req_id][tran_id]
                coll_ids1 = [coll['coll_id'] for coll in colls1]
                coll_ids2 = [coll['coll_id'] for coll in colls2]
                coll_ids1.sort()
                coll_ids2.sort()
                if not coll_ids1 == coll_ids2:
                    return False
    else:
        trans1 = merge_dicts(req_trans_colls1.values())
        trans2 = merge_dicts(req_trans_colls2.values())
        tran_ids1 = list(trans1.keys())
        tran_ids2 = list(trans2.keys())
        if not tran_ids1 == tran_ids2:
            return False

        for tran_id in tran_ids1:
            colls1 = trans1[tran_id]
            colls2 = trans2[tran_id]
            coll_ids1 = [coll['coll_id'] for coll in colls1]
            coll_ids2 = [coll['coll_id'] for coll in colls2]
            coll_ids1.sort()
            coll_ids2.sort()
            if not coll_ids1 == coll_ids2:
                return False
    return True


def is_same_req_trans_coll_contents(req_trans_colls1, req_trans_colls2, allow_request_id_None=True):
    req_ids1 = list(req_trans_colls1.keys())
    req_ids2 = list(req_trans_colls2.keys())
    if not allow_request_id_None and not (req_ids1 == req_ids2):
        return False

    if not allow_request_id_None:
        for req_id in req_ids1:
            tran_ids1 = list(req_trans_colls1[req_id].keys())
            tran_ids2 = list(req_trans_colls2[req_id].keys())
            if not tran_ids1 == tran_ids2:
                return False

            for tran_id in tran_ids1:
                coll_contents1 = req_trans_colls1[req_id][tran_id]
                coll_contents2 = req_trans_colls2[req_id][tran_id]
                coll_scope_names1 = [scope_name for scope_name in coll_contents1]
                coll_scope_names2 = [scope_name for scope_name in coll_contents2]
                coll_scope_names1.sort()
                coll_scope_names2.sort()
                if not coll_scope_names1 == coll_scope_names1:
                    return False

                for scope_name in coll_scope_names1:
                    contents1 = req_trans_colls1[req_id][tran_id][scope_name]['contents']
                    contents2 = req_trans_colls2[req_id][tran_id][scope_name]['contents']
                    content_ids1 = [content['content_id'] for content in contents1]
                    content_ids2 = [content['content_id'] for content in contents2]
                    if not content_ids1 == content_ids2:
                        return False
    else:
        if True:
            trans1 = merge_dicts(req_trans_colls1.values())
            trans2 = merge_dicts(req_trans_colls2.values())
            tran_ids1 = list(trans1.keys())
            tran_ids2 = list(trans2.keys())
            if not tran_ids1 == tran_ids2:
                return False

            for tran_id in tran_ids1:
                coll_contents1 = trans1[tran_id]
                coll_contents2 = trans2[tran_id]
                coll_scope_names1 = [scope_name for scope_name in coll_contents1]
                coll_scope_names2 = [scope_name for scope_name in coll_contents2]
                coll_scope_names1.sort()
                coll_scope_names2.sort()
                if not coll_scope_names1 == coll_scope_names1:
                    return False

                for scope_name in coll_scope_names1:
                    contents1 = trans1[tran_id][scope_name]['contents']
                    contents2 = trans2[tran_id][scope_name]['contents']
                    content_ids1 = [content['content_id'] for content in contents1]
                    content_ids2 = [content['content_id'] for content in contents2]
                    if not content_ids1 == content_ids2:
                        return False
    return True
