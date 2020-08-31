#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


"""
Test client.
"""

import traceback

from rucio.client.client import Client as Rucio_Client
from rucio.common.exception import CannotAuthenticate

from idds.client.client import Client
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request


def get_rucio_client():
    try:
        client = Rucio_Client()
    except CannotAuthenticate as error:
        print(traceback.format_exc())
        raise Exception('%s: %s' % (str(error), traceback.format_exc()))
    return client


def get_rule(scope, name, rucio_client, src_rse, dest_rse, account='ddmadmin'):
    rules = rucio_client.list_did_rules(scope=scope, name=name)
    for rule in rules:
        if rule['source_replica_expression'] == src_rse and rule['rse_expression']:
            print(rule['id'])
            return rule['id']
        # print(rule)
    return None


def create_rule(scope, name, rucio_client, src_rse, dest_rse, account='ddmadmin'):
    did = {'scope': scope, 'name': name}
    rule_id = rucio_client.add_replication_rule(dids=[did],
                                                copies=1,
                                                rse_expression=dest_rse,
                                                source_replica_expression=src_rse,
                                                lifetime=24 * 7 * 3600,
                                                locked=False,
                                                account=account,
                                                grouping='DATASET',
                                                ask_approval=False)
    return rule_id


# max_waiting_time is used for idds to create new rules
def get_req_properties():
    req_properties = {
        'scope': 'data16_13TeV',
        'name': 'data16_13TeV.00298862.physics_Main.daq.RAW',
        'requester': 'panda',
        'request_type': RequestType.StageIn,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': '20776840', 'max_waiting_time': 3600, 'src_rse': 'NDGF-T1_DATATAPE', 'dest_rse': 'NDGF-T1_DATADISK', 'rule_id': '236e4bf87e11490291e3259b14724e30'}
    }
    return req_properties


def pre_check(req):
    rucio_client = get_rucio_client()
    rule_id = get_rule(req['scope'], req['name'], rucio_client, req['request_metadata']['src_rse'], req['request_metadata']['dest_rse'])
    if not rule_id:
        rule_id = create_rule(req['scope'], req['name'], rucio_client, req['request_metadata']['src_rse'], req['request_metadata']['dest_rse'])
    if rule_id:
        print("new rule id: %s" % rule_id)
        req['request_metadata']['rule_id'] = rule_id
    return req


host = get_rest_host()
props = get_req_properties()
# props = get_example_real_tape_stagein_request()
# props = get_example_prodsys2_tape_stagein_request()
# props = get_example_active_learning_request()

props = pre_check(props)
print(props)

client = Client(host=host)

request_id = client.add_request(**props)
print(request_id)
