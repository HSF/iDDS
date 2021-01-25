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

# from idds.client.client import Client
from idds.client.clientmanager import ClientManager
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request

# from idds.workflow.work import Work, Parameter, WorkStatus
# from idds.workflow.workflow import Condition, Workflow
from idds.workflow.workflow import Workflow
from idds.atlas.workflow.atlasstageinwork import ATLASStageinWork


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


def get_rule_id(scope, name, src_rse, dest_rse):
    rucio_client = get_rucio_client()
    rule_id = get_rule(scope, name, rucio_client, src_rse, dest_rse)
    if not rule_id:
        rule_id = create_rule(scope, name, rucio_client, src_rse, dest_rse)
    if rule_id:
        print("new rule id: %s" % rule_id)
    return rule_id


def get_workflow():
    scope = 'data16_13TeV'
    name = 'data16_13TeV.00298862.physics_Main.daq.RAW'
    src_rse = 'NDGF-T1_DATATAPE'
    dest_rse = 'NDGF-T1_DATADISK'
    rule_id = get_rule_id(scope, name, src_rse, dest_rse)
    work = ATLASStageinWork(executable=None, arguments=None, parameters=None, setup=None,
                            exec_type='local', sandbox=None, work_id=None,
                            primary_input_collection={'scope': scope, 'name': name},
                            other_input_collections=None,
                            output_collections={'scope': scope, 'name': name + '.idds.stagein'},
                            log_collections=None,
                            logger=None,
                            max_waiting_time=3600 * 7 * 24, src_rse=src_rse, dest_rse=dest_rse, rule_id=rule_id)
    wf = Workflow()
    wf.add_work(work)
    # work.set_workflow(wf)
    return wf


def pre_check(req):
    rucio_client = get_rucio_client()
    rule_id = get_rule(req['scope'], req['name'], rucio_client, req['request_metadata']['src_rse'], req['request_metadata']['dest_rse'])
    if not rule_id:
        rule_id = create_rule(req['scope'], req['name'], rucio_client, req['request_metadata']['src_rse'], req['request_metadata']['dest_rse'])
    if rule_id:
        print("new rule id: %s" % rule_id)
        req['request_metadata']['rule_id'] = rule_id
    return req


if __name__ == '__main__':
    host = get_rest_host()
    # props = get_req_properties()
    # props = get_example_real_tape_stagein_request()
    # props = get_example_prodsys2_tape_stagein_request()
    # props = get_example_active_learning_request()
    workflow = get_workflow()

    # props = pre_check(props)
    # print(props)

    wm = ClientManager(host=host)
    request_id = wm.submit(workflow)
    print(request_id)
