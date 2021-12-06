#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

from idds.common.constants import RequestType
from idds.common.utils import get_rest_host
from idds.core.requests import get_requests
from idds.workflowv2.workflow import Workflow

from idds.atlas.workflowv2.atlasstageinwork import ATLASStageinWork

from idds.client.clientmanager import ClientManager


def convert_req2reqv2(req):
    # v1: {'created_at': datetime.datetime(2020, 11, 3, 10, 9, 32), 'substatus': None, 'priority': 0, 'transform_tag': '2', 'requester': 'panda', 'request_metadata': {'workload_id': 23083304, 'rule_id': 'bef3da17f17c49ac97863bb9e96af672'}, 'name': 'valid1.361027.Pythia8EvtGen_A14NNPDF23LO_jetjet_JZ7W.simul.HITS.e5112_s3227_tid12560193_00', 'request_id': 3775, 'accessed_at': datetime.datetime(2020, 11, 3, 10, 9, 32), 'updated_at': datetime.datetime(2020, 11, 3, 10, 9, 32), 'locking': <RequestLocking.Idle: 0>, 'status': <RequestStatus.Cancelled: 9>, 'workload_id': 23083304, 'request_type': <RequestType.StageIn: 2>, 'errors': None, 'processing_metadata': None, 'scope': 'valid1', 'expired_at': datetime.datetime(2020, 12, 3, 10, 9, 32), 'next_poll_at': datetime.datetime(2020, 11, 3, 10, 9, 32)}  # noqa E501

    if req['request_type'] == RequestType.StageIn:
        request_metadata = req['request_metadata']
        work = ATLASStageinWork(executable=None, arguments=None, parameters=None, setup=None,
                                exec_type='local', sandbox=None,
                                primary_input_collection={'scope': req['scope'], 'name': req['name']},
                                other_input_collections=None,
                                output_collections={'scope': req['scope'], 'name': req['name'] + '.idds.stagein'},
                                log_collections=None,
                                logger=None,
                                max_waiting_time=request_metadata.get('max_waiting_time', 3600 * 7 * 24),
                                src_rse=request_metadata.get('src_rse', None),
                                dest_rse=request_metadata.get('dest_rse', None),
                                rule_id=request_metadata.get('rule_id', None))
    if req['request_type'] == RequestType.Workflow:
        pass
        ori_workflow = req['request_metadata']['workflow']
        ori_work = ori_workflow.works[ori_workflow.primary_initial_work]
        input_coll = ori_work.collections[ori_work.primary_input_collection]
        work = ATLASStageinWork(executable=None, arguments=None, parameters=None, setup=None,
                                exec_type='local', sandbox=None,
                                primary_input_collection={'scope': input_coll['scope'], 'name': input_coll['name']},
                                other_input_collections=None,
                                output_collections={'scope': input_coll['scope'], 'name': input_coll['name'] + '.idds.stagein'},
                                log_collections=None,
                                logger=None,
                                max_waiting_time=ori_work.max_waiting_time,
                                src_rse=ori_work.src_rse,
                                dest_rse=ori_work.dest_rse,
                                rule_id=ori_work.rule_id)

    workload_id = req['workload_id']
    if not workload_id and 'workload_id' in request_metadata:
        workload_id = request_metadata['workload_id']

    wf = Workflow()
    wf.set_workload_id(workload_id)
    wf.add_work(work)

    host = get_rest_host()
    wm = ClientManager(host=host)
    request_id = wm.submit(wf)
    # print(request_id)
    return request_id


reqs = get_requests()
print(len(reqs))
for req in reqs:
    # if req['request_id'] in [3743, 3755, 3769, 3775]:
    # if req['request_id'] in [3787, 3789, 3791]:
    if req['request_id'] in [11673, 17615, 19289, 19397]:
        # print(req)
        # print(req['request_metadata']['workflow'].to_dict())
        new_req_id = convert_req2reqv2(req)
        print("convert old request %s to new request %s" % (req['request_id'], new_req_id))
    # print(req['request_metadata']['workflow'].to_dict())
    pass
