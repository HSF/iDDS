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

# import traceback

# from rucio.client.client import Client as Rucio_Client
# from rucio.common.exception import CannotAuthenticate

# from idds.client.client import Client
from idds.client.workflowmanager import WorkflowManager
# from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request

# from idds.workflow.work import Work, Parameter, WorkStatus
# from idds.workflow.workflow import Condition, Workflow
from idds.workflow.workflow import Condition, Workflow
# from idds.atlas.workflow.atlasstageinwork import ATLASStageinWork
from idds.doma.workflow.domalsstwork import DomaLSSTWork
import string, random

def randStr(chars=string.ascii_lowercase + string.digits, N=10):
    return ''.join(random.choice(chars) for _ in range(N))


def setup_workflow():

    task1genid = randStr()
    task2genid = randStr()
    task3genid = randStr()

    dependencymap1 = {"taskname": "init_"+task1genid, "quantum_map":[("999999",None)]}
    dependencymap2 = {"taskname": "step1_"+task2genid, "quantum_map": [("000000",[task1genid + "/" + "999999"]),("000001", [task1genid + "/" + "999999"]),("000002", [task1genid + "/" + "999999"])]}

    work1 = DomaLSSTWork(executable='echo',
                         parameters=dependencymap1,
                         primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                         output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                         log_collections=[])
    # work2 = DomaLSSTWork(executable='echo',
    #                      arguments=dependencymap2,
    #                      primary_input_collection={'scope': 'lsst.test', 'name': 'pseudo_input_collection#2'},
    #                      output_collections=[{'scope': 'lsst.test', 'name': 'pseudo_output_collection#2'}],
    #                      log_collections=[])

    # work3 = DomaLSSTWork(executable='echo',
    #                      arguments=None,
    #                      primary_input_collection={'scope': 'lsst.test', 'name': 'pseudo_input_collection#2'},
    #                      output_collections=[{'scope': 'lsst.test', 'name': 'pseudo_output_collection#2'}],
    #                      log_collections=[])


    workflow = Workflow()
    workflow.add_work(work1)
    #workflow.add_work(work2)

    #cond = Condition(cond=work1.my_condition, current_work=work1, true_work=work2)
    #workflow.add_condition(cond)


    #Unit tests

    """
    *** Function called by Transformer agent.
    """
    # ret1 = work1.get_input_collections()
    # ret2 = work1.get_new_input_output_maps()
    # ret3 = work1.get_processing(None)
    # ret4 = work1.create_processing(ret2)
    # work1.submit_processing(ret4)
    # ret6 = work1.poll_processing_updates(ret4, ret2)


    return workflow


if __name__ == '__main__':
    host = get_rest_host()
    workflow = setup_workflow()

    wm = WorkflowManager(host=host)
    request_id = wm.submit(workflow)
    print(request_id)
