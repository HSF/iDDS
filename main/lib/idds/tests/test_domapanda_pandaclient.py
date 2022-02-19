#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Sergey Padolski, <spadolski@bnl.gov>, 2021
# - Wen Guan, <wen.guan@cern.ch>, 2021


"""
Test client.
"""

import string
import random
import time

# import traceback

# from rucio.client.client import Client as Rucio_Client
# from rucio.common.exception import CannotAuthenticate

# from idds.client.client import Client
# from idds.client.clientmanager import ClientManager
# from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request

# from idds.workflowv2.work import Work, Parameter, WorkStatus
# from idds.workflowv2.workflow import Condition, Workflow
from idds.workflowv2.workflow import Workflow
# from idds.atlas.workflowv2.atlasstageinwork import ATLASStageinWork
from idds.doma.workflowv2.domapandawork import DomaPanDAWork

import idds.common.utils as idds_utils
import pandaclient.idds_api


task_queue = 'DOMA_LSST_GOOGLE_TEST'
task_queue = 'DOMA_LSST_GOOGLE_MERGE'


def randStr(chars=string.ascii_lowercase + string.digits, N=10):
    return ''.join(random.choice(chars) for _ in range(N))


class PanDATask(object):
    name = None
    step = None
    dependencies = []


def setup_workflow():

    taskN1 = PanDATask()
    taskN1.step = "step1"
    taskN1.name = taskN1.step + "_" + randStr()
    taskN1.dependencies = [
        {"name": "00000" + str(k),
         "dependencies": [],
         "submitted": False} for k in range(6)
    ]

    taskN2 = PanDATask()
    taskN2.step = "step2"
    taskN2.name = taskN2.step + "_" + randStr()
    taskN2.dependencies = [
        {
            "name": "000010",
            "dependencies": [{"task": taskN1.name, "inputname": "000001", "available": False},
                             {"task": taskN1.name, "inputname": "000002", "available": False}],
            "submitted": False
        },
        {
            "name": "000011",
            "dependencies": [{"task": taskN1.name, "inputname": "000001", "available": False},
                             {"task": taskN1.name, "inputname": "000002", "available": False}],
            "submitted": False
        },
        {
            "name": "000012",
            "dependencies": [{"task": taskN1.name, "inputname": "000001", "available": False},
                             {"task": taskN1.name, "inputname": "000002", "available": False}],
            "submitted": False
        }
    ]

    taskN3 = PanDATask()
    taskN3.step = "step3"
    taskN3.name = taskN3.step + "_" + randStr()
    taskN3.dependencies = [
        {
            "name": "000020",
            "dependencies": [],
            "submitted": False
        },
        {
            "name": "000021",
            "dependencies": [{"task": taskN2.name, "inputname": "000010", "available": False},
                             {"task": taskN2.name, "inputname": "000011", "available": False}],
            "submitted": False
        },
        {
            "name": "000022",
            "dependencies": [{"task": taskN2.name, "inputname": "000011", "available": False},
                             {"task": taskN2.name, "inputname": "000012", "available": False}],
            "submitted": False
        },
        {
            "name": "000023",
            "dependencies": [],
            "submitted": False
        },
        {
            "name": "000024",
            "dependencies": [{"task": taskN3.name, "inputname": "000021", "available": False},
                             {"task": taskN3.name, "inputname": "000023", "available": False}],
            "submitted": False
        },
    ]

    work1 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=taskN1.dependencies,
                          task_name=taskN1.name, task_queue=task_queue,
                          encode_command_line=True,
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud='LSST')
    work2 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#2'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#2'}],
                          log_collections=[], dependency_map=taskN2.dependencies,
                          task_name=taskN2.name, task_queue=task_queue,
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          encode_command_line=True,
                          task_cloud='LSST')
    work3 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#3'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#3'}],
                          log_collections=[], dependency_map=taskN3.dependencies,
                          task_name=taskN3.name, task_queue=task_queue,
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud='LSST',
                          encode_command_line=True)

    pending_time = 12
    # pending_time = None
    workflow = Workflow(pending_time=pending_time)
    workflow.add_work(work1)
    workflow.add_work(work2)
    workflow.add_work(work3)
    workflow.name = 'test_workflow.idds.%s' % time.time()
    return workflow


def submit(workflow, idds_server):

    c = pandaclient.idds_api.get_api(idds_utils.json_dumps,
                                     idds_host=idds_server, compress=True, manager=True)
    request_id = c.submit(workflow, username=None, use_dataset_name=False)
    print("Submitted into iDDs with request id=%s", str(request_id))


if __name__ == '__main__':
    host = get_rest_host()
    workflow = setup_workflow()

    # wm = ClientManager(host=host)
    # request_id = wm.submit(workflow)
    # print(request_id)
    submit(workflow, host)
