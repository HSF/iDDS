#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023


"""
Test client.
"""

import sys
import string
import random
import time

# import traceback

# from rucio.client.client import Client as Rucio_Client
# from rucio.common.exception import CannotAuthenticate

# from idds.client.client import Client
from idds.client.clientmanager import ClientManager
# from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request

# from idds.workflowv2.work import Work, Parameter, WorkStatus
# from idds.workflowv2.workflow import Condition, Workflow
from idds.workflowv2.workflow import Workflow
# from idds.atlas.workflowv2.atlasstageinwork import ATLASStageinWork
from idds.doma.workflowv2.domapandawork import DomaPanDAWork


if len(sys.argv) > 1 and sys.argv[1] == "in2p3":
    site = 'in2p3'
    task_cloud = 'EU'
    # task_queue = 'CC-IN2P3_TEST'
    task_queue = 'CC-IN2P3_Rubin'
    task_queue1 = 'CC-IN2P3_Rubin_Medium'
    task_queue2 = 'CC-IN2P3_Rubin_Himem'
    task_queue3 = 'CC-IN2P3_Rubin_Extra_Himem'
    task_queue4 = 'CC-IN2P3_Rubin_Merge'
elif len(sys.argv) > 1 and sys.argv[1] == "lancs":
    site = 'lancs'
    task_cloud = 'EU'
    # task_queue = 'LANCS_TEST'
    task_queue = 'LANCS_Rubin'
    task_queue1 = 'LANCS_Rubin_Medium'
    task_queue2 = 'LANCS_Rubin_Himem'
    task_queue3 = 'LANCS_Rubin_Extra_Himem'
    task_queue3 = 'LANCS_Rubin_Himem'
    task_queue4 = 'LANCS_Rubin_Merge'
else:
    site = 'slac'
    # task_cloud = 'LSST'
    task_cloud = 'US'

    task_queue = 'DOMA_LSST_GOOGLE_TEST'
    # task_queue = 'DOMA_LSST_GOOGLE_MERGE'
    # task_queue = 'SLAC_TEST'
    # task_queue = 'DOMA_LSST_SLAC_TEST'
    task_queue = 'SLAC_Rubin'
    task_queue1 = 'SLAC_Rubin_Medium'
    task_queue2 = 'SLAC_Rubin_Himem'
    task_queue3 = 'SLAC_Rubin_Extra_Himem'
    task_queue4 = 'SLAC_Rubin_Merge'
    # task_queue = 'SLAC_Rubin_Extra_Himem_32Cores'
    # task_queue = 'SLAC_Rubin_Merge'
    # task_queue = 'SLAC_TEST'

# task_cloud = None


def randStr(chars=string.ascii_lowercase + string.digits, N=10):
    return ''.join(random.choice(chars) for _ in range(N))


class PanDATask(object):
    name = None
    step = None
    dependencies = []


def setup_workflow():

    taskN1 = PanDATask()
    taskN1.step = "step1"
    taskN1.name = site + "_" + taskN1.step + "_" + randStr()
    taskN1.dependencies = [
        {"name": "00000" + str(k),
         "dependencies": [],
         "submitted": False} for k in range(6)
    ]

    taskN2 = PanDATask()
    taskN2.step = "step2"
    taskN2.name = site + "_" + taskN2.step + "_" + randStr()
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
    taskN3.name = site + "_" + taskN3.step + "_" + randStr()
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

    taskN4 = PanDATask()
    taskN4.step = "step4"
    taskN4.name = site + "_" + taskN4.step + "_" + randStr()
    taskN4.dependencies = [
        {"name": "00004" + str(k),
         "dependencies": [],
         "submitted": False} for k in range(6)
    ]

    taskN5 = PanDATask()
    taskN5.step = "step5"
    taskN5.name = site + "_" + taskN5.step + "_" + randStr()
    taskN5.dependencies = [
        {"name": "00005" + str(k),
         "dependencies": [],
         "submitted": False} for k in range(6)
    ]

    work1 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=taskN1.dependencies,
                          task_name=taskN1.name, task_queue=task_queue,
                          encode_command_line=True,
                          task_priority=981,
                          task_rss=3, task_rss_retry_offset=3, task_rss_retry_step=1000, task_rss_max=8000,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)
    work2 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#2'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#2'}],
                          log_collections=[], dependency_map=taskN2.dependencies,
                          task_name=taskN2.name, task_queue=task_queue1,
                          encode_command_line=True,
                          task_priority=881,
                          task_rss=3, task_rss_retry_offset=3, task_rss_retry_step=1000, task_rss_max=8000,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)
    work3 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#3'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#3'}],
                          log_collections=[], dependency_map=taskN3.dependencies,
                          task_name=taskN3.name, task_queue=task_queue2,
                          encode_command_line=True,
                          task_priority=781,
                          task_rss=3, task_rss_retry_offset=3, task_rss_retry_step=1000, task_rss_max=8000,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)

    work4 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=taskN4.dependencies,
                          task_name=taskN4.name, task_queue=task_queue3,
                          encode_command_line=True,
                          task_priority=981,
                          task_rss=3, task_rss_retry_offset=3, task_rss_retry_step=1000, task_rss_max=8000,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)

    work5 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=taskN5.dependencies,
                          task_name=taskN5.name, task_queue=task_queue4,
                          encode_command_line=True,
                          task_priority=981,
                          task_rss=3, task_rss_retry_offset=3, task_rss_retry_step=1000, task_rss_max=8000,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)

    pending_time = 12
    # pending_time = None
    workflow = Workflow(pending_time=pending_time)
    workflow.add_work(work1)
    workflow.add_work(work2)
    workflow.add_work(work3)
    workflow.add_work(work4)
    workflow.add_work(work5)
    workflow.name = site + "_" + 'test_workflow.idds.%s.test' % time.time()
    return workflow


if __name__ == '__main__':
    host = get_rest_host()
    workflow = setup_workflow()

    wm = ClientManager(host=host)
    # wm.set_original_user(user_name="wguandev")
    request_id = wm.submit(workflow, use_dataset_name=False)
    print(request_id)
