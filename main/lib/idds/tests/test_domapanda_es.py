#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Sergey Padolski, <spadolski@bnl.gov>, 2021
# - Wen Guan, <wen.guan@cern.ch>, 2023


"""
Test client.
"""

import json   # noqa F401
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
    task_queue = 'SLAC_TEST'
    task_queue4 = task_queue3 = task_queue2 = task_queue1 = task_queue

# task_cloud = None


def randStr(chars=string.ascii_lowercase + string.digits, N=10):
    return ''.join(random.choice(chars) for _ in range(N))


class PanDATask(object):
    name = None
    step = None
    dependencies = []


def setup_workflow():

    es_map = {}
    taskN1 = PanDATask()
    taskN1.step = "step1"
    taskN1.name = site + "_" + taskN1.step + "_" + randStr()
    taskN1.dependencies = [
        {"name": "00000" + str(k),
         "order_id": k,
         "dependencies": [],
         "submitted": False} for k in range(6)
    ]

    es_map[taskN1.step] = {str(item["order_id"]): item["name"] for item in taskN1.dependencies}

    taskN2 = PanDATask()
    taskN2.step = "step2"
    taskN2.name = site + "_" + taskN2.step + "_" + randStr()
    taskN2.dependencies = [
        {
            "name": "000010",
            "order_id": 0,
            "dependencies": [{"task": taskN1.name, "inputname": "000001", "available": False},
                             {"task": taskN1.name, "inputname": "000002", "available": False}],
            "submitted": False
        },
        {
            "name": "000011",
            "order_id": 1,
            "dependencies": [{"task": taskN1.name, "inputname": "000001", "available": False},
                             {"task": taskN1.name, "inputname": "000002", "available": False}],
            "submitted": False
        },
        {
            "name": "000012",
            "order_id": 2,
            "dependencies": [{"task": taskN1.name, "inputname": "000001", "available": False},
                             {"task": taskN1.name, "inputname": "000002", "available": False}],
            "submitted": False
        }
    ]

    es_map[taskN2.step] = {str(item["order_id"]): item["name"] for item in taskN2.dependencies}

    taskN3 = PanDATask()
    taskN3.step = "step3"
    taskN3.name = site + "_" + taskN3.step + "_" + randStr()
    taskN3.dependencies = [
        {
            "name": "000020",
            "order_id": 0,
            "dependencies": [],
            "submitted": False
        },
        {
            "name": "000021",
            "order_id": 1,
            "dependencies": [{"task": taskN2.name, "inputname": "000010", "available": False},
                             {"task": taskN2.name, "inputname": "000011", "available": False}],
            "submitted": False
        },
        {
            "name": "000022",
            "order_id": 2,
            "dependencies": [{"task": taskN2.name, "inputname": "000011", "available": False},
                             {"task": taskN2.name, "inputname": "000012", "available": False}],
            "submitted": False
        },
        {
            "name": "000023",
            "order_id": 3,
            "dependencies": [],
            "submitted": False
        },
        {
            "name": "000024",
            "order_id": 4,
            "groups": taskN3.name,
            "dependencies": [{"task": taskN3.name, "inputname": "000021", "available": False},
                             {"task": taskN3.name, "inputname": "000023", "available": False}],
            "submitted": False
        },
    ]

    es_map[taskN3.step] = {str(item["order_id"]): item["name"] for item in taskN3.dependencies}

    taskN4 = PanDATask()
    taskN4.step = "step4"
    taskN4.name = site + "_" + taskN4.step + "_" + randStr()
    taskN4.dependencies = [
        {"name": "00004" + str(k),
         "order_id": k,
         "dependencies": [],
         "submitted": False} for k in range(6)
    ]

    es_map[taskN4.step] = {str(item["order_id"]): item["name"] for item in taskN4.dependencies}

    taskN5 = PanDATask()
    taskN5.step = "step5"
    taskN5.name = site + "_" + taskN5.step + "_" + randStr()
    taskN5.dependencies = [
        {"name": "00005" + str(k),
         "order_id": k,
         "dependencies": [],
         "submitted": False} for k in range(6)
    ]

    es_map[taskN5.step] = {str(item["order_id"]): item["name"] for item in taskN5.dependencies}

    # print(json.dumps(es_map))
    # raise
    # executable = "wget https://wguan-wisc.web.cern.ch/wguan-wisc/doma_es_executor.py; chmod +x doma_es_executor.py; ./doma_es_executor.py echo ${IN/L}"
    # executable = "export RUBIN_ES_CORES=4; echo; RUBIN_ES_MAP=%s; echo ${IN/L}" % json.dumps(es_map)

    es_map_file = "/sdf/data/rubin/panda_jobs/panda_env_pilot/test_rubin_es_map.json"
    executable = "export RUBIN_ES_CORES=4; echo; RUBIN_ES_MAP_FILE=%s; echo ${IN/L}" % es_map_file

    work1 = DomaPanDAWork(executable=executable,
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=taskN1.dependencies,
                          task_name=taskN1.name, task_queue=task_queue,
                          encode_command_line=True,
                          task_priority=981,
                          es=True,
                          es_label=taskN1.step,
                          max_events_per_job=100,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)
    work2 = DomaPanDAWork(executable=executable,
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#2'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#2'}],
                          log_collections=[], dependency_map=taskN2.dependencies,
                          task_name=taskN2.name, task_queue=task_queue1,
                          encode_command_line=True,
                          task_priority=881,
                          es=True,
                          es_label=taskN2.step,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)
    work3 = DomaPanDAWork(executable=executable,
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#3'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#3'}],
                          log_collections=[], dependency_map=taskN3.dependencies,
                          task_name=taskN3.name, task_queue=task_queue2,
                          encode_command_line=True,
                          task_priority=781,
                          es=True,
                          es_label=taskN3.step,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)

    work4 = DomaPanDAWork(executable=executable,
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=taskN4.dependencies,
                          task_name=taskN4.name, task_queue=task_queue3,
                          encode_command_line=True,
                          task_priority=981,
                          es=True,
                          es_label=taskN4.step,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)

    work5 = DomaPanDAWork(executable=executable,
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=taskN5.dependencies,
                          task_name=taskN5.name, task_queue=task_queue4,
                          encode_command_line=True,
                          task_priority=981,
                          es=True,
                          es_label=taskN5.step,
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
