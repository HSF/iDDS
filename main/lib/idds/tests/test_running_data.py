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

# import traceback

# from rucio.client.client import Client as Rucio_Client
# from rucio.common.exception import CannotAuthenticate

from idds.common.utils import json_dumps                 # noqa F401
from idds.common.constants import ContentStatus, RequestType, RequestStatus          # noqa F401
from idds.core.requests import get_requests, add_request              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401

# from idds.client.client import Client
from idds.client.clientmanager import ClientManager                           # noqa F401
# from idds.common.constants import RequestType, RequestStatus                # noqa F401
from idds.common.utils import get_rest_host                                   # noqa F401
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request

# from idds.workflow.work import Work, Parameter, WorkStatus
# from idds.workflow.workflow import Condition, Workflow
from idds.workflow.workflow import Workflow
# from idds.atlas.workflow.atlasstageinwork import ATLASStageinWork
from idds.doma.workflow.domapandawork import DomaPanDAWork


task_queue = 'DOMA_LSST_GOOGLE_TEST'


def randStr(chars=string.ascii_lowercase + string.digits, N=10):
    return ''.join(random.choice(chars) for _ in range(N))


class PanDATask(object):
    name = None
    step = None
    dependencies = []


def get_workflow():

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
                          log_collections=[], dependency_map=taskN1.dependencies, task_name=taskN1.name, task_queue=task_queue)
    work2 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#2'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#2'}],
                          log_collections=[], dependency_map=taskN2.dependencies, task_name=taskN2.name, task_queue=task_queue)
    work3 = DomaPanDAWork(executable='echo',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#3'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#3'}],
                          log_collections=[], dependency_map=taskN3.dependencies, task_name=taskN3.name, task_queue=task_queue)

    workflow = Workflow()
    workflow.add_work(work1)
    workflow.add_work(work2)
    workflow.add_work(work3)
    return workflow


def get_workflow_props(workflow):
    props = {
        'scope': 'workflow',
        'name': workflow.name,
        'requester': 'panda',
        'request_type': RequestType.Workflow,
        'transform_tag': 'workflow',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'workload_id': workflow.get_workload_id(),
        'request_metadata': {'workload_id': workflow.get_workload_id(), 'workflow': workflow}
    }
    # workflow.add_proxy()
    primary_init_work = workflow.get_primary_initial_collection()
    if primary_init_work:
        props['scope'] = primary_init_work['scope']
        props['name'] = primary_init_work['name']
    return props


def test_running_data():
    workflow = get_workflow()
    props = get_workflow_props(workflow)
    request_id = add_request(**props)
    return request_id


def show_request(req):
    workflow = req['processing_metadata']['workflow']
    print(workflow.independent_works)
    print(len(workflow.independent_works))
    print(workflow.works_template.keys())
    print(len(workflow.works_template.keys()))
    print(workflow.work_sequence.keys())
    print(len(workflow.work_sequence.keys()))
    print(workflow.works.keys())
    print(len(workflow.works.keys()))

    work_ids = []
    for i_id in workflow.works:
        work = workflow.works[i_id]
        print(i_id)
        print(work.work_name)
        print(work.task_name)
        print(work.work_id)
        work_ids.append(work.work_id)
    print(work_ids)


def test_get_requests(request_id):
    reqs = get_requests(request_id=request_id, with_detail=False)
    for req in reqs:
        # show_request(req)
        print(req)
        print(json_dumps(req, sort_keys=True, indent=4))
        pass


if __name__ == '__main__':
    request_id = test_running_data()
    print(request_id)
    test_get_requests(request_id=3)
