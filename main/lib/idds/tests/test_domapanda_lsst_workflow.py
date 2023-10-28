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
Test lsst generic workflow.
"""

import json
import logging
import sys                                                    # noqa E402 F401

logging.basicConfig(level=logging.DEBUG)

# import traceback

from collections import Counter                                # noqa #402
from lsst.ctrl.bps import generic_workflow as gw               # noqa #402

# from rucio.client.client import Client as Rucio_Client
# from rucio.common.exception import CannotAuthenticate

# from idds.client.client import Client
from idds.client.clientmanager import ClientManager   # noqa E402
# from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host                               # noqa E402
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request

# from idds.workflowv2.work import Work, Parameter, WorkStatus
# from idds.workflowv2.workflow import Condition, Workflow
from idds.workflowv2.workflow import Workflow, Condition     # noqa E402
# from idds.atlas.workflowv2.atlasstageinwork import ATLASStageinWork
from idds.doma.workflowv2.domapandawork import DomaPanDAWork       # noqa E402
from idds.doma.workflowv2.domapandaeswork import DomaPanDAESWork   # noqa E402
from idds.doma.workflowv2.domatree import DomaTree                 # noqa E402
from idds.doma.workflowv2.domaeventmap import DomaEventMap         # noqa E402


task_cloud = 'US'
task_queue = 'SLAC_Rubin'
# task_queue = 'SLAC_Rubin_Extra_Himem_32Cores'


# task_cloud = 'EU'
# task_queue = 'CC-IN2P3_TEST'

# task_cloud = 'EU'
# task_queue = 'LANCS_TEST'


def setup_gw_workflow():
    gwf = gw.GenericWorkflow("mytest")

    exec1 = gw.GenericWorkflowExec(
        name="test.py", src_uri="${CTRL_BPS_DIR}/bin/test1.py", transfer_executable=False
    )
    job1 = gw.GenericWorkflowJob("job1", label="label1")
    job1.quanta_counts = Counter({"pt1": 1, "pt2": 2})
    job1.executable = exec1

    job2 = gw.GenericWorkflowJob("job2", label="label2")
    job2.quanta_counts = Counter({"pt1": 1, "pt2": 2})
    job2.executable = exec1

    job3 = gw.GenericWorkflowJob("job3")
    job3.label = "label2"
    job3.quanta_counts = Counter({"pt1": 1, "pt2": 2})
    job3.executable = exec1

    gwf.add_job(job1)
    gwf.add_job(job2)
    gwf.add_job(job3)
    gwf.add_job_relationships("job1", ["job2", "job3"])
    # gwf.add_job_relationships("job1", "job2")

    srcjob1 = gw.GenericWorkflowJob("srcjob1")
    srcjob1.label = "srclabel1"
    srcjob1.executable = exec1
    srcjob2 = gw.GenericWorkflowJob("srcjob2")
    srcjob2.label = "srclabel1"
    srcjob2.executable = exec1
    srcjob3 = gw.GenericWorkflowJob("srcjob3")
    srcjob3.label = "srclabel2"
    srcjob3.executable = exec1
    srcjob4 = gw.GenericWorkflowJob("srcjob4")
    srcjob4.label = "srclabel2"
    srcjob4.executable = exec1
    gwf2 = gw.GenericWorkflow("mytest2")
    gwf2.add_job(srcjob1)
    gwf2.add_job(srcjob2)
    gwf2.add_job(srcjob3)
    gwf2.add_job(srcjob4)
    gwf2.add_job_relationships("srcjob1", "srcjob3")
    gwf2.add_job_relationships("srcjob2", "srcjob4")

    gwf.add_workflow_source(gwf2)
    return gwf


def setup_gw_workflow2():
    gwf = gw.GenericWorkflow("mytest")

    exec1 = gw.GenericWorkflowExec(
        name="test1.py", src_uri="${CTRL_BPS_DIR}/bin/test1.py", transfer_executable=False
    )

    exec2 = gw.GenericWorkflowExec(
        name="test2.py", src_uri="${CTRL_BPS_DIR}/bin/test2.py", transfer_executable=False
    )

    exec3 = gw.GenericWorkflowExec(
        name="test3.py", src_uri="${CTRL_BPS_DIR}/bin/test3.py", transfer_executable=False
    )

    exec4 = gw.GenericWorkflowExec(
        name="test4.py", src_uri="${CTRL_BPS_DIR}/bin/test4.py", transfer_executable=False
    )

    exec5 = gw.GenericWorkflowExec(
        name="test5.py", src_uri="${CTRL_BPS_DIR}/bin/test5.py", transfer_executable=False
    )

    job1 = gw.GenericWorkflowJob("init1", label="init")
    job1.quanta_counts = Counter({"pt1": 1, "pt2": 2})
    job1.executable = exec1
    gwf.add_job(job1)

    for i in range(1000):
        job2 = gw.GenericWorkflowJob("isr_%s" % i, label="isr")
        job2.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        job2.executable = exec2
        job2.attrs['grouping'] = True
        job2.attrs['grouping_max_jobs'] = 160
        gwf.add_job(job2)
        gwf.add_job_relationships("init1", "isr_%s" % i)

    for i in range(1000):
        job3 = gw.GenericWorkflowJob("characterizeImage_%s" % i, label="characterizeImage")
        job3.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        job3.executable = exec3
        job3.attrs['grouping'] = True
        job3.attrs['grouping_max_jobs'] = 160
        gwf.add_job(job3)
        gwf.add_job_relationships("isr_%s" % i, "characterizeImage_%s" % i)

    for i in range(1000):
        job4 = gw.GenericWorkflowJob("calibrate_%s" % i, label="calibrate")
        job4.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        job4.executable = exec4
        job4.attrs['grouping'] = True
        job4.attrs['grouping_max_jobs'] = 100
        gwf.add_job(job4)
        gwf.add_job_relationships("characterizeImage_%s" % i, "calibrate_%s" % i)

    for i in range(10):
        job5 = gw.GenericWorkflowJob("writePreSourceTable_%s" % i, label="writePreSourceTable")
        job5.quanta_counts = Counter({"pt1": 1, "pt2": 2})
        job5.executable = exec5
        job5.attrs['grouping'] = True
        job5.attrs['grouping_max_jobs'] = 8
        gwf.add_job(job5)

    for i in range(1000):
        gwf.add_job_relationships("calibrate_%s" % i, "writePreSourceTable_%s" % int(i / 100))

    return gwf


def test_show_jobs(generic_workflow):
    for job_label in generic_workflow.labels:
        jobs_by_label = generic_workflow.get_jobs_by_label(job_label)
        for gwjob in jobs_by_label:
            # pseudo_filename = _make_pseudo_filename(config, gwjob)
            # job_to_pseudo_filename[gwjob.name] = pseudo_filename
            # job_to_task[gwjob.name] = work.get_work_name()

            # deps = []
            for parent_job_name in generic_workflow.predecessors(gwjob.name):
                # deps.append({"task": job_to_task[parent_job_name],
                #              "inputname": job_to_pseudo_filename[parent_job_name],
                #              "available": False})
                pass

            # job = {"name": pseudo_filename, "dependencies": deps}


def construct_doma_jobs(generic_workflow):
    tree = DomaTree('test_tree', group_type='width')
    grouped_jobs = tree.from_generic_workflow(generic_workflow)
    # print(grouped_jobs)
    for grouped_label in grouped_jobs:
        print(grouped_label)
        for eventservice in grouped_jobs[grouped_label]:
            print("    %s" % eventservice['name'])
            print("        %s" % eventservice['events'])

    event_map = tree.construct_event_map(grouped_jobs)
    print(event_map)

    event_map1 = DomaEventMap()
    event_map1.load()
    print(event_map1)

    return event_map


def setup_workflow(event_map):
    pending_time = 12
    # pending_time = None
    workflow = Workflow(pending_time=pending_time)
    workflow.name = event_map.name

    for task_name in event_map.tasks:
        task = event_map.get_task(task_name)
        executable = "cmd_line_es_decoder.py"
        dependency_map = task.get_dependency_map()
        work = DomaPanDAESWork(executable=executable,
                               primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                               output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                               log_collections=[], es_dependency_map=dependency_map,
                               task_name=task_name, task_queue=task_queue,
                               encode_command_line=True,
                               prodSourceLabel='managed',
                               task_log={"dataset": "PandaJob_#{pandaid}/",
                                         "destination": "local",
                                         "param_type": "log",
                                         "token": "local",
                                         "type": "template",
                                         "value": "log.tgz"},
                               task_cloud=task_cloud)
        workflow.add_work(work)
    return workflow


def test():
    gw_workflow = setup_gw_workflow()
    # print(json.dumps(gw_workflow))
    # gw_workflow = setup_gw_workflow2()
    test_show_jobs(gw_workflow)
    event_map = construct_doma_jobs(gw_workflow)

    print("event_map")
    print(json.dumps(event_map.dict(), sort_keys=True, indent=4))
    # sys.exit(0)

    print("task_dep_map")

    for task_name in event_map.tasks:
        task = event_map.get_task(task_name)
        print("task_name :%s" % task_name)
        print(json.dumps(task.get_dependency_map(), sort_keys=True, indent=4))
    workflow = setup_workflow(event_map)

    # sys.exit(0)

    host = get_rest_host()
    wm = ClientManager(host=host)
    # wm.set_original_user(user_name="wguandev")
    request_id = wm.submit(workflow, use_dataset_name=False)
    print(request_id)


def test_load():
    event_map = DomaEventMap()
    event_map.load()
    print(event_map)

    # event file name
    task_name = "srclabel1_srclabel2_label1_label2"
    job_name = "eventservice_srclabel1_srclabel2_label1_label2_0_0"
    event_id = [0, 1, 2, 3]
    event_task = event_map.get_task(task_name)
    event_job = event_task.get_job(job_name)

    # sync event status from panda
    # get event status from panda
    event_job.set_event_finished(event_id[0], reported=True)    # reported True means that we don't need to update this event to panda
    event_job.set_event_failed(event_id[1], reported=True)
    event_job.set_event_missing(event_id[2], reported=True)

    # check the forth event
    is_ok = event_job.is_ok_to_process_event(event_id[3])
    print("is_ok: %s" % is_ok)
    if is_ok:
        event = event_job.get_event(event_id[3])
        # process event
        print(event)
        # ctrl bps job
        print(event.gwjob)
        event_job.set_event_finished(event.event_index)

    to_report = event_job.get_events_to_report()
    print(to_report)
    # report event status to panda
    event_job.acknowledge_event_report(to_report)   # update the report status, to avoid reporting it again
    to_report = event_job.get_events_to_report()
    print(to_report)


def test1():
    # gw_workflow = setup_gw_workflow()
    # print(json.dumps(gw_workflow))
    gw_workflow = setup_gw_workflow2()
    test_show_jobs(gw_workflow)
    construct_doma_jobs(gw_workflow)


def test_load1():
    event_map = DomaEventMap()
    event_map.load()
    print(event_map)

    # event file name
    # task_name = "srclabel1_srclabel2_label1_label2"
    # job_name = "eventservice_srclabel1_srclabel2_label1_label2_0_0"


if __name__ == '__main__':
    test()
    test_load()
    # test1()
    # test_load1()
