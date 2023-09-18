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
Test workflow condtions.
"""

# import json
import logging

# from nose.tools import assert_equal
from idds.common.utils import setup_logging, get_logger                  # noqa F401

from idds.common.utils import json_dumps, json_loads                     # noqa F401

from idds.common.dict_class import DictClass                             # noqa F401
from idds.workflowv2.work import Work, WorkStatus
from idds.workflowv2.workflow import (CompositeCondition, AndCondition, OrCondition,                # noqa F401
                                      Condition, ConditionTrigger, Workflow, ParameterLink)         # noqa F401


setup_logging(__name__)

logger = logging.getLogger("main")


def test_workflow_condition():
    work1 = Work(executable='/bin/hostname1', arguments=None, sandbox=None, work_id=1)
    work2 = Work(executable='/bin/hostname2', arguments=None, sandbox=None, work_id=2)

    workflow1 = Workflow()
    workflow1.add_work(work1, initial=False)
    workflow1.add_work(work2, initial=False)

    workflow1.test = True
    cond = Condition(cond=work1.is_finished, true_work=work2)
    workflow1.add_condition(cond)

    workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow1 = json_loads(workflow_str)

    workflow_str1 = json_dumps(workflow1, sort_keys=True, indent=4)     # noqa F841
    # print(workflow_str1)

    # assert(sorted(json.loads(workflow_str).items()) == sorted(json.loads(workflow_str1).items()))


def test_workflow_subloopworkflow_reload():
    work1 = Work(executable='/bin/hostname1', arguments=None, sandbox=None, work_id=1)
    work2 = Work(executable='/bin/hostname2', arguments=None, sandbox=None, work_id=2)

    workflow1 = Workflow()
    workflow1.add_work(work1, initial=False)
    workflow1.add_work(work2, initial=False)

    cond = Condition(cond=work2.is_finished)
    workflow1.add_loop_condition(cond)

    work3 = Work(executable='/bin/hostname3', arguments=None, sandbox=None, work_id=3)
    cond1 = Condition(cond=work3.is_finished, true_work=workflow1)

    workflow = Workflow()
    workflow.add_work(work3, initial=False)
    workflow.add_work(workflow1, initial=False)
    workflow.add_condition(cond1)

    # reload
    # print(workflow.conditions)
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)
    # print(workflow)
    # print(json_dumps(workflow, sort_keys=True, indent=4))

    logger.info("1")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    assert(works == [work3])
    # assert(workflow.num_run == 1)

    for work in works:
        # if work.work_id == 3:
        work.transforming = True
        work.submitted = True
        work.status = WorkStatus.Finished
        # print(work)
        # print(work.get_internal_id())

    # reload
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)

    logger.info("2")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    print(works)
    assert(works == [work1, work2])
    assert(workflow.is_terminated() is False)

    for work in works:
        work.transforming = True
        work.submitted = True
        work.status = WorkStatus.Finished

    # reload
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)

    logger.info("3")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    print(works)
    assert(works == [work1, work2])
    assert(workflow.is_terminated() is False)

    for work in works:
        work.transforming = True
        work.submitted = True
        work.status = WorkStatus.Failed

    # reload
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)

    logger.info("4")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    assert(works == [])
    assert(workflow.is_terminated() is True)


def test_workflow_subloopworkflow_reload1():
    work1 = Work(executable='/bin/hostname1', arguments=None, sandbox=None, work_id=1)
    work2 = Work(executable='/bin/hostname2', arguments=None, sandbox=None, work_id=2)

    workflow1 = Workflow()
    workflow1.add_work(work1, initial=False)
    workflow1.add_work(work2, initial=False)

    cond = Condition(cond=work2.is_finished)
    workflow1.add_loop_condition(cond)

    work3 = Work(executable='/bin/hostname3', arguments=None, sandbox=None, work_id=3)
    cond1 = Condition(cond=workflow1.is_terminated, true_work=work3)

    workflow = Workflow()
    workflow.add_work(work3, initial=False)
    workflow.add_work(workflow1, initial=False)
    workflow.add_condition(cond1)

    # reload
    # print(workflow.conditions)
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)
    # print(workflow)
    # print(json_dumps(workflow, sort_keys=True, indent=4))

    logger.info("1")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    assert(works == [work1, work2])
    # assert(workflow.num_run == 1)

    for work in works:
        # if work.work_id == 3:
        work.transforming = True
        work.submitted = True
        work.status = WorkStatus.Finished
        # print(work)
        # print(work.get_internal_id())

    # reload
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)

    logger.info("2")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    print(works)
    assert(works == [work1, work2])
    assert(workflow.is_terminated() is False)

    for work in works:
        work.transforming = True
        work.submitted = True
        work.status = WorkStatus.Failed

    # reload
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)

    logger.info("3")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    print(works)
    assert(works == [work3])
    assert(workflow.is_terminated() is False)

    for work in works:
        work.transforming = True
        work.submitted = True
        work.status = WorkStatus.Finished

    # reload
    workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
    # print(workflow_str)
    workflow = json_loads(workflow_str)

    logger.info("4")
    works = workflow.get_new_works()
    works.sort(key=lambda x: x.work_id)
    assert(works == [])
    assert(workflow.is_terminated() is True)


if __name__ == "__main__":
    print("==================================================")
    print("test_workflow_condition")
    test_workflow_condition()

    print("==================================================")
    print("test_workflow_subloopworkflow_reload")
    test_workflow_subloopworkflow_reload()

    print("==================================================")
    print("test_workflow_subloopworkflow_reload1")
    test_workflow_subloopworkflow_reload1()
