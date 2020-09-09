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


def setup_workflow():
    work1 = DomaLSSTWork(executable='echo',
                         arguments=None,
                         primary_input_collection={'scope': 'user.wguan', 'name': 'user.wguan.test_domalsst.1'},
                         output_collections=[{'scope': 'user.wguan', 'name': 'user.wguan.test_domalsst.output.1'}],
                         log_collections=[{'scope': 'user.wguan', 'name': 'user.wguan.test_domalsst.log.1'}])
    work2 = DomaLSSTWork(executable='echo',
                         arguments=None,
                         primary_input_collection={'scope': 'user.wguan', 'name': 'user.wguan.test_domalsst.1'},
                         output_collections=[{'scope': 'user.wguan', 'name': 'user.wguan.test_domalsst.output.1'}],
                         log_collections=[{'scope': 'user.wguan', 'name': 'user.wguan.test_domalsst.log.1'}])

    workflow = Workflow()
    workflow.add_work(work1)
    workflow.add_work(work2)

    cond = Condition(cond=work1.my_condition, current_work=work1, true_work=work2)
    workflow.add_condition(cond)
    return workflow


if __name__ == '__main__':
    host = get_rest_host()
    workflow = setup_workflow()

    wm = WorkflowManager(host=host)
    request_id = wm.submit(workflow)
    print(request_id)
