#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2021


"""
Test workflow.
"""

import unittest2 as unittest
# from nose.tools import assert_equal
from idds.common.utils import setup_logging

from idds.client.client import Client
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host

from idds.workflowv2.work import Work   # Parameter, WorkStatus
from idds.workflowv2.workflow import Condition, Workflow


setup_logging(__name__)


class TestWorkflow(unittest.TestCase):

    def init(self):
        # init_p = Parameter({'input_dataset': 'data17:data17.test.raw.1'})
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=2,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.raw.1'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work2'}])
        work3 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=3,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.work2'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work3'}])

        workflow = Workflow()
        workflow.add_work(work1, initial=True)
        workflow.add_work(work2, initial=True)
        workflow.add_work(work3, initial=False)

        cond = Condition(cond=work2.is_finished, true_work=work3)
        workflow.add_condition(cond)

        return workflow

    def test_workflow(self):
        """ Workflow: Test workflow """
        # init_p = Parameter({'input_dataset': 'data17:data17.test.raw.1'})
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=2,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.raw.1'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work2'}])
        work3 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=3,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.work2'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work3'}])

        workflow = Workflow()
        workflow.add_work(work1, initial=True)
        workflow.add_work(work2, initial=True)
        workflow.add_work(work3, initial=False)

        cond = Condition(cond=work2.is_finished, true_work=work3)
        # print(cond.all_works())
        workflow.add_condition(cond)

        # check
        # workflow_str = workflow.serialize()
        # workflow1 = Workflow.deserialize(workflow_str)
        # print(workflow_str)
        # print(workflow1)
        works = workflow.get_current_works()
        # print([str(work) for work in works])
        # print([w.work_id for w in works])
        assert(works == [])

    def test_workflow_request(self):
        workflow = self.init()

        props = {
            'scope': 'workflow',
            'name': workflow.name,
            'requester': 'panda',
            'request_type': RequestType.Workflow,
            'transform_tag': 'workflow',
            'status': RequestStatus.New,
            'priority': 0,
            'lifetime': 30,
            'request_metadata': {'workload_id': '20776840', 'workflow': workflow}
        }

        # print(props)
        host = get_rest_host()
        client = Client(host=host)
        request_id = client.add_request(**props)
        print(request_id)
