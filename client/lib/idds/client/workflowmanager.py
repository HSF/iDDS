#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


"""
Workflow manager.
"""

from idds.common.utils import setup_logging

from idds.client.client import Client
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host

# from idds.workflow.work import Work, Parameter, WorkStatus
# from idds.workflow.workflow import Condition, Workflow


setup_logging(__name__)


class WorkflowManager:
    def __init__(self, host=None):
        self.host = host
        if self.host is None:
            self.host = get_rest_host()

    def submit(self, workflow):
        props = {
            'scope': 'workflow',
            'name': workflow.get_name(),
            'requester': 'panda',
            'request_type': RequestType.Workflow,
            'transform_tag': 'workflow',
            'status': RequestStatus.New,
            'priority': 0,
            'lifetime': 30,
            'workload_id': workflow.get_workload_id(),
            'request_metadata': {'workload_id': workflow.get_workload_id(), 'workflow': workflow}
        }
        primary_init_work = workflow.get_primary_initial_collection()
        if primary_init_work:
            props['scope'] = primary_init_work['scope']
            props['name'] = primary_init_work['name']

        print(props)
        client = Client(host=self.host)
        request_id = client.add_request(**props)
        return request_id
