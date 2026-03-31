#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025


"""
REST client for iDDS workflow task management endpoints.
"""

import os

from idds.client.base import BaseRestClient


class WorkflowTaskClient(BaseRestClient):

    """REST client for /workflow_task endpoints."""

    BASEURL = 'workflow_task'

    def __init__(self, host=None, auth=None, timeout=None):
        super(WorkflowTaskClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def create_workflow_task(self, workflow):
        """
        Create a workflow task.

        Corresponds to PandaClient.idds_create_workflow_task(workflow).

        :param workflow: The workflow dict (``message["content"]["workflow"]`` from the spec).
        :returns: dict with request_id, transform_id, processing_id,
                  input_coll_id, output_coll_id, workload_id.
        """
        url = self.build_url(self.host, path=self.BASEURL)
        data = {'workflow': workflow}
        return self.get_request_response(url, type='POST', data=data)

    def adjust_worker(self, request_id, transform_id, workload_id, parameters):
        """
        Adjust worker resource parameters for a running workflow task.

        Corresponds to PandaClient.idds_adjust_worker(content).

        :param request_id:   iDDS request id.
        :param transform_id: iDDS transform id.
        :param workload_id:  PanDA workload/task id.
        :param parameters:   Dict with core_count, memory_per_core, site, content.
        """
        if request_id is None:
            request_id = 'null'
        if transform_id is None:
            transform_id = 'null'
        if workload_id is None:
            workload_id = 'null'
        path = os.path.join(self.BASEURL, str(request_id), str(transform_id), str(workload_id), 'adjust')
        url = self.build_url(self.host, path=path)
        data = {'parameters': parameters}
        return self.get_request_response(url, type='PUT', data=data)

    def close_workflow_task(self, request_id, parameters):
        """
        Close a workflow task.

        Corresponds to PandaClient.idds_close_workflow_task(content).

        :param request_id:  iDDS request id.
        :param parameters:  Dict that must include workload_id; may include transform_id, run_id.
        """
        if request_id is None:
            request_id = 'null'
        path = os.path.join(self.BASEURL, str(request_id), 'close')
        url = self.build_url(self.host, path=path)
        data = {'parameters': parameters}
        return self.get_request_response(url, type='PUT', data=data)
