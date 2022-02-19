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
HyperParameterOptimization Rest client to access IDDS system.
"""

import os

from idds.common import exceptions
from idds.client.base import BaseRestClient


class HPOClient(BaseRestClient):

    """HPO Rest client"""

    HPO_BASEURL = 'hpo'

    def __init__(self, host=None, auth=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the IDDS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(HPOClient, self).__init__(host=host, auth=auth, timeout=timeout)

    def update_hyperparameter(self, workload_id, request_id, id, loss):
        """
        Update hyperparameter to the Head service.

        :param workload_id: the workload id.
        :param request_id: the request.
        :param id: id of the hyper parameter.
        :param loss: the loss.

        :raise exceptions if it's not updated successfully.
        """
        if not workload_id:
            workload_id = 'null'
        if not request_id:
            request_id = 'null'

        if workload_id == 'null' and request_id == 'null':
            raise exceptions.IDDSException("One of workload_id and request_id should not be None or empty")

        path = self.HPO_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(workload_id), str(request_id), str(id), str(loss)))

        r = self.get_request_response(url, type='PUT')
        return r

    def get_hyperparameters(self, workload_id, request_id, id=None, status=None, limit=None):
        """
        Get hyperparameters from the Head service.

        :param workload_id: the workload id.
        :param request_id: the request id.
        :param status: the status of the hyperparameters.
        :param limit: limit number of hyperparameters

        :raise exceptions if it's not got successfully.
        """
        path = self.HPO_BASEURL
        if not workload_id:
            workload_id = 'null'
        if not request_id:
            request_id = 'null'
        if id is None:
            id = 'null'
        if status is None:
            status = 'null'
        if limit is None:
            limit = 'null'

        if workload_id == 'null' and request_id == 'null':
            raise exceptions.IDDSException("One of workload_id and request_id should not be None or empty")

        url = self.build_url(self.host, path=os.path.join(path, str(workload_id), str(request_id), str(id), str(status), str(limit)))

        params = self.get_request_response(url, type='GET')

        return params
