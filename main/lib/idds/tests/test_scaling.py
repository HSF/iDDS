#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021


"""
Test client.
"""

import copy

from idds.client.clientmanager import ClientManager
from idds.common.utils import json_dumps  # noqa F401
from idds.rest.v1.utils import convert_old_req_2_workflow_req


def migrate():
    # dev
    dev_host = 'https://aipanda160.cern.ch:443/idds'   # noqa F841
    # doma
    doma_host = 'https://aipanda015.cern.ch:443/idds'  # noqa F841

    cm1 = ClientManager(host=dev_host)
    # reqs = cm1.get_requests(request_id=290)

    # 230K jobs
    reqs = cm1.get_requests(request_id=38, with_metadata=True)

    # reqs = cm1.get_requests(request_id=37, with_metadata=True)

    cm2 = ClientManager(host=dev_host)
    for req in reqs:
        req = convert_old_req_2_workflow_req(req)
        workflow = req['request_metadata']['workflow']
        workflow.clean_works()
        # print(json_dumps(workflow))
        # print(json_dumps(workflow, sort_keys=True, indent=4))
        for i in range(1):
            wf = copy.deepcopy(workflow)
            req_id = cm2.submit(wf)
            print(req_id)


if __name__ == '__main__':
    migrate()
