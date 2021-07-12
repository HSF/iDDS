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


from idds.client.clientmanager import ClientManager
from idds.common.utils import json_dumps  # noqa F401
from idds.rest.v1.utils import convert_old_req_2_workflow_req


def migrate():
    # 72533, 72569, 72733, 72769, 72783, 72939, 73351, 73983, 74545,
    # 74567, 74569, 74573
    # dev
    dev_host = 'https://aipanda160.cern.ch:443/idds'    # noqa F841
    # doma
    doma_host = 'https://aipanda015.cern.ch:443/idds'   # noqa F841
    # atlas
    atlas_host = 'https://aipanda181.cern.ch:443/idds'  # noqa F841

    cm1 = ClientManager(host=doma_host)
    # reqs = cm1.get_requests(request_id=290)
    old_request_id = 72533
    for old_request_id in [27]:
    # for old_request_id in [60]:    # noqa E115
        reqs = cm1.get_requests(request_id=old_request_id, with_metadata=True)

        cm2 = ClientManager(host=dev_host)
        for req in reqs:
            req = convert_old_req_2_workflow_req(req)
            workflow = req['request_metadata']['workflow']
            workflow.clean_works()
            # print(json_dumps(workflow))
            # print(json_dumps(workflow, sort_keys=True, indent=4))
            req_id = cm2.submit(workflow)
            print("old request %s -> new request %s" % (old_request_id, req_id))


if __name__ == '__main__':
    migrate()
