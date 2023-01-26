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
from idds.common.utils import setup_logging


setup_logging("idds.log")


def migrate():
    # 72533, 72569, 72733, 72769, 72783, 72939, 73351, 73983, 74545,
    # 74567, 74569, 74573
    # dev
    dev_host = 'https://aipanda160.cern.ch:443/idds'    # noqa F841
    # doma
    doma_host = 'https://aipanda015.cern.ch:443/idds'   # noqa F841
    # atlas
    atlas_host = 'https://aipanda181.cern.ch:443/idds'  # noqa F841
    # doma google
    doma_google_host = 'https://34.133.138.229:443/idds'  # noqa F841

    slac_k8s_dev_host = 'https://rubin-panda-idds-dev.slac.stanford.edu:8443/idds'  # noqa F841

    cern_k8s_dev_host = 'https://panda-idds-dev.cern.ch/idds'  # noqa F841

    # cm1 = ClientManager(host=atlas_host)
    cm1 = ClientManager(host=doma_host)
    # cm1 = ClientManager(host=slac_k8s_dev_host)
    # reqs = cm1.get_requests(request_id=290)
    # old_request_id = 298163
    # old_request_id = 350723
    old_request_id = 359383
    # old_request_id = 349
    old_request_id = 2400
    old_request_id = 371204
    old_request_id = 372930
    old_request_id = 2603
    old_request_id = 2802
    old_request_id = 2816
    old_request_id = 3178

    # old_request_id = 1
    # for old_request_id in [152]:
    # for old_request_id in [60]:    # noqa E115
    # for old_request_id in [200]:    # noqa E115
    for old_request_id in [old_request_id]:    # noqa E115  # doma 183
        reqs = cm1.get_requests(request_id=old_request_id, with_metadata=True)

        # cm2 = ClientManager(host=dev_host)
        cm2 = ClientManager(host=doma_host)
        # cm2 = ClientManager(host=atlas_host)
        # cm2 = ClientManager(host=slac_k8s_dev_host)
        # cm2 = ClientManager(host=cern_k8s_dev_host)
        # print(reqs)

        print("num requests: %s" % len(reqs))
        for req in reqs[:1]:
            # print(req)
            # workflow = req['request_metadata']['workflow']
            # print(json_dumps(workflow, sort_keys=True, indent=4))

            req = convert_old_req_2_workflow_req(req)
            workflow = req['request_metadata']['workflow']
            workflow.clean_works()

            # for old idds version
            t_works = workflow.template.works
            if not t_works and hasattr(workflow, 'works_template'):
                workflow.template.works = workflow.works_template

            # print(json_dumps(workflow))
            # print(json_dumps(workflow, sort_keys=True, indent=4))
            req_id = cm2.submit(workflow)
            print("old request %s -> new request %s" % (old_request_id, req_id))


if __name__ == '__main__':
    migrate()
