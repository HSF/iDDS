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
from idds.rest.v1.utils import convert_old_req_2_workflow_req   # noqa F401
from idds.common.utils import setup_logging


setup_logging("idds.log")


def test():
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

    cm1 = ClientManager(host=atlas_host)
    cm1 = ClientManager(host=doma_host)
    cm1 = ClientManager(host=dev_host)
    request_id = 414

    ret = cm1.get_requests(request_id, with_detail=True)
    print(json_dumps(ret, sort_keys=True, indent=4))

    cm1.setup_json_outputs()
    ret = cm1.get_requests(request_id, with_detail=True)
    print(json_dumps(ret, sort_keys=True, indent=4))

    ret = cm1.get_request_id_by_name(name='test_workflow.idds.1672836584.9900262.test')
    print(json_dumps(ret, sort_keys=True, indent=4))

    ret = cm1.get_request_id_by_name(name='test_workflow.idds.1672836584.9900262.test1')
    print(json_dumps(ret, sort_keys=True, indent=4))

    ret = cm1.get_request_id_by_name(name='test_workflow.idds*')
    print(json_dumps(ret, sort_keys=True, indent=4))

    ret = cm1.get_contents_output_ext(request_id=request_id)
    print(json_dumps(ret, sort_keys=True, indent=4))

    ret = cm1.get_contents_output_ext(request_id=request_id, group_by_jedi_task_id=True)
    print(json_dumps(ret, sort_keys=True, indent=4))

    ret = cm1.get_contents_output_ext(request_id=None)
    print(json_dumps(ret, sort_keys=True, indent=4))

    ret = cm1.get_contents_output_ext(request_id=99999)
    print(json_dumps(ret, sort_keys=True, indent=4))


if __name__ == '__main__':
    test()
