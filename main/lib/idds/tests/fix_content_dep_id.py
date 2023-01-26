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
Test client.
"""


from idds.client.clientmanager import ClientManager      # noqa F401
from idds.common.utils import json_dumps  # noqa F401
from idds.rest.v1.utils import convert_old_req_2_workflow_req    # noqa F401
from idds.common.utils import setup_logging

from idds.common.utils import json_dumps, setup_logging                 # noqa F401
from idds.common.constants import ContentStatus, ContentType, ContentRelationType, ContentLocking          # noqa F401
from idds.core.requests import get_requests              # noqa F401
from idds.core.messages import retrieve_messages         # noqa F401
from idds.core.transforms import get_transforms, get_transform          # noqa F401
from idds.core.workprogress import get_workprogresses    # noqa F401
from idds.core.processings import get_processings        # noqa F401
from idds.core import transforms as core_transforms      # noqa F401
from idds.core import catalog as core_catalog            # noqa F401
from idds.orm.contents import get_contents               # noqa F401
from idds.orm import contents as orm_contents            # noqa F401
from idds.core.transforms import release_inputs_by_collection, release_inputs_by_collection_old     # noqa F401
from idds.workflowv2.workflow import Workflow            # noqa F401
from idds.workflowv2.work import Work                    # noqa F401


setup_logging(__name__)


def fix_content_dep_id(request_id):
    contents = core_catalog.get_contents_by_request_transform(request_id=request_id)
    available_output_contents = {}
    output_content_name_id = {}
    contents_updates = []
    for content in contents:
        if content['content_relation_type'] == ContentRelationType.Output:
            if content['coll_id'] not in output_content_name_id:
                output_content_name_id[content['coll_id']] = {}
            output_content_name_id[content['coll_id']][content['name']] = content['content_id']
            if content['substatus'] in [ContentStatus.Available]:
                available_output_contents[content['content_id']] = content
                n_content = {'content_id': content['content_id'],
                             'substatus': content['substatus']}
                contents_updates.append(n_content)
    to_update_contents = []
    for content in contents:
        if content['content_relation_type'] == ContentRelationType.InputDependency:
            u_content = {'content_id': content['content_id'],
                         'content_dep_id': output_content_name_id[content['coll_id']][content['name']]}
            to_update_contents.append(u_content)
    orm_contents.update_contents(to_update_contents)
    orm_contents.add_contents_update(contents_updates)


if __name__ == "__main__":
    request_id = 3188
    fix_content_dep_id(request_id)
    core_catalog.delete_contents_update()
