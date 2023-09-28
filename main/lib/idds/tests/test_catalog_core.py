#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023


from idds.common.constants import CollectionRelationType
from idds.common.utils import json_dumps
from idds.core.catalog import get_contents, get_contents_ext, combine_contents_ext


def test(request_id, workload_id, transform_id=None, group_by_jedi_task_id=False):
    contents = get_contents(request_id=request_id, workload_id=workload_id, transform_id=transform_id,
                            relation_type=CollectionRelationType.Output)
    print(len(contents))
    contents_ext = get_contents_ext(request_id=request_id, workload_id=workload_id, transform_id=transform_id)

    print((len(contents_ext)))
    ret_contents = combine_contents_ext(contents, contents_ext, with_status_name=True)
    print(len(ret_contents))
    rets = {}
    for content in ret_contents:
        if group_by_jedi_task_id:
            jedi_task_id = content.get('jedi_task_id', 'None')
            if jedi_task_id not in rets:
                rets[jedi_task_id] = []
            rets[jedi_task_id].append(content)
        else:
            transform_id = content.get('transform_id')
            if transform_id not in rets:
                rets[transform_id] = []
            rets[transform_id].append(content)
    return rets


if __name__ == '__main__':
    print('164443')
    rets = test(5460, 164443)
    # print(json_dumps(rets, sort_keys=True, indent=4))
    print('164448')
    rets = test(5460, 164448)
    # print(str(rets))
    print(json_dumps(rets, sort_keys=True, indent=4))
