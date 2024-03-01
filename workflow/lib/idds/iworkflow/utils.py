#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

# from idds.common.utils import encode_base64


def show_relation_map(relation_map, level=0):
    # a workflow with a list of works.
    if level == 0:
        prefix = ""
    else:
        prefix = " " * level * 4

    for item in relation_map:
        if type(item) in [dict]:
            # it's a Work
            print("%s%s" % (prefix, item['work']['workload_id']))
            if 'next_works' in item:
                # print("%s%s next_works:" % (prefix, item['work']['workload_id']))
                next_works = item['next_works']
                # it's a list.
                show_relation_map(next_works, level=level + 1)
        elif type(item) in [list]:
            # it's a subworkflow with a list of works.
            print("%ssubworkflow:" % (prefix))
            show_relation_map(next_works, level=level + 1)


def perform_workflow(workflow):
    workflow.load()
    workflow.run()
