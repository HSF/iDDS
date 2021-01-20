#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


"""
Test client.
"""


from idds.client.clientmanager import ClientManager
from idds.common.utils import get_rest_host


def get_workflow():
    from idds.workflow.workflow import Workflow
    from idds.atlas.workflow.atlashpowork import ATLASHPOWork

    # request_metadata for predefined method 'nevergrad'
    request_metadata = {'workload_id': '20525135', 'sandbox': None, 'method': 'nevergrad', 'opt_space': {"A": {"type": "Choice", "params": {"choices": [1, 4]}}, "B": {"type": "Scalar", "bounds": [0, 5]}}, 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'max_points': 20, 'num_points_per_generation': 10}   # noqa E501

    # request_metadata for docker method
    request_metadata = {'workload_id': '20525134', 'sandbox': 'wguanicedew/idds_hpo_nevergrad', 'workdir': '/data', 'executable': 'docker', 'arguments': 'python /opt/hyperparameteropt_nevergrad.py --max_points=%MAX_POINTS --num_points=%NUM_POINTS --input=/data/%IN --output=/data/%OUT', 'output_json': 'output.json', 'opt_space': {"A": {"type": "Choice", "params": {"choices": [1, 4]}}, "B": {"type": "Scalar", "bounds": [0, 5]}}, 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'max_points': 20, 'num_points_per_generation': 10}  # noqa E501

    work = ATLASHPOWork(executable=request_metadata.get('executable', None),
                        arguments=request_metadata.get('arguments', None),
                        parameters=request_metadata.get('parameters', None),
                        setup=None, exec_type='local',
                        sandbox=request_metadata.get('sandbox', None),
                        method=request_metadata.get('method', None),
                        container_workdir=request_metadata.get('workdir', None),
                        output_json=request_metadata.get('output_json', None),
                        opt_space=request_metadata.get('opt_space', None),
                        initial_points=request_metadata.get('initial_points', None),
                        max_points=request_metadata.get('max_points', None),
                        num_points_per_iteration=request_metadata.get('num_points_per_iteration', 10))
    wf = Workflow()
    wf.set_workload_id(request_metadata.get('workload_id', None))
    wf.add_work(work)
    return wf


if __name__ == '__main__':
    host = get_rest_host()
    # props = get_req_properties()
    # props = get_example_real_tape_stagein_request()
    # props = get_example_prodsys2_tape_stagein_request()
    # props = get_example_active_learning_request()
    workflow = get_workflow()

    # props = pre_check(props)
    # print(props)

    wm = ClientManager(host=host)
    request_id = wm.submit(workflow)
    print(request_id)
