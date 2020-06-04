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
Test hyper parameter optimization docker container in local env.
"""

import json
import os
import random
import re
import subprocess
import tempfile
# from uuid import uuid4 as uuid

# from idds.client.client import Client
# from idds.common.utils import replace_parameters_with_values, run_command
from idds.common.constants import RequestType, RequestStatus
# from idds.common.utils import get_rest_host


def get_test_codes():
    dir_name = os.path.dirname(os.path.abspath(__file__))
    test_codes = os.path.join(dir_name, 'activelearning_test_codes/activelearning_test_codes.tgz')
    return test_codes


def run_command(cmd):
    """
    Runs a command in an out-of-procees shell.
    """
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    stdout, stderr = process.communicate()
    if stdout is not None and type(stdout) in [bytes]:
        stdout = stdout.decode()
    if stderr is not None and type(stderr) in [bytes]:
        stderr = stderr.decode()
    status = process.returncode
    return status, stdout, stderr


def replace_parameters_with_values(text, values):
    """
    Replace all strings starting with '%'. For example, for this string below, it should replace ['%NUM_POINTS', '%IN', '%OUT']
    'run --rm -it -v "$(pwd)":/payload gitlab-registry.cern.ch/zhangruihpc/endpointcontainer:latest /bin/bash -c "echo "--num_points %NUM_POINTS"; /bin/cat /payload/%IN>/payload/%OUT"'

    :param text
    :param values: parameter values, for example {'NUM_POINTS': 5, 'IN': 'input.json', 'OUT': 'output.json'}
    """
    for key in values:
        key1 = '%' + key
        text = re.sub(key1, str(values[key]), text)
    return text


def create_temp_dir():
    dirpath = tempfile.mkdtemp()
    return dirpath


def create_idds_input_json(job_dir, req_meta):
    input_json = 'idds_input_%s.json' % random.randint(1, 1000)
    if 'initial_points' in req_meta:
        points = req_meta['initial_points']
    else:
        points = []

    opt_space = None
    opt_points = {'points': points, 'opt_space': opt_space}
    if 'opt_space' in req_meta:
        opt_space = req_meta['opt_space']
        opt_points['opt_space'] = opt_space
    with open(os.path.join(job_dir, input_json), 'w') as f:
        json.dump(opt_points, f)
    return input_json


def get_output_json(req_meta):
    output_json = None
    if 'output_json' in req_meta:
        output_json = req_meta['output_json']
    return output_json


def get_exec(job_dir, req_meta):
    if 'max_points' in req_meta:
        max_points = req_meta['max_points']
    else:
        max_points = 20
    if 'num_points_per_generation' in req_meta:
        num_points = req_meta['num_points_per_generation']
    else:
        num_points = 10
    num_init_points = 0
    if 'initial_points' in req_meta:
        num_init_points = len(req_meta['initial_points'])
    num_points = min(max_points, num_points) - num_init_points

    idds_input_json = create_idds_input_json(job_dir, req_meta)
    output_json = get_output_json(req_meta)

    param_values = {'MAX_POINTS': max_points,
                    'NUM_POINTS': num_points,
                    'IN': idds_input_json,
                    'OUT': output_json}

    executable = req_meta['executable'].strip()
    arguments = req_meta['arguments']
    executable = replace_parameters_with_values(executable, param_values)
    arguments = replace_parameters_with_values(arguments, param_values)

    if executable == 'docker':
        executable = 'docker run --rm -v $(pwd):%s %s' % (req_meta['workdir'], req_meta['sandbox'])
    exect = executable + ' ' + arguments
    return exect


def get_req_properties():
    req_properties = {
        # 'scope': 'data15_13TeV',
        # 'name': 'data15_13TeV.00270949.pseudo.%s' % str(uuid()),
        'requester': 'panda',
        'request_type': RequestType.HyperParameterOpt,
        'transform_tag': 'prodsys2',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        # 'request_metadata': {'workload_id': '20525134', 'sandbox': None, 'executable': 'docker run --rm -it -v "$(pwd)":/payload gitlab-registry.cern.ch/zhangruihpc/endpointcontainer:latest /bin/bash -c "/bin/cat /payload/input_json.txt>/payload/output_json.txt"', 'arguments': '-s --input %IN', 'output_json': 'output.json'}  # noqa: E501
        # 'request_metadata': {'workload_id': '20525134', 'is_pseudo_input': True, 'sandbox': None, 'executable': 'docker', 'arguments': 'run --rm -it -v "$(pwd)":/payload gitlab-registry.cern.ch/zhangruihpc/endpointcontainer:latest /bin/cat /payload/%IN>%OUT', 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'output_json': 'output.json'}  # noqa: E501
        # 'request_metadata': {'workload_id': '20525134', 'is_pseudo_input': True, 'sandbox': None, 'method': 'nevergrad', 'opt_space': {'A': [1, 2, 3], 'B': (1, 10)}, 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'max_points': 10}  # noqa: E501
        # 'request_metadata': {'workload_id': '20525134', 'sandbox': None, 'executable': 'docker', 'arguments': 'run -v $(pwd):/data wguanicedew/idds_hpo_nevergrad python /opt/hyperparameteropt_nevergrad.py --max_points=%MAX_POINTS --num_points=%NUM_POINTS --input=/data/%IN --output=/data/%OUT', 'output_json': 'output.json', 'opt_space': {"A": {"type": "Choice", "params": {"choices": [1, 4]}}, "B": {"type": "Scalar", "bounds": [0, 5]}}, 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'max_points': 20, 'num_points_per_generation': 10}  # noqa: E501
        'request_metadata': {'workload_id': '20525134', 'sandbox': 'wguanicedew/idds_hpo_nevergrad', 'workdir': '/data', 'executable': 'docker', 'arguments': 'python /opt/hyperparameteropt_nevergrad.py --max_points=%MAX_POINTS --num_points=%NUM_POINTS --input=/data/%IN --output=/data/%OUT', 'output_json': 'output.json', 'opt_space': {"A": {"type": "Choice", "params": {"choices": [1, 4]}}, "B": {"type": "Scalar", "bounds": [0, 5]}}, 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'max_points': 20, 'num_points_per_generation': 10}  # noqa: E501
    }
    return req_properties


def run_test():
    job_dir = create_temp_dir()
    print('job dir: %s' % job_dir)
    os.chdir(job_dir)
    print("$pwd: %s" % os.getcwd())
    req = get_req_properties()
    req_meta = req['request_metadata']
    exe = get_exec(job_dir, req_meta)
    print('executable: %s' % exe)
    status, output, error = run_command(exe)
    print('status: %s, output: %s, error: %s' % (status, output, error))
    output_json = get_output_json(req_meta)
    if not output_json:
        print('output_json is not defined')
    else:
        output_json = os.path.join(job_dir, output_json)
        if not os.path.exists(output_json):
            print("Failed: output_json is not created")
        else:
            try:
                with open(output_json, 'r') as f:
                    data = f.read()
                outputs = json.loads(data)
                print("outputs: %s" % str(outputs))
                if type(outputs) not in [list, tuple]:
                    print("Failed: outputs is not a list")
            except Exception as ex:
                print("Failed to parse output_json: %s" % ex)


if __name__ == '__main__':
    run_test()
