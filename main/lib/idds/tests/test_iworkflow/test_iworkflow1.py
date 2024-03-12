#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


"""
Test workflow.
"""

import datetime
import inspect

# import unittest2 as unittest
# from nose.tools import assert_equal
from idds.common.utils import setup_logging


from idds.iworkflow.work import work
from idds.iworkflow.workflow import workflow

# from idds.iworkflow.utils import perform_workflow, run_workflow

from idds.common.utils import json_dumps, run_process, encode_base64


setup_logging(__name__)


@work
def test_func(name):
    print('test_func starts')
    print(name)
    print('test_func ends')
    return 'test result: %s' % name


def test_workflow():
    print("test workflow starts")
    ret = test_func(name='idds')
    print(ret)
    print("test workflow ends")


@work
def get_params():
    list_params = [i for i in range(10)]
    return list_params


def test_workflow_mulitple_work():
    print("test workflow multiple work starts")
    list_params = get_params()

    ret = test_func(list_params)
    print(ret)
    print("test workflow multiple work ends")


def submit_workflow(workflow):
    req_id = workflow.submit()
    print("req id: %s" % req_id)


def perform_workflow_wrapper(workflow):
    # ret = perform_workflow(workflow)
    # print(ret)
    setup = workflow.setup()
    cmd = setup + ";"
    return cmd


def run_workflow_wrapper(workflow):
    setup = workflow.setup()

    cmd = setup + "; perform_workflow --workflow " + encode_base64(json_dumps(workflow))
    print(f'To run workflow: {cmd}')
    exit_code = run_process(cmd)
    print(f'Run workflow finished with exit code: {exit_code}')
    return exit_code


@workflow
def test_workflow1():
    print("test workflow starts")
    # ret = test_func(name='idds')
    # print(ret)
    print("test workflow ends")


def test_workflow2():
    print("test workflow2 starts")
    # ret = test_func(name='idds')
    # print(ret)
    print("test workflow2 ends")


if __name__ == '__main__':
    print("datetime.datetime object")
    f = datetime.datetime(2019, 10, 10, 10, 10)
    print(dir(f))
    print(inspect.getmodule(f))
    # print(inspect.getfile(f))
    # print(inspect.getmodule(f).__name__)
    # print(inspect.getmodule(f).__file__)
    # print(inspect.signature(f))

    print("datetime.datetime function")
    f = datetime.datetime
    print(dir(f))
    print(f.__module__)
    print(f.__name__)
    print(inspect.getmodule(f))
    print(inspect.getfile(f))
    print(inspect.getmodule(f).__name__)
    print(inspect.getmodule(f).__file__)
    # print(inspect.signature(f))

    print("test_workflow function")
    f = test_workflow
    print(dir(f))
    print(f.__module__)
    print(f.__name__)
    print(inspect.getmodule(f))
    print(inspect.getfile(f))
    print(inspect.getmodule(f).__name__)
    print(inspect.getmodule(f).__file__)
    print(inspect.signature(f))

    print("test_workflow1")
    test_workflow1()
