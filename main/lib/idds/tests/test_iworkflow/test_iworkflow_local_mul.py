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

import inspect     # noqa F401
import logging
import os          # noqa F401
import shutil      # noqa F401
import sys         # noqa F401

# from nose.tools import assert_equal
from idds.common.utils import setup_logging, run_process, json_dumps, json_loads, create_archive_file    # noqa F401

from idds.iworkflow.workflow import Workflow, workflow       # workflow    # noqa F401
from idds.iworkflow.work import work


setup_logging(__name__)


@work
def test_func(name):
    print('test_func starts')
    print(name)
    print('test_func ends')
    return 'test result: %s' % name


def test_func1(name):
    print('test_func1 starts')
    print(name)
    print('test_func1 ends')
    return 'test result: %s' % name


# @workflow(service='idds', local=True, cloud='US', queue='FUNCX_TEST')   # queue = 'BNL_OSG_2'
@workflow(service='panda', local=True, cloud='US', queue='BNL_OSG_2')
def test_workflow():
    print("test workflow starts")
    ret = test_func(name='idds', multi_jobs_kwargs_list=[{'name': 'idds1'}, {'name': 'idds2'}, {'name': 'idds3'}, {'name': 'idds4'}, {'name': 'idds5'}])
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


def submit_workflow(wf):
    req_id = wf.submit()
    print("req id: %s" % req_id)


def run_workflow_wrapper(wf):
    cmd = wf.get_runner()
    logging.info(f'To run workflow: {cmd}')

    exit_code = run_process(cmd, wait=True)
    logging.info(f'Run workflow finished with exit code: {exit_code}')
    return exit_code


def run_workflow_remote_wrapper(wf):
    cmd = wf.get_runner()
    logging.info('To run workflow: %s' % cmd)

    work_dir = '/tmp/idds'
    shutil.rmtree(work_dir)
    os.makedirs(work_dir)
    os.chdir(work_dir)
    logging.info("current dir: %s" % os.getcwd())

    # print(dir(wf))
    # print(inspect.getmodule(wf))
    # print(inspect.getfile(wf))
    setup = wf.setup_source_files()
    logging.info("setup: %s" % setup)

    exc_cmd = 'cd %s' % work_dir
    exc_cmd += "; wget https://wguan-wisc.web.cern.ch/wguan-wisc/run_workflow_wrapper"
    exc_cmd += "; chmod +x run_workflow_wrapper; bash run_workflow_wrapper %s" % cmd
    logging.info("exc_cmd: %s" % exc_cmd)
    exit_code = run_process(exc_cmd, wait=True)
    logging.info(f'Run workflow finished with exit code: {exit_code}')
    return exit_code


def test_create_archive_file(wf):
    archive_name = wf._context.get_archive_name()
    source_dir = wf._context._source_dir
    logging.info("archive_name :%s, source dir: %s" % (archive_name, source_dir))
    archive_file = create_archive_file('/tmp', archive_name, [source_dir])
    logging.info("created archive file: %s" % archive_file)


if __name__ == '__main__':
    logging.info("start")
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    ret = test_workflow()
    print('result: %s' % str(ret))
