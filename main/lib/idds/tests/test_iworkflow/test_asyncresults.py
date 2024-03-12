#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


import inspect     # noqa F401
import logging     # noqa F401
import os          # noqa F401
import shutil      # noqa F401
import sys         # noqa F401

# from nose.tools import assert_equal
from idds.common.utils import setup_logging

from idds.iworkflow.asyncresult import AsyncResult
from idds.iworkflow.workflow import Workflow       # workflow

setup_logging(__name__)
# setup_logging(__name__, loglevel='debug')
# logging.getLogger('stomp').setLevel(logging.DEBUG)


def test_workflow():
    print("test workflow starts")
    print('idds')
    print("test workflow ends")


if __name__ == '__main__':

    wf = Workflow(func=test_workflow, service='idds')

    workflow_context = wf._context

    logging.info("Test AsyncResult")
    a_ret = AsyncResult(workflow_context, wait_num=1, timeout=30)
    a_ret.subscribe()

    async_ret = AsyncResult(workflow_context, internal_id=a_ret.internal_id)
    test_result = "AsyncResult test (request_id: %s)" % (workflow_context.request_id)
    logging.info("AsyncResult publish: %s" % test_result)
    async_ret.publish(test_result)

    ret_q = a_ret.wait_result()
    logging.info("AsyncResult results: %s" % str(ret_q))
