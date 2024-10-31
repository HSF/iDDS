#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

import logging
import json

from idds.common.constants import WorkflowType
from idds.common.utils import encode_base64, setup_logging


setup_logging(__name__)


class BaseSubmitter(object):

    def __init__(self, *args, **kwargs):
        pass

    def get_task_params(self, work):
        if work.workflow_type in [WorkflowType.iWork]:
            task_name = work.name + "_" + str(work.request_id) + "_" + str(work.transform_id)
        elif work.workflow_type in [WorkflowType.iWorkflow, WorkflowType.iWorkflowLocal]:
            task_name = work.name + "_" + str(work.request_id)
        else:
            task_name = work.name

        in_files = []
        multi_jobs_kwargs_list = work.multi_jobs_kwargs_list
        for p in multi_jobs_kwargs_list:
            p = json.dumps(p)
            p = encode_base64(p)
            in_files.append(p)

        task_param_map = {}
        task_param_map['vo'] = work.vo if work.vo else 'wlcg'
        if work.queue and len(work.queue) > 0:
            task_param_map['site'] = work.queue
        if work.site and len(work.site) > 0:
            task_param_map['PandaSite'] = work.site
        if work.cloud and len(work.cloud) > 0:
            task_param_map['cloud'] = work.cloud

        task_param_map['workingGroup'] = work.working_group

        task_param_map['nFilesPerJob'] = 1
        if in_files:
            # if has_dependencies:
            #     task_param_map['inputPreStaging'] = True
            task_param_map['nFiles'] = len(in_files)
            task_param_map['noInput'] = True
            task_param_map['pfnList'] = in_files
        else:
            # task_param_map['inputPreStaging'] = True
            in_files = [json.dumps('pseudo_file')]
            task_param_map['nFiles'] = len(in_files)
            task_param_map['noInput'] = True
            task_param_map['pfnList'] = in_files

        task_param_map['taskName'] = task_name
        task_param_map['userName'] = work.username if work.username else 'iDDS'
        task_param_map['taskPriority'] = work.priority
        task_param_map['architecture'] = ''
        task_param_map['transUses'] = ''
        task_param_map['transHome'] = None

        # executable = work.executable
        executable = work.get_runner()
        # task_param_map['transPath'] = 'https://storage.googleapis.com/drp-us-central1-containers/bash-c-enc'
        # task_param_map['encJobParams'] = True
        # task_param_map['transPath'] = 'https://wguan-wisc.web.cern.ch/wguan-wisc/run_workflow_wrapper'
        # task_param_map['transPath'] = 'https://wguan-idds.web.cern.ch/run_workflow_wrapper'
        # task_param_map['transPath'] = 'http://pandaserver-doma.cern.ch:25080/trf/user/run_workflow_wrapper'
        # task_param_map['transPath'] = 'https://panda-doma-k8s-panda.cern.ch/trf/user/run_workflow_wrapper'
        task_param_map['transPath'] = 'https://pandaserver-doma.cern.ch/trf/user/run_workflow_wrapper'

        task_param_map['processingType'] = None
        task_param_map['prodSourceLabel'] = 'managed'   # managed, test, ptest

        task_param_map['noWaitParent'] = True
        task_param_map['taskType'] = 'iDDS'
        task_param_map['coreCount'] = work.core_count
        task_param_map['skipScout'] = True
        task_param_map['ramCount'] = work.total_memory / work.core_count if work.core_count else work.total_memory
        # task_param_map['ramUnit'] = 'MB'
        task_param_map['ramUnit'] = 'MBPerCoreFixed'

        # task_param_map['inputPreStaging'] = True
        task_param_map['prestagingRuleID'] = 123
        task_param_map['nChunksToWait'] = 1
        task_param_map['maxCpuCount'] = work.core_count
        task_param_map['maxWalltime'] = work.max_walltime
        task_param_map['maxFailure'] = work.max_attempt if work.max_attempt else 5
        task_param_map['maxAttempt'] = work.max_attempt if work.max_attempt else 5
        if task_param_map['maxAttempt'] < work.max_attempt:
            task_param_map['maxAttempt'] = work.max_attempt
        if task_param_map['maxFailure'] < work.max_attempt:
            task_param_map['maxFailure'] = work.max_attempt

        if work.enable_separate_log:
            logging.debug(f"BaseSubmitter enable_separate_log: {work.enable_separate_log}")
            task_param_map['log'] = {"dataset": "PandaJob_iworkflow/",   # "PandaJob_#{pandaid}/"
                                     "destination": "local",
                                     "param_type": "log",
                                     "token": "local",
                                     "type": "template",
                                     "value": "log.tgz"}

        task_param_map['jobParameters'] = [
            {'type': 'constant',
             'value': executable,  # noqa: E501
             },
        ]

        task_param_map['reqID'] = work.request_id

        if work.container_options:
            if type(work.container_options) in [dict] and work.container_options.get('container_image', None):
                container_image = work.container_options.get('container_image', None)
                task_param_map['container_name'] = container_image

        return task_param_map

    def submit(self, *args, **kwargs):
        pass


class BasePoller(object):

    def __init__(self, *args, **kwargs):
        pass

    def poll(self, *args, **kwargs):
        pass


class BaseSubmitterPoller(BaseSubmitter):

    def __init__(self, *args, **kwargs):
        super(BaseSubmitterPoller, self).__init__(*args, **kwargs)

    def poll(self, *args, **kwargs):
        pass
