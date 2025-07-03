#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024 - 2025

import logging
import json
import re

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

        if work.no_wait_parent:
            task_param_map['noWaitParent'] = work.no_wait_parent

        task_param_map['workingGroup'] = work.working_group

        if work.num_events:
            task_param_map['nEvents'] = work.num_events
            if work.num_events_per_job:
                task_param_map['nEventsPerJob'] = work.num_events_per_job
            else:
                task_param_map['nEventsPerJob'] = work.num_events

        task_param_map['taskName'] = task_name
        task_param_map['userName'] = work.username if work.username else 'iDDS'
        task_param_map['taskPriority'] = work.priority
        # task_param_map['architecture'] = ''
        task_param_map['architecture'] = '@el9'
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
        # task_param_map['transPath'] = 'https://pandaserver-doma.cern.ch/trf/user/run_workflow_wrapper'
        task_param_map['transPath'] = 'https://storage.googleapis.com/drp-us-central1-containers/run_workflow_wrapper'

        task_param_map['processingType'] = None
        task_param_map['prodSourceLabel'] = 'managed'   # managed, test, ptest

        # task_param_map['noWaitParent'] = True
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

        task_param_map['jobParameters'] = [
            {'type': 'constant',
             'value': executable,  # noqa: E501
             },
        ]

        # task_param_map['nFilesPerJob'] = 1
        if in_files:
            # if has_dependencies:
            #     task_param_map['inputPreStaging'] = True
            task_param_map['nFiles'] = len(in_files)
            task_param_map['noInput'] = True
            task_param_map['pfnList'] = in_files
            task_param_map['nFilesPerJob'] = 1
        elif work.input_datasets:
            for i, (input_file_name, input_dataset_name) in enumerate(work.input_datasets.items()):
                input_dataset_name = input_dataset_name.replace("$WORKFLOWID", str(work.request_id))
                tmp_dict = {
                    "type": "template",
                    "param_type": "input",
                    "exclude": "\.log\.tgz(\.\d+)*$",   # noqa W605
                    "expand": True,
                    # "value": '--inputs%s "${IN/T}" --input_map%s %s' % (i, i, input_file_name),
                    "value": f'--inputs{i} "${{IN/T}}" --input_map{i} {input_file_name}',
                    "dataset": input_dataset_name
                }
                i += 1
                task_param_map['jobParameters'].append(tmp_dict)
                task_param_map['dsForIN'] = input_dataset_name
        else:
            # task_param_map['inputPreStaging'] = True
            in_files = [json.dumps('pseudo_file')]
            task_param_map['nFiles'] = len(in_files)
            task_param_map['noInput'] = True
            task_param_map['pfnList'] = in_files
            task_param_map['nFilesPerJob'] = 1

        if work.output_dataset_name:
            if not work.output_dataset_name.endswith("/"):
                work.output_dataset_name = work.output_dataset_name + "/"

        if work.output_dataset_name and work.output_file_name:
            output_dataset_name = work.output_dataset_name.replace("$WORKFLOWID", str(work.request_id))
            output_dataset_name_no_scope = output_dataset_name.split(":")[-1]
            output_file_name = f"{output_dataset_name_no_scope[:-1]}_${{SN/P}}.{work.output_file_name}"
            tmp_dict = {"dataset": output_dataset_name,
                        "container": output_dataset_name,
                        # "destination": "local",
                        "param_type": "output",
                        # "token": "local",
                        "type": "template",
                        # "value": "log.tgz"}
                        "value": output_file_name
                        }

            task_param_map['jobParameters'].append(tmp_dict)

            output_map = {work.output_file_name: output_file_name}
            task_param_map["jobParameters"] += [
                {
                    "type": "constant",
                    # "value": f" --output {work.output_file_name} --mapped_output {output_file_name}",
                    "value": ' --output_map "{0}"'.format(str(output_map)),
                },
            ]

        # if work.enable_separate_log:
        if True:
            if work.log_dataset_name:
                log_dataset_name = work.log_dataset_name
            elif work.output_dataset_name:
                log_dataset_name = re.sub('/$', '.log/', work.output_dataset_name)
            else:
                log_dataset_name = f"Panda.iworkflow.{work.request_id}/"   # "PandaJob_#{pandaid}/"

            log_dataset_name = log_dataset_name.replace("$WORKFLOWID", str(work.request_id))
            log_dataset_name_no_scope = log_dataset_name.split(":")[-1]

            logging.debug(f"BaseSubmitter enable_separate_log: {work.enable_separate_log}")
            task_param_map['log'] = {"dataset": log_dataset_name,
                                     "container": log_dataset_name,
                                     # "destination": "local",
                                     "param_type": "log",
                                     # "token": "local",
                                     "type": "template",
                                     # "value": "log.tgz"}
                                     # 'value': '{0}.$JEDITASKID.${{SN}}.log.tgz'.format(log_dataset_name[:-1])
                                     'value': '{0}.${{SN}}.log.tgz'.format(log_dataset_name_no_scope[:-1])
                                     }

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
