#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2022
# - Sergey Padolski, <spadolski@bnl.gov>, 2020


try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

import datetime
import os
import traceback

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionStatus, CollectionType,
                                   ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
from idds.workflow.work import Work, Processing
from idds.workflow.workflow import Condition


class DomaCondition(Condition):
    def __init__(self, cond=None, current_work=None, true_work=None, false_work=None):
        super(DomaCondition, self).__init__(cond=cond, current_work=current_work,
                                            true_work=true_work, false_work=false_work)


class DomaPanDAWork(Work):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='lsst', exec_type='panda', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 input_collections=None,
                 primary_output_collection=None, other_output_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None, dependency_map=None, task_name="",
                 task_queue=None, queue=None, processing_type=None,
                 prodSourceLabel='test', task_type='test',
                 maxwalltime=90000, maxattempt=5, core_count=1,
                 encode_command_line=False,
                 num_retries=5,
                 task_priority=900,
                 task_log=None,
                 task_cloud=None,
                 task_site=None,
                 task_rss=1000,
                 vo='wlcg',
                 working_group='lsst'):

        super(DomaPanDAWork, self).__init__(executable=executable, arguments=arguments,
                                            parameters=parameters, setup=setup, work_type=TransformType.Processing,
                                            work_tag=work_tag, exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                            primary_input_collection=primary_input_collection,
                                            other_input_collections=other_input_collections,
                                            primary_output_collection=primary_output_collection,
                                            other_output_collections=other_output_collections,
                                            input_collections=input_collections,
                                            output_collections=output_collections,
                                            log_collections=log_collections,
                                            release_inputs_after_submitting=True,
                                            logger=logger)
        # self.pandamonitor = None
        self.panda_url = None
        self.panda_url_ssl = None
        self.panda_monitor = None
        self.panda_auth = None
        self.panda_auth_vo = None
        self.panda_config_root = None
        self.pandacache_url = None
        self.panda_verify_host = None

        self.dependency_map = dependency_map
        self.dependency_map_deleted = []
        # self.logger.setLevel(logging.DEBUG)

        self.task_name = task_name
        self.real_task_name = None
        self.set_work_name(task_name)
        self.task_queue = task_queue
        self.queue = queue
        self.dep_tasks_id_names_map = {}
        self.executable = executable
        self.processingType = processing_type
        self.prodSourceLabel = prodSourceLabel
        self.task_type = task_type
        self.maxWalltime = maxwalltime
        self.maxAttempt = maxattempt if maxattempt else 5
        self.core_count = core_count if core_count else 1
        self.task_log = task_log

        self.encode_command_line = encode_command_line
        self.task_cloud = task_cloud
        self.task_site = task_site
        self.task_rss = task_rss
        self.task_priority = task_priority

        self.vo = vo
        self.working_group = working_group

        self.retry_number = 0
        self.num_retries = num_retries

        self.poll_panda_jobs_chunk_size = 2000

        self.load_panda_urls()

        self.dependency_tasks = None

    def my_condition(self):
        if self.is_finished():
            return True
        return False

    def load_panda_config(self):
        panda_config = ConfigParser.ConfigParser()
        if os.environ.get('IDDS_PANDA_CONFIG', None):
            configfile = os.environ['IDDS_PANDA_CONFIG']
            if panda_config.read(configfile) == [configfile]:
                return panda_config

        configfiles = ['%s/etc/panda/panda.cfg' % os.environ.get('IDDS_HOME', ''),
                       '/etc/panda/panda.cfg', '/opt/idds/etc/panda/panda.cfg',
                       '%s/etc/panda/panda.cfg' % os.environ.get('VIRTUAL_ENV', '')]
        for configfile in configfiles:
            if panda_config.read(configfile) == [configfile]:
                return panda_config
        return panda_config

    def load_panda_urls(self):
        panda_config = self.load_panda_config()
        # self.logger.debug("panda config: %s" % panda_config)
        self.panda_url = None
        self.panda_url_ssl = None
        self.panda_monitor = None
        self.panda_auth = None
        self.panda_auth_vo = None
        self.panda_config_root = None
        self.pandacache_url = None
        self.panda_verify_host = None

        if panda_config.has_section('panda'):
            if 'PANDA_MONITOR_URL' not in os.environ and panda_config.has_option('panda', 'panda_monitor_url'):
                self.panda_monitor = panda_config.get('panda', 'panda_monitor_url')
                os.environ['PANDA_MONITOR_URL'] = self.panda_monitor
                # self.logger.debug("Panda monitor url: %s" % str(self.panda_monitor))
            if 'PANDA_URL' not in os.environ and panda_config.has_option('panda', 'panda_url'):
                self.panda_url = panda_config.get('panda', 'panda_url')
                os.environ['PANDA_URL'] = self.panda_url
                # self.logger.debug("Panda url: %s" % str(self.panda_url))
            if 'PANDACACHE_URL' not in os.environ and panda_config.has_option('panda', 'pandacache_url'):
                self.pandacache_url = panda_config.get('panda', 'pandacache_url')
                os.environ['PANDACACHE_URL'] = self.pandacache_url
                # self.logger.debug("Pandacache url: %s" % str(self.pandacache_url))
            if 'PANDA_VERIFY_HOST' not in os.environ and panda_config.has_option('panda', 'panda_verify_host'):
                self.panda_verify_host = panda_config.get('panda', 'panda_verify_host')
                os.environ['PANDA_VERIFY_HOST'] = self.panda_verify_host
                # self.logger.debug("Panda verify host: %s" % str(self.panda_verify_host))
            if 'PANDA_URL_SSL' not in os.environ and panda_config.has_option('panda', 'panda_url_ssl'):
                self.panda_url_ssl = panda_config.get('panda', 'panda_url_ssl')
                os.environ['PANDA_URL_SSL'] = self.panda_url_ssl
                # self.logger.debug("Panda url ssl: %s" % str(self.panda_url_ssl))
            if 'PANDA_AUTH' not in os.environ and panda_config.has_option('panda', 'panda_auth'):
                self.panda_auth = panda_config.get('panda', 'panda_auth')
                os.environ['PANDA_AUTH'] = self.panda_auth
            if 'PANDA_AUTH_VO' not in os.environ and panda_config.has_option('panda', 'panda_auth_vo'):
                self.panda_auth_vo = panda_config.get('panda', 'panda_auth_vo')
                os.environ['PANDA_AUTH_VO'] = self.panda_auth_vo
            if 'PANDA_CONFIG_ROOT' not in os.environ and panda_config.has_option('panda', 'panda_config_root'):
                self.panda_config_root = panda_config.get('panda', 'panda_config_root')
                os.environ['PANDA_CONFIG_ROOT'] = self.panda_config_root

    def set_agent_attributes(self, attrs, req_attributes=None):
        if 'life_time' not in attrs[self.class_name] or int(attrs[self.class_name]['life_time']) <= 0:
            attrs['life_time'] = None
        super(DomaPanDAWork, self).set_agent_attributes(attrs)
        if 'num_retries' in self.agent_attributes and self.agent_attributes['num_retries']:
            self.num_retries = int(self.agent_attributes['num_retries'])
        if 'poll_panda_jobs_chunk_size' in self.agent_attributes and self.agent_attributes['poll_panda_jobs_chunk_size']:
            self.poll_panda_jobs_chunk_size = int(self.agent_attributes['poll_panda_jobs_chunk_size'])

    def depend_on(self, work):
        self.logger.debug("checking depending on")
        if self.dependency_tasks is None:
            self.logger.debug("constructing dependency_tasks set")
            dependency_tasks = set([])
            for job in self.dependency_map:
                inputs_dependency = job["dependencies"]

                for input_d in inputs_dependency:
                    task_name = input_d['task']
                    dependency_tasks.add(task_name)
            self.dependency_tasks = list(dependency_tasks)

        if work.task_name in self.dependency_tasks:
            self.logger.debug("finished checking depending on")
            return True
        else:
            self.logger.debug("finished checking depending on")
            return False

    def get_ancestry_works(self):
        tasks = set([])
        for job in self.dependency_map:
            inputs_dependency = job["dependencies"]

            for input_d in inputs_dependency:
                task_name = input_d['task']
                if task_name not in tasks:
                    tasks.add(task_name)
        return list(tasks)

    def poll_external_collection(self, coll):
        try:
            if coll.status in [CollectionStatus.Closed]:
                return coll
            else:
                coll.coll_metadata['bytes'] = 1
                coll.coll_metadata['availability'] = 1
                coll.coll_metadata['events'] = 1
                coll.coll_metadata['is_open'] = True
                coll.coll_metadata['run_number'] = 1
                coll.coll_metadata['did_type'] = 'DATASET'
                coll.coll_metadata['list_all_files'] = False

                # if (not self.dependency_map_deleted and not self.dependency_map):
                if not self.has_new_inputs:
                    coll.coll_metadata['is_open'] = False
                if 'is_open' in coll.coll_metadata and not coll.coll_metadata['is_open']:
                    coll_status = CollectionStatus.Closed
                else:
                    coll_status = CollectionStatus.Open
                coll.status = coll_status
                coll.coll_metadata['coll_type'] = CollectionType.Dataset

                return coll
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_input_collections(self, poll_externel=True):
        """
        *** Function called by Transformer agent.
        """
        colls = [self._primary_input_collection] + self._other_input_collections
        for coll_int_id in colls:
            coll = self.collections[coll_int_id]
            # if self.is_internal_collection(coll):
            #     coll = self.poll_internal_collection(coll)
            # else:
            #     coll = self.poll_external_collection(coll)
            if poll_externel:
                coll = self.poll_external_collection(coll)
            self.collections[coll_int_id] = coll
        return super(DomaPanDAWork, self).get_input_collections()

    def get_mapped_inputs(self, mapped_input_output_maps):
        ret = []
        for map_id in mapped_input_output_maps:
            inputs = mapped_input_output_maps[map_id]['inputs']

            # if 'primary' is not set, the first one is the primary input.
            primary_input = inputs[0]
            for ip in inputs:
                if 'primary' in ip['content_metadata'] and ip['content_metadata']['primary']:
                    primary_input = ip
            ret.append(primary_input)
        return ret

    def get_mapped_outputs(self, mapped_input_output_maps):
        ret = []
        for map_id in mapped_input_output_maps:
            outputs = mapped_input_output_maps[map_id]['outputs']

            # if 'primary' is not set, the first one is the primary input.
            primary_output = outputs[0]
            for ip in outputs:
                if 'primary' in ip['content_metadata'] and ip['content_metadata']['primary']:
                    primary_output = ip
            ret.append(primary_output)
        return ret

    def map_file_to_content(self, coll_id, scope, name):
        content = {'coll_id': coll_id,
                   'scope': scope,
                   'name': name,  # or a different file name from the dataset name
                   'bytes': 1,
                   'adler32': '12345678',
                   'min_id': 0,
                   'max_id': 1,
                   'content_type': ContentType.File,
                   # 'content_relation_type': content_relation_type,
                   # here events is all events for eventservice, not used here.
                   'content_metadata': {'events': 1}}
        return content

    def is_all_dependency_tasks_available(self, inputs_dependency, task_name_to_coll_map):
        for input_d in inputs_dependency:
            task_name = input_d['task']
            if (task_name not in task_name_to_coll_map                    # noqa: W503
               or 'outputs' not in task_name_to_coll_map[task_name]      # noqa: W503
               or not task_name_to_coll_map[task_name]['outputs']):      # noqa: W503
                return False
        return True

    def get_unmapped_jobs(self, mapped_input_output_maps={}):
        mapped_outputs = self.get_mapped_outputs(mapped_input_output_maps)
        mapped_outputs_name = [ip['name'] for ip in mapped_outputs]
        unmapped_jobs = []
        for job in self.dependency_map:
            output_name = job['name']
            if output_name not in mapped_outputs_name:
                unmapped_jobs.append(job)
        return unmapped_jobs

    def has_dependency(self):
        for job in self.dependency_map:
            if "dependencies" in job and job["dependencies"]:
                return True
        return False

    def get_parent_work_names(self):
        parent_work_names = []
        for job in self.dependency_map:
            if "dependencies" in job and job["dependencies"]:
                inputs_dependency = job["dependencies"]
                for input_d in inputs_dependency:
                    task_name = input_d['task']
                    if task_name not in parent_work_names:
                        parent_work_names.append(task_name)
        return parent_work_names

    def get_parent_workload_ids(self):
        parent_workload_ids = []
        parent_work_names = self.get_parent_work_names()
        work_name_to_coll_map = self.get_work_name_to_coll_map()
        for work_name in parent_work_names:
            if work_name in work_name_to_coll_map:
                input_d_coll = work_name_to_coll_map[work_name]['outputs'][0]
                if input_d_coll and 'workload_id' in input_d_coll:
                    parent_workload_ids.append(input_d_coll['workload_id'])
        return parent_workload_ids

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
        *** Function called by Transformer agent.
        New inputs which are not yet mapped to outputs.

        :param mapped_input_output_maps: Inputs that are already mapped.
        """
        new_input_output_maps = {}

        unmapped_jobs = self.get_unmapped_jobs(mapped_input_output_maps)
        if not unmapped_jobs:
            self.set_has_new_inputs(False)
            return new_input_output_maps

        if unmapped_jobs:
            mapped_keys = mapped_input_output_maps.keys()
            if mapped_keys:
                next_key = max(mapped_keys) + 1
            else:
                next_key = 1

            input_coll = self.get_input_collections()[0]
            input_coll_id = input_coll.coll_id
            output_coll = self.get_output_collections()[0]
            output_coll_id = output_coll.coll_id

            task_name_to_coll_map = self.get_work_name_to_coll_map()

            for job in unmapped_jobs:
                output_name = job['name']
                inputs_dependency = job["dependencies"]

                if self.is_all_dependency_tasks_available(inputs_dependency, task_name_to_coll_map):
                    input_content = self.map_file_to_content(input_coll_id, input_coll.scope, output_name)
                    output_content = self.map_file_to_content(output_coll_id, output_coll.scope, output_name)
                    new_input_output_maps[next_key] = {'inputs_dependency': [],
                                                       'logs': [],
                                                       'inputs': [input_content],
                                                       'outputs': [output_content]}
                    for input_d in inputs_dependency:
                        task_name = input_d['task']
                        input_name = input_d['inputname']
                        input_d_coll = task_name_to_coll_map[task_name]['outputs'][0]
                        input_d_content = self.map_file_to_content(input_d_coll['coll_id'], input_d_coll['scope'], input_name)
                        new_input_output_maps[next_key]['inputs_dependency'].append(input_d_content)

                    # all inputs are parsed. move it to dependency_map_deleted
                    # self.dependency_map_deleted.append(job)
                    next_key += 1
                else:
                    # not all inputs for this job can be parsed.
                    # self.dependency_map.append(job)
                    pass

        # self.logger.debug("get_new_input_output_maps, new_input_output_maps: %s" % str(new_input_output_maps))
        self.logger.debug("get_new_input_output_maps, new_input_output_maps len: %s" % len(new_input_output_maps))
        return new_input_output_maps

    def use_dependency_to_release_jobs(self):
        """
        *** Function called by Transformer agent.
        """
        return True

    def get_processing(self, input_output_maps=[], without_creating=False):
        """
        *** Function called by Transformer agent.

        If there is already an active processing for this work, will do nothing.
        If there is no active processings, create_processing will be called.
        """
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            if not without_creating:
                # return None
                return self.create_processing(input_output_maps)
        return None

    def create_processing(self, input_output_maps=[]):
        """
        *** Function called by Transformer agent.

        :param input_output_maps: new maps from inputs to outputs.
        """
        # avoid duplicated task name
        self.task_name = self.task_name + "_" + str(self.get_request_id()) + "_" + str(self.get_work_id())

        in_files = []
        for job in self.dependency_map:
            in_files.append(job['name'])

        task_param_map = {}
        task_param_map['vo'] = self.vo
        if self.task_queue and len(self.task_queue) > 0:
            task_param_map['site'] = self.task_queue
        elif self.queue and len(self.queue) > 0:
            task_param_map['site'] = self.queue
        task_param_map['workingGroup'] = self.working_group
        task_param_map['nFilesPerJob'] = 1
        task_param_map['nFiles'] = len(in_files)
        task_param_map['noInput'] = True
        task_param_map['pfnList'] = in_files
        task_param_map['taskName'] = self.task_name
        task_param_map['userName'] = self.username if self.username else 'iDDS'
        task_param_map['taskPriority'] = self.task_priority
        task_param_map['architecture'] = ''
        task_param_map['transUses'] = ''
        task_param_map['transHome'] = None
        if self.encode_command_line:
            # task_param_map['transPath'] = 'https://atlpan.web.cern.ch/atlpan/bash-c-enc'
            task_param_map['transPath'] = 'https://storage.googleapis.com/drp-us-central1-containers/bash-c-enc'
            task_param_map['encJobParams'] = True
        else:
            # task_param_map['transPath'] = 'https://atlpan.web.cern.ch/atlpan/bash-c'
            task_param_map['transPath'] = 'https://storage.googleapis.com/drp-us-central1-containers/bash-c'
        task_param_map['processingType'] = self.processingType
        task_param_map['prodSourceLabel'] = self.prodSourceLabel
        task_param_map['noWaitParent'] = True
        task_param_map['taskType'] = self.task_type
        task_param_map['coreCount'] = self.core_count
        task_param_map['skipScout'] = True
        task_param_map['cloud'] = self.task_cloud
        task_param_map['PandaSite'] = self.task_site
        if self.task_rss and self.task_rss > 0:
            task_param_map['ramCount'] = self.task_rss
            # task_param_map['ramUnit'] = 'MB'
            task_param_map['ramUnit'] = 'MBPerCoreFixed'

        task_param_map['inputPreStaging'] = True
        task_param_map['prestagingRuleID'] = 123
        task_param_map['nChunksToWait'] = 1
        task_param_map['maxCpuCount'] = self.core_count
        task_param_map['maxWalltime'] = self.maxWalltime
        task_param_map['maxFailure'] = self.maxAttempt if self.maxAttempt else 5
        task_param_map['maxAttempt'] = self.maxAttempt if self.maxAttempt else 5
        task_param_map['log'] = self.task_log
        task_param_map['jobParameters'] = [
            {'type': 'constant',
             'value': self.executable,  # noqa: E501
             },
        ]

        task_param_map['reqID'] = self.get_request_id()

        processing_metadata = {'task_param': task_param_map}
        proc = Processing(processing_metadata=processing_metadata)
        proc.workload_id = None
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc.internal_id)
        return proc

    def submit_panda_task(self, processing):
        try:
            from pandaclient import Client

            proc = processing['processing_metadata']['processing']
            task_param = proc.processing_metadata['task_param']
            if self.has_dependency():
                return_code = Client.insertTaskParams(task_param, verbose=True, parent_tid=self.parent_workload_id)
            else:
                return_code = Client.insertTaskParams(task_param, verbose=True)
            if return_code[0] == 0 and return_code[1][0] is True:
                try:
                    task_id = int(return_code[1][1])
                    return task_id, None
                except Exception as ex:
                    self.logger.warn("task id is not retruned: (%s) is not task id: %s" % (return_code[1][1], str(ex)))
                    # jediTaskID=26468582
                    if return_code[1][1] and 'jediTaskID=' in return_code[1][1]:
                        parts = return_code[1][1].split(" ")
                        for part in parts:
                            if 'jediTaskID=' in part:
                                task_id = int(part.split("=")[1])
                                return task_id, None
                    else:
                        return None, return_code
            else:
                self.logger.warn("submit_panda_task, return_code: %s" % str(return_code))
                return None, return_code
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
            return None, str(ex)
        return None, None

    def submit_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        proc = processing['processing_metadata']['processing']
        if proc.workload_id:
            pass
            return True, proc.workload_id, None
        else:
            task_id, errors = self.submit_panda_task(processing)
            if task_id:
                proc.workload_id = task_id
                proc.submitted_at = datetime.datetime.utcnow()
                return True, task_id, errors
        return False, None, errors

    def get_panda_task_id(self, processing):
        from pandaclient import Client

        start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=10)
        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        status, results = Client.getJobIDsJediTasksInTimeRange(start_time, task_type=self.task_type, verbose=False)
        if status != 0:
            self.logger.warn("Error to poll latest tasks in last ten hours: %s, %s" % (status, results))
            return None

        proc = processing['processing_metadata']['processing']
        task_id = None
        for req_id in results:
            task_name = results[req_id]['taskName']
            local_task_name = proc.task_name
            if not local_task_name:
                local_task_name = self.task_name
            if proc.workload_id is None and task_name == local_task_name:
                task_id = results[req_id]['jediTaskID']
                # processing['processing_metadata']['task_id'] = task_id
                # processing['processing_metadata']['workload_id'] = task_id
                proc.workload_id = task_id
                if task_id:
                    proc.submitted_at = datetime.datetime.utcnow()

        return task_id

    def poll_panda_task_status(self, processing):
        if 'processing' in processing['processing_metadata']:
            from pandaclient import Client

            proc = processing['processing_metadata']['processing']
            status, task_status = Client.getTaskStatus(proc.workload_id)
            if status == 0:
                return task_status
        else:
            return 'failed'
        return None

    def get_processing_status_from_panda_status(self, task_status):
        if task_status in ['registered', 'defined', 'assigning']:
            processing_status = ProcessingStatus.Submitting
        elif task_status in ['ready', 'pending', 'scouting', 'scouted', 'prepared', 'topreprocess', 'preprocessing']:
            processing_status = ProcessingStatus.Submitted
        elif task_status in ['running', 'toretry', 'toincexec', 'throttled']:
            processing_status = ProcessingStatus.Running
        elif task_status in ['done']:
            processing_status = ProcessingStatus.Finished
        elif task_status in ['finished', 'paused']:
            # finished, finishing, waiting it to be done
            processing_status = ProcessingStatus.SubFinished
        elif task_status in ['failed', 'aborted', 'exhausted']:
            # aborting, tobroken
            processing_status = ProcessingStatus.Failed
        elif task_status in ['broken']:
            processing_status = ProcessingStatus.Broken
        else:
            # finished, finishing, aborting, topreprocess, preprocessing, tobroken
            # toretry, toincexec, rerefine, paused, throttled, passed
            processing_status = ProcessingStatus.Submitted
        return processing_status

    def is_all_contents_terminated_and_with_missing(self, input_output_maps):
        with_missing = False
        for map_id in input_output_maps:
            outputs = input_output_maps[map_id]['outputs']
            for content in outputs:
                if not content['status'] in [ContentStatus.Failed, ContentStatus.FinalFailed,
                                             ContentStatus.Lost, ContentStatus.Deleted,
                                             ContentStatus.Missing]:
                    return False
                if not with_missing and content['status'] in [ContentStatus.Missing]:
                    with_missing = True
        if with_missing:
            return True
        return False

    def reactive_contents(self, input_output_maps):
        updated_contents = []
        for map_id in input_output_maps:
            inputs = input_output_maps[map_id]['inputs'] if 'inputs' in input_output_maps[map_id] else []
            outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
            inputs_dependency = input_output_maps[map_id]['inputs_dependency'] if 'inputs_dependency' in input_output_maps[map_id] else []

            all_outputs_available = True
            for content in outputs:
                if not content['status'] in [ContentStatus.Available]:
                    all_outputs_available = False
                    break

            if not all_outputs_available:
                for content in inputs + outputs:
                    update_content = {'content_id': content['content_id'],
                                      'status': ContentStatus.New,
                                      'substatus': ContentStatus.New}
                    updated_contents.append(update_content)
                for content in inputs_dependency:
                    if content['status'] not in [ContentStatus.Available]:
                        update_content = {'content_id': content['content_id'],
                                          'status': ContentStatus.New,
                                          'substatus': ContentStatus.New}
                        updated_contents.append(update_content)
        return updated_contents

    def sort_panda_jobids(self, input_output_maps):
        panda_job_ids = {}
        panda_id_to_map_ids = {}
        map_id_without_panda_ids = []
        for map_id in input_output_maps:
            outputs = input_output_maps[map_id]['outputs']
            for content in outputs:
                if content['status'] not in panda_job_ids:
                    panda_job_ids[content['status']] = []

                if 'panda_id' in content['content_metadata']:
                    panda_job_ids[content['status']].append(content['content_metadata']['panda_id'])
                    panda_id_to_map_ids[content['content_metadata']['panda_id']] = map_id
                else:
                    map_id_without_panda_ids.append(map_id)

        return panda_job_ids, map_id_without_panda_ids, panda_id_to_map_ids

    def get_registered_panda_jobids(self, input_output_maps):
        panda_job_ids, map_id_without_panda_ids, panda_id_to_map_ids = self.sort_panda_jobids(input_output_maps)
        unterminated_panda_ids = []
        finished_panda_ids = []
        failed_panda_ids = []
        for key in panda_job_ids:
            if key in [ContentStatus.Available]:
                finished_panda_ids += panda_job_ids[key]
            elif key in [ContentStatus.Failed, ContentStatus.FinalFailed,
                         ContentStatus.Lost, ContentStatus.Deleted,
                         ContentStatus.Missing]:
                failed_panda_ids += panda_job_ids[key]
            else:
                unterminated_panda_ids += panda_job_ids[key]
        return finished_panda_ids + failed_panda_ids, unterminated_panda_ids, map_id_without_panda_ids, panda_id_to_map_ids

    def get_map_id_from_input(self, input_output_maps, input_file):
        map_keys = list(input_output_maps.keys())
        map_keys.reverse()
        for map_id in map_keys:
            inputs = input_output_maps[map_id]['inputs']
            # outputs = input_output_maps[map_id]['outputs']
            for content in inputs:
                if content['name'] == input_file:
                    return map_id
        return None

    def get_content_status_from_panda_status(self, job_info):
        if job_info is None:
            return ContentStatus.Processing

        jobstatus = job_info.jobStatus
        if jobstatus in ['finished', 'merging']:
            return ContentStatus.Available
        elif jobstatus in ['failed', 'closed', 'cancelled', 'lost', 'broken', 'missing']:
            attempt_nr = int(job_info.attemptNr) if job_info.attemptNr else 0
            max_attempt = int(job_info.maxAttempt) if job_info.maxAttempt else 0
            if (attempt_nr >= max_attempt) and (attempt_nr >= self.maxAttempt):
                return ContentStatus.FinalFailed
            else:
                return ContentStatus.Failed
        else:
            return ContentStatus.Processing

    def get_update_contents_from_map_id(self, map_id, input_output_maps, job_info):
        outputs = input_output_maps[map_id]['outputs']
        update_contents = []
        for content in outputs:
            status = self.get_content_status_from_panda_status(job_info)
            content['substatus'] = status

            if 'panda_id' in content['content_metadata'] and content['content_metadata']['panda_id']:
                # if content['content_metadata']['panda_id'] != job_info.PandaID:
                if content['content_metadata']['panda_id'] < job_info.PandaID:
                    # new panda id is the bigger one.
                    if 'old_panda_id' not in content['content_metadata']:
                        content['content_metadata']['old_panda_id'] = []
                    if content['content_metadata']['panda_id'] not in content['content_metadata']['old_panda_id']:
                        content['content_metadata']['old_panda_id'].append(content['content_metadata']['panda_id'])
            content['content_metadata']['panda_id'] = job_info.PandaID

            update_contents.append(content)
        return update_contents

    def get_panda_job_status(self, jobids):
        self.logger.debug("get_panda_job_status, jobids[:10]: %s" % str(jobids[:10]))
        from pandaclient import Client
        ret = Client.getJobStatus(jobids, verbose=0)
        if ret[0] == 0:
            left_jobids = []
            ret_jobs = []
            jobs_list = ret[1]
            for jobid, jobinfo in zip(jobids, jobs_list):
                if jobinfo is None:
                    left_jobids.append(jobid)
                else:
                    ret_jobs.append(jobinfo)
            if left_jobids:
                ret1 = Client.getFullJobStatus(ids=left_jobids, verbose=False)
                if ret1[0] == 0:
                    left_jobs_list = ret1[1]
                ret_jobs = ret_jobs + left_jobs_list
            return ret_jobs
        return []

    def map_panda_ids(self, unregistered_job_ids, input_output_maps):
        self.logger.debug("map_panda_ids, unregistered_job_ids[:10]: %s" % str(unregistered_job_ids[:10]))

        # updated_map_ids = []
        full_update_contents = []
        chunksize = 2000
        chunks = [unregistered_job_ids[i:i + chunksize] for i in range(0, len(unregistered_job_ids), chunksize)]
        for chunk in chunks:
            # jobs_list = Client.getJobStatus(chunk, verbose=0)[1]
            jobs_list = self.get_panda_job_status(chunk)
            for job_info in jobs_list:
                if job_info and job_info.Files and len(job_info.Files) > 0:
                    for job_file in job_info.Files:
                        # if job_file.type in ['log']:
                        if job_file.type not in ['pseudo_input']:
                            continue
                        if ':' in job_file.lfn:
                            pos = job_file.lfn.find(":")
                            input_file = job_file.lfn[pos + 1:]
                            # input_file = job_file.lfn.split(':')[1]
                        else:
                            input_file = job_file.lfn
                        map_id = self.get_map_id_from_input(input_output_maps, input_file)
                        if map_id:
                            update_contents = self.get_update_contents_from_map_id(map_id, input_output_maps, job_info)
                            full_update_contents += update_contents
        return full_update_contents

    def get_status_changed_contents(self, unterminated_job_ids, input_output_maps, panda_id_to_map_ids):
        self.logger.debug("get_status_changed_contents, unterminated_job_ids[:10]: %s" % str(unterminated_job_ids[:10]))

        full_update_contents = []
        chunksize = 2000
        chunks = [unterminated_job_ids[i:i + chunksize] for i in range(0, len(unterminated_job_ids), chunksize)]
        for chunk in chunks:
            # jobs_list = Client.getJobStatus(chunk, verbose=0)[1]
            jobs_list = self.get_panda_job_status(chunk)
            for job_info in jobs_list:
                panda_id = job_info.PandaID
                map_id = panda_id_to_map_ids[panda_id]
                update_contents = self.get_update_contents_from_map_id(map_id, input_output_maps, job_info)
                full_update_contents += update_contents
        return full_update_contents

    def get_final_update_contents(self, input_output_maps):
        update_contents = []
        for map_id in input_output_maps:
            outputs = input_output_maps[map_id]['outputs'] if 'outputs' in input_output_maps[map_id] else []
            for content in outputs:
                if (content['substatus'] not in [ContentStatus.Available, ContentStatus.FakeAvailable, ContentStatus.FinalFailed]):
                    content['content_metadata']['old_final_status'] = content['substatus']
                    content['substatus'] = ContentStatus.FinalFailed
                    update_contents.append(content)

        return update_contents

    def poll_panda_jobs(self, job_ids):
        job_ids = list(job_ids)
        self.logger.debug("poll_panda_jobs, poll_panda_jobs_chunk_size: %s, job_ids[:10]: %s" % (self.poll_panda_jobs_chunk_size, str(job_ids[:10])))

        # updated_map_ids = []
        inputname_jobid_map = {}
        chunksize = self.poll_panda_jobs_chunk_size
        chunks = [job_ids[i:i + chunksize] for i in range(0, len(job_ids), chunksize)]
        for chunk in chunks:
            # jobs_list = Client.getJobStatus(chunk, verbose=0)[1]
            jobs_list = self.get_panda_job_status(chunk)
            if jobs_list:
                self.logger.debug("poll_panda_jobs, input jobs: %s, output_jobs: %s" % (len(chunk), len(jobs_list)))
                for job_info in jobs_list:
                    job_status = self.get_content_status_from_panda_status(job_info)
                    if job_info and job_info.Files and len(job_info.Files) > 0:
                        for job_file in job_info.Files:
                            # if job_file.type in ['log']:
                            if job_file.type not in ['pseudo_input']:
                                continue
                            if ':' in job_file.lfn:
                                pos = job_file.lfn.find(":")
                                input_file = job_file.lfn[pos + 1:]
                                # input_file = job_file.lfn.split(':')[1]
                            else:
                                input_file = job_file.lfn
                            inputname_jobid_map[input_file] = {'panda_id': job_info.PandaID, 'status': job_status}
            else:
                self.logger.warn("poll_panda_jobs, input jobs: %s, output_jobs: %s" % (len(chunk), jobs_list))
        return inputname_jobid_map

    def get_job_maps(self, input_output_maps):
        inputname_mapid_map = {}
        finished_jobs, failed_jobs = [], []
        for map_id in input_output_maps:
            inputs = input_output_maps[map_id]['inputs']
            outputs = input_output_maps[map_id]['outputs']
            for content in outputs:
                if content['substatus'] in [ContentStatus.Available]:
                    if 'panda_id' in content['content_metadata']:
                        finished_jobs.append(content['content_metadata']['panda_id'])
                elif content['substatus'] in [ContentStatus.Failed, ContentStatus.FinalFailed,
                                              ContentStatus.Lost, ContentStatus.Deleted,
                                              ContentStatus.Missing]:
                    if 'panda_id' in content['content_metadata']:
                        failed_jobs.append(content['content_metadata']['panda_id'])
            for content in inputs:
                inputname_mapid_map[content['name']] = {'map_id': map_id,
                                                        'outputs': outputs}
        return finished_jobs + failed_jobs, inputname_mapid_map

    def get_update_contents(self, inputnames, inputname_mapid_map, inputname_jobid_map):
        self.logger.debug("get_update_contents, inputnames[:5]: %s" % str(inputnames[:5]))
        # self.logger.debug("get_update_contents, inputname_mapid_map[:5]: %s" % str({k: inputname_mapid_map[k] for k in inputnames[:5]}))
        self.logger.debug("get_update_contents, inputname_jobid_map[:3]: %s" % str({k: inputname_jobid_map[k] for k in inputnames[:3]}))

        update_contents = []
        update_contents_full = []
        num_updated_contents, num_unupdated_contents = 0, 0
        for inputname in inputnames:
            panda_id_status = inputname_jobid_map[inputname]
            panda_id = panda_id_status['panda_id']
            panda_status = panda_id_status['status']
            map_id_contents = inputname_mapid_map[inputname]
            contents = map_id_contents['outputs']
            for content in contents:
                if content['substatus'] != panda_status:
                    # content['status'] = panda_status
                    content['substatus'] = panda_status
                    update_contents_full.append(content)
                    update_content = {'content_id': content['content_id'],
                                      # 'status': panda_status,
                                      'substatus': panda_status}
                    # 'content_metadata': content['content_metadata']
                    if 'panda_id' in content['content_metadata'] and content['content_metadata']['panda_id']:
                        # if content['content_metadata']['panda_id'] != job_info.PandaID:
                        if content['content_metadata']['panda_id'] < panda_id:
                            # new panda id is the bigger one.
                            if 'old_panda_id' not in content['content_metadata']:
                                content['content_metadata']['old_panda_id'] = []
                            if content['content_metadata']['panda_id'] not in content['content_metadata']['old_panda_id']:
                                content['content_metadata']['old_panda_id'].append(content['content_metadata']['panda_id'])
                            content['content_metadata']['panda_id'] = panda_id
                            # content['status'] = panda_status
                            content['substatus'] = panda_status
                            update_content['content_metadata'] = content['content_metadata']
                        elif content['content_metadata']['panda_id'] > panda_id:
                            if 'old_panda_id' not in content['content_metadata']:
                                content['content_metadata']['old_panda_id'] = []
                            if panda_id not in content['content_metadata']['old_panda_id']:
                                content['content_metadata']['old_panda_id'].append(panda_id)
                            # content['content_metadata']['panda_id'] = content['content_metadata']['panda_id']
                            # content['substatus'] = panda_status
                            update_content['content_metadata'] = content['content_metadata']
                        else:
                            pass
                            # content['content_metadata']['panda_id'] = panda_id
                            content['substatus'] = panda_status
                    else:
                        content['content_metadata']['panda_id'] = panda_id
                        content['substatus'] = panda_status
                        update_content['content_metadata'] = content['content_metadata']

                    update_contents.append(update_content)
                    num_updated_contents += 1
                else:
                    # num_unupdated_contents += 1
                    pass

        self.logger.debug("get_update_contents, num_updated_contents: %s, num_unupdated_contents: %s" % (num_updated_contents, num_unupdated_contents))
        self.logger.debug("get_update_contents, update_contents[:3]: %s" % (str(update_contents[:3])))
        return update_contents, update_contents_full

    def poll_panda_task(self, processing=None, input_output_maps=None, log_prefix=''):
        task_id = None
        try:
            from pandaclient import Client

            if processing:
                proc = processing['processing_metadata']['processing']
                task_id = proc.workload_id
                if task_id is None:
                    task_id = self.get_panda_task_id(processing)

                if task_id:
                    # ret_ids = Client.getPandaIDsWithTaskID(task_id, verbose=False)
                    self.logger.debug(log_prefix + "poll_panda_task, task_id: %s" % str(task_id))
                    task_info = Client.getJediTaskDetails({'jediTaskID': task_id}, True, True, verbose=True)
                    self.logger.debug(log_prefix + "poll_panda_task, task_info[0]: %s" % str(task_info[0]))
                    if task_info[0] != 0:
                        self.logger.warn(log_prefix + "poll_panda_task %s, error getting task status, task_info: %s" % (task_id, str(task_info)))
                        return ProcessingStatus.Running, [], []

                    task_info = task_info[1]

                    processing_status = self.get_processing_status_from_panda_status(task_info["status"])
                    self.logger.info(log_prefix + "poll_panda_task processing_status: %s" % processing_status)

                    all_jobs_ids = task_info['PandaID']

                    terminated_jobs, inputname_mapid_map = self.get_job_maps(input_output_maps)
                    self.logger.debug(log_prefix + "poll_panda_task, task_id: %s, all jobs: %s, terminated_jobs: %s" % (str(task_id), len(all_jobs_ids), len(terminated_jobs)))

                    all_jobs_ids = set(all_jobs_ids)
                    terminated_jobs = set(terminated_jobs)
                    unterminated_jobs = all_jobs_ids - terminated_jobs

                    inputname_jobid_map = self.poll_panda_jobs(unterminated_jobs)
                    intersection_keys = set(inputname_mapid_map.keys()) & set(inputname_jobid_map.keys())

                    updated_contents, update_contents_full = self.get_update_contents(list(intersection_keys),
                                                                                      inputname_mapid_map,
                                                                                      inputname_jobid_map)

                    return processing_status, updated_contents, update_contents_full
                else:
                    return ProcessingStatus.Running, [], []
        except Exception as ex:
            msg = "Failed to check the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            self.logger.error(log_prefix + msg)
            self.logger.error(log_prefix + str(ex))
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException(msg)
        return ProcessingStatus.Running, [], []

    def kill_processing(self, processing, log_prefix=''):
        try:
            if processing:
                from pandaclient import Client
                proc = processing['processing_metadata']['processing']
                task_id = proc.workload_id
                # task_id = processing['processing_metadata']['task_id']
                # Client.killTask(task_id)
                Client.finishTask(task_id, soft=False)
                self.logger.info(log_prefix + "finishTask: %s" % task_id)
        except Exception as ex:
            msg = "Failed to kill the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            # raise exceptions.IDDSException(msg)
            self.logger.error(log_prefix + "Failed to finishTask: %s, %s" % (task_id, msg))

    def kill_processing_force(self, processing, log_prefix=''):
        try:
            if processing:
                from pandaclient import Client
                proc = processing['processing_metadata']['processing']
                task_id = proc.workload_id
                # task_id = processing['processing_metadata']['task_id']
                Client.killTask(task_id)
                # Client.finishTask(task_id, soft=True)
                self.logger.info(log_prefix + "killTask: %s" % task_id)
        except Exception as ex:
            msg = "Failed to force kill the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            # raise exceptions.IDDSException(msg)
            self.logger.error(log_prefix + "Failed to force kill: %s, %s" % (task_id, msg))

    def reactivate_processing(self, processing, log_prefix=''):
        try:
            if processing:
                from pandaclient import Client
                # task_id = processing['processing_metadata']['task_id']
                proc = processing['processing_metadata']['processing']
                task_id = proc.workload_id

                # Client.retryTask(task_id)
                status, out = Client.retryTask(task_id, newParams={})
                self.logger.warn(log_prefix + "Resume processing(%s) with task id(%s): %s, %s" % (processing['processing_id'], task_id, status, out))
                # Client.reactivateTask(task_id)
                # Client.resumeTask(task_id)
        except Exception as ex:
            msg = "Failed to resume the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            # raise exceptions.IDDSException(msg)
            self.logger.error(log_prefix + msg)

    def abort_processing(self, processing, log_prefix=''):
        self.kill_processing_force(processing, log_prefix=log_prefix)

    def resume_processing(self, processing, log_prefix=''):
        self.reactivate_processing(processing, log_prefix=log_prefix)

    def poll_processing_updates(self, processing, input_output_maps, contents_ext=None, log_prefix=''):
        """
        *** Function called by Carrier agent.
        """
        update_contents = []
        update_contents_full = []
        self.logger.debug(log_prefix + "poll_processing_updates, input_output_maps.keys[:3]: %s" % str(list(input_output_maps.keys())[:3]))

        if processing:
            proc = processing['processing_metadata']['processing']

            processing_status, update_contents, update_contents_full = self.poll_panda_task(processing=processing,
                                                                                            input_output_maps=input_output_maps,
                                                                                            log_prefix=log_prefix)
            # self.logger.debug(log_prefix + "poll_processing_updates, processing_status: %s" % str(processing_status))
            # self.logger.debug(log_prefix + "poll_processing_updates, update_contents[:10]: %s" % str(update_contents[:10]))

            if update_contents:
                proc.has_new_updates()
        return processing_status, update_contents, {}, update_contents_full, {}, [], []

    def get_status_statistics(self, registered_input_output_maps):
        status_statistics = {}
        for map_id in registered_input_output_maps:
            outputs = registered_input_output_maps[map_id]['outputs']

            for content in outputs:
                if content['status'].name not in status_statistics:
                    status_statistics[content['status'].name] = 0
                status_statistics[content['status'].name] += 1
        self.status_statistics = status_statistics
        self.logger.debug("registered_input_output_maps, status_statistics: %s" % str(status_statistics))
        return status_statistics

    def syn_work_status(self, registered_input_output_maps, all_updates_flushed=True, output_statistics={}, to_release_input_contents=[]):
        super(DomaPanDAWork, self).syn_work_status(registered_input_output_maps, all_updates_flushed, output_statistics, to_release_input_contents)
        # self.get_status_statistics(registered_input_output_maps)
        self.status_statistics = output_statistics

        self.logger.debug("syn_work_status, self.active_processings: %s" % str(self.active_processings))
        self.logger.debug("syn_work_status, self.has_new_inputs(): %s" % str(self.has_new_inputs))
        self.logger.debug("syn_work_status, coll_metadata_is_open: %s" %
                          str(self.collections[self._primary_input_collection].coll_metadata['is_open']))
        self.logger.debug("syn_work_status, primary_input_collection_status: %s" %
                          str(self.collections[self._primary_input_collection].status))

        self.logger.debug("syn_work_status(%s): is_processings_terminated: %s" % (str(self.get_processing_ids()), str(self.is_processings_terminated())))
        self.logger.debug("syn_work_status(%s): is_input_collections_closed: %s" % (str(self.get_processing_ids()), str(self.is_input_collections_closed())))
        self.logger.debug("syn_work_status(%s): has_new_inputs: %s" % (str(self.get_processing_ids()), str(self.has_new_inputs)))
        self.logger.debug("syn_work_status(%s): has_to_release_inputs: %s" % (str(self.get_processing_ids()), str(self.has_to_release_inputs())))
        self.logger.debug("syn_work_status(%s): to_release_input_contents: %s" % (str(self.get_processing_ids()), str(to_release_input_contents)))

        # if self.is_processings_terminated() and self.is_input_collections_closed() and not self.has_new_inputs and not self.has_to_release_inputs() and not to_release_input_contents:
        if self.is_processings_terminated():
            # if not self.is_all_outputs_flushed(registered_input_output_maps):
            if not all_updates_flushed:
                self.logger.warn("The work processings %s is terminated. but not all outputs are flushed. Wait to flush the outputs then finish the transform" % str(self.get_processing_ids()))
                return

            if self.is_processings_finished():
                self.status = WorkStatus.Finished
            elif self.is_processings_subfinished():
                self.status = WorkStatus.SubFinished
            elif self.is_processings_failed():
                self.status = WorkStatus.Failed
            elif self.is_processings_expired():
                self.status = WorkStatus.Expired
            elif self.is_processings_cancelled():
                self.status = WorkStatus.Cancelled
            elif self.is_processings_suspended():
                self.status = WorkStatus.Suspended
        elif self.is_processings_running():
            self.status = WorkStatus.Running
        else:
            self.status = WorkStatus.Transforming
