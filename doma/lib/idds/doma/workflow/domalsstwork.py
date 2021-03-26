#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020
# - Sergey Padolski, <spadolski@bnl.gov>, 2020


try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

import copy
import json
import os
import traceback
import uuid
import urllib
import ssl
import datetime

from pandatools import Client

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionType, CollectionStatus,
                                   ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
from idds.workflow.work import Work
from idds.workflow.workflow import Condition
import logging

DEBUG = False
CACHE_TIMEOUT = 30 * 60


class DomaCondition(Condition):
    def __init__(self, cond=None, current_work=None, true_work=None, false_work=None):
        super(DomaCondition, self).__init__(cond=cond, current_work=current_work,
                                            true_work=true_work, false_work=false_work)


class DomaLSSTWork(Work):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='lsst', exec_type='panda', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None, dependency_map=None, task_name="", task_queue=None, processing_type=None,
                 maxwalltime=90000, maxattempt=5, core_count=1):

        super(DomaLSSTWork, self).__init__(executable=executable, arguments=arguments,
                                           parameters=parameters, setup=setup, work_type=TransformType.Processing,
                                           work_tag=work_tag, exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                           primary_input_collection=primary_input_collection,
                                           other_input_collections=other_input_collections,
                                           output_collections=output_collections,
                                           log_collections=log_collections,
                                           release_inputs_after_submitting=True,
                                           logger=logger)
        self.pandamonitor = None
        self.dependency_map = dependency_map
        self.logger.setLevel(logging.DEBUG)
        self.task_name = task_name
        self.queue = task_queue
        self.dep_tasks_id_names_map = {}
        self.executable = executable
        self.jobs_cache = {}
        self.processingType = processing_type
        self.maxWalltime = maxwalltime
        self.maxAttempt = maxattempt
        self.core_count = core_count

    def my_condition(self):
        if self.is_finished():
            return True
        return False

    def put_into_cache(self, input_index, status, job_id):
        if status == ContentStatus.Available:
            validity = datetime.datetime.now(tz=None) + datetime.timedelta(days=365)
        else:
            validity = datetime.datetime.now(tz=None) + datetime.timedelta(seconds=CACHE_TIMEOUT)
        self.jobs_cache[job_id] = {'status': status, 'validity': validity, 'input_index': input_index}

    def get_from_cache(self, job_id):
        obj = self.jobs_cache.get(job_id, None)
        if obj:
            if obj['validity'] > datetime.datetime.now(tz=None):
                return obj['input_index'], obj['status']
        return None

    def jobs_to_idd_ds_status(self, jobstatus):
        if jobstatus == 'finished':
            return ContentStatus.Available
        elif jobstatus == 'failed':
            return ContentStatus.Failed
        else:
            return ContentStatus.Processing

    def load_panda_config(self):
        panda_config = ConfigParser.SafeConfigParser()
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

    def load_panda_monitor(self):
        panda_config = self.load_panda_config()
        self.logger.info("panda config: %s" % panda_config)
        if panda_config.has_section('panda'):
            if panda_config.has_option('panda', 'pandamonitor'):
                pandamonitor = panda_config.get('panda', 'pandamonitor')
                return pandamonitor
        return None

    def poll_external_collection(self, coll):
        try:
            if 'status' in coll and coll['status'] in [CollectionStatus.Closed]:
                return coll
            else:
                if 'coll_metadata' not in coll:
                    coll['coll_metadata'] = {}
                coll['coll_metadata']['bytes'] = 1
                coll['coll_metadata']['availability'] = 1
                coll['coll_metadata']['events'] = 1
                coll['coll_metadata']['is_open'] = False
                coll['coll_metadata']['run_number'] = 1
                coll['coll_metadata']['did_type'] = 'DATASET'
                coll['coll_metadata']['list_all_files'] = False

                if 'is_open' in coll['coll_metadata'] and not coll['coll_metadata']['is_open']:
                    coll_status = CollectionStatus.Closed
                else:
                    coll_status = CollectionStatus.Open
                coll['status'] = coll_status
                coll['coll_type'] = CollectionType.Dataset

                return coll
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_input_collections(self):
        """
        *** Function called by Transformer agent.
        """
        colls = [self.primary_input_collection] + self.other_input_collections
        for coll_int_id in colls:
            coll = self.collections[coll_int_id]
            coll = self.poll_external_collection(coll)
            self.collections[coll_int_id] = coll
        return super(DomaLSSTWork, self).get_input_collections()

    def get_unsubmitted_inputs(self):
        not_submitted_inputs = filter(lambda t: not t["submitted"], self.dependency_map)
        tasks_to_check = []
        for job in not_submitted_inputs:
            tasks_to_check.extend([(input["task"], input["inputname"])
                                   for input in job["dependencies"] if not input["available"]])
        tasks_to_check_compact = {}
        for task in tasks_to_check:
            tasks_to_check_compact.setdefault(task[0], set()).add(task[1])
        return tasks_to_check_compact

    def set_dependency_input_available(self, taskname, available_inputs):
        for job in self.dependency_map:
            for dependency in job["dependencies"]:
                if dependency["task"] == taskname and dependency["inputname"] in available_inputs:
                    dependency["available"] = True

    def update_dependencies(self):
        tasks_to_check = self.get_unsubmitted_inputs()
        for task, inputs in tasks_to_check.items():
            _, outputs = self.poll_panda_task(task_name=task)
            available_inputs = set(list(dict(filter(lambda t: t[1] == ContentStatus.Available,
                                                    outputs.items())).keys()))
            self.set_dependency_input_available(task, available_inputs)

    def get_ready_inputs(self):
        not_submitted_inputs = filter(lambda j: not j["submitted"], self.dependency_map)
        files_to_submit = []
        for job in not_submitted_inputs:
            unresolved_deps = [input for input in job["dependencies"] if not input["available"]]
            if len(unresolved_deps) == 0:
                files_to_submit.append(job["name"])
        return files_to_submit

    def check_dependencies(self):
        self.update_dependencies()
        return self.get_ready_inputs()

    def can_close(self):
        not_submitted_inputs = list(filter(lambda t: not t["submitted"], self.dependency_map))
        if len(not_submitted_inputs) == 0:
            return True
        else:
            return False

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        try:
            ret_files = []
            coll = self.collections[self.primary_input_collection]
            for file in self.dependency_map:
                ret_file = {'coll_id': coll['coll_id'] if not DEBUG else None,
                            'scope': coll['scope'],
                            'name': file['name'],  # or a different file name from the dataset name
                            'bytes': 1,
                            'adler32': '12345678',
                            'min_id': 0,
                            'max_id': 1,
                            'content_type': ContentType.File,
                            # here events is all events for eventservice, not used here.
                            'content_metadata': {'events': 1}}
                ret_files.append(ret_file)
            return ret_files
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

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

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
        *** Function called by Transformer agent.
        New inputs which are not yet mapped to outputs.

        :param mapped_input_output_maps: Inputs that are already mapped.
        """
        inputs = self.get_input_contents()
        mapped_inputs = self.get_mapped_inputs(mapped_input_output_maps)
        mapped_inputs_scope_name = [ip['name'] for ip in mapped_inputs]

        new_inputs = []
        new_input_output_maps = {}
        for ip in inputs:
            ip_scope_name = ip['name']
            if ip_scope_name not in mapped_inputs_scope_name:
                new_inputs.append(ip)

        # to avoid cheking new inputs if there are no new inputs anymore
        if not new_inputs and self.collections[self.primary_input_collection]['status'] in [CollectionStatus.Closed]:
            self.set_has_new_inputs(False)
        else:
            mapped_keys = mapped_input_output_maps.keys()
            if mapped_keys:
                next_key = max(mapped_keys) + 1
            else:
                next_key = 1
            for ip in new_inputs:
                out_ip = copy.deepcopy(ip)
                if not DEBUG:
                    out_ip['coll_id'] = self.collections[self.output_collections[0]]['coll_id']
                new_input_output_maps[next_key] = {'inputs': [ip],
                                                   'outputs': [out_ip]}
                next_key += 1
        self.logger.debug("get_new_input_output_maps, new_input_output_maps: %s" % str(new_input_output_maps))
        self.collections[self.primary_input_collection]['coll_metadata']['is_open'] = False
        self.collections[self.primary_input_collection]['status'] = CollectionStatus.Closed
        return new_input_output_maps

    def get_processing(self, input_output_maps):
        """
        *** Function called by Transformer agent.

        If there is already an active processing for this work, will do nothing.
        If there is no active processings, create_processing will be called.
        """
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            return None

    def create_processing(self, input_output_maps):
        """
        *** Function called by Transformer agent.

        :param input_output_maps: new maps from inputs to outputs.
        """
        in_files = []
        for map_id in input_output_maps:
            # one map is a job which transform the inputs to outputs.
            inputs = input_output_maps[map_id]['inputs']
            # outputs = input_output_maps[map_id]['outputs']
            for ip in inputs:
                in_files.append(ip['name'])

        task_param_map = {}
        task_param_map['vo'] = 'wlcg'
        task_param_map['site'] = self.queue
        task_param_map['workingGroup'] = 'lsst'
        task_param_map['nFilesPerJob'] = 1
        task_param_map['nFiles'] = len(in_files)
        task_param_map['noInput'] = True
        task_param_map['pfnList'] = in_files
        task_param_map['taskName'] = self.task_name
        task_param_map['userName'] = 'Siarhei Padolski'
        task_param_map['taskPriority'] = 900
        task_param_map['architecture'] = ''
        task_param_map['transUses'] = ''
        task_param_map['transHome'] = None
        task_param_map['transPath'] = 'https://atlpan.web.cern.ch/atlpan/bash-c'
        task_param_map['processingType'] = self.processingType
        task_param_map['prodSourceLabel'] = 'test'
        task_param_map['taskType'] = 'test'
        task_param_map['coreCount'] = self.core_count
        task_param_map['skipScout'] = True
        task_param_map['cloud'] = 'US'
        task_param_map['inputPreStaging'] = True
        task_param_map['prestagingRuleID'] = 123
        task_param_map['nChunksToWait'] = 1
        task_param_map['maxCpuCount'] = self.maxWalltime
        task_param_map['maxWalltime'] = self.maxWalltime
        task_param_map['maxFailure'] = self.maxAttempt
        task_param_map['maxAttempt'] = self.maxAttempt
        task_param_map['jobParameters'] = [
            {'type': 'constant',
             'value': self.executable,  # noqa: E501
             },
        ]

        proc = {'processing_metadata': {'internal_id': str(uuid.uuid1()),
                                        'task_id': None,
                                        'task_param': task_param_map}}
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc['processing_metadata']['internal_id'])
        return proc

    def submit_panda_task(self, processing):
        try:
            task_param = processing['processing_metadata']['task_param']
            return_code = Client.insertTaskParams(task_param, verbose=True)
            if return_code[0] == 0:
                return return_code[1][1]
            else:
                self.logger.error("submit_panda_task, return_code: %s" % str(return_code))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
        return None

    def submit_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        if 'task_id' in processing['processing_metadata'] and processing['processing_metadata']['task_id']:
            pass
        else:
            task_id = self.submit_panda_task(processing)
            processing['processing_metadata']['task_id'] = task_id
            processing['processing_metadata']['workload_id'] = task_id

    def download_payload_json(self, task_url):
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(task_url)
            response = urllib.request.urlopen(req, timeout=180, context=ctx).read()
            response = json.loads(response)
        except Exception or urllib.request.error as e:
            raise e
        return response

    def jedi_semaphore(self, processing):
        task_id = processing['processing_metadata']['task_id']
        task_info = Client.getJediTaskDetails({'jediTaskID': task_id}, True, True)
        if task_info[0] != 0:
            return False
        task_info = task_info[1]
        jobs_statistics = task_info.get('statistics', "")
        if len(jobs_statistics) == 0:
            return False
        return True

    def poll_panda_task(self, processing=None, task_name=None):
        task_id = None
        try:
            if not self.pandamonitor:
                self.pandamonitor = self.load_panda_monitor()
                self.logger.info("panda server: %s" % self.pandamonitor)
            task_info = None
            jobs_ids = None
            if processing:
                task_id = processing['processing_metadata']['task_id']
            elif task_name:
                task_id = self.dep_tasks_id_names_map.get(task_name, None)

            if task_id:
                task_info = Client.getJediTaskDetails({'jediTaskID': task_id}, True, True)
                if task_info[0] != 0:
                    self.logger.error("poll_panda_task, error getting task status, task_info: %s" % str(task_info))
                    return "No status", {}
                task_info = task_info[1]
                jobs_ids = task_info['PandaID']
            elif task_name:
                task_url = self.pandamonitor + '/tasks/?taskname=' + str(task_name) + "&status=!aborted&json&days=180"
                self.logger.debug("poll_panda_task, task_url: %s" % str(task_url))
                task_json = self.download_payload_json(task_url)
                if len(task_json) > 0:
                    task_info = task_json[0]
                    self.dep_tasks_id_names_map[task_name] = task_info.get('jeditaskid', None)
                else:
                    return "No status", {}
                task_id = task_info.get('jeditaskid', None)
            if not task_id:
                return "No status", {}
            if not jobs_ids:
                jobs_ids = Client.getPandaIDsWithTaskID(task_id)[1]

            outputs_status = {}
            if len(jobs_ids) > 0:
                not_cached_jobs_ids = []

                for job_id in jobs_ids:
                    obj = self.get_from_cache(job_id)
                    if obj:
                        if obj[0] not in outputs_status or outputs_status[obj[0]] != ContentStatus.Available:
                            outputs_status[obj[0]] = obj[1]
                    else:
                        not_cached_jobs_ids.append(job_id)

                self.logger.debug("poll_panda_task, not_cached_jobs_ids: %s" % str(not_cached_jobs_ids))
                chunksize = 2000
                chunks = [not_cached_jobs_ids[i:i + chunksize] for i in range(0, len(not_cached_jobs_ids), chunksize)]
                for chunk in chunks:
                    jobs_list = Client.getJobStatus(chunk, verbose=0)[1]
                    for job_info in jobs_list:
                        if job_info.Files and len(job_info.Files) > 0:
                            output_index = job_info.Files[0].lfn.split(':')[1]
                            status = self.jobs_to_idd_ds_status(job_info.jobStatus)
                            if output_index not in outputs_status or \
                                    outputs_status[output_index] != ContentStatus.Available:
                                outputs_status[output_index] = status
                            self.put_into_cache(output_index, status, job_info.PandaID)

            self.logger.debug("poll_panda_task, task_info: %s" % str(task_info))
            task_status = task_info["status"]
            return task_status, outputs_status
        except Exception as ex:
            if task_id:
                msg = "Failed to check the panda task (%s) status: %s" % (str(task_id), str(ex))
            else:
                msg = "Failed to check the panda task (%s) status: %s" % (task_name, str(ex))
            raise exceptions.IDDSException(msg)

    def poll_processing_updates(self, processing, input_output_maps):
        """
        *** Function called by Carrier agent.
        """
        updated_contents = []
        update_processing = {}
        self.logger.debug("poll_processing_updates, input_output_maps: %s" % str(input_output_maps))

        if processing:
            task_status, outputs_status = self.poll_panda_task(processing=processing)
            just_resolved_deps = self.check_dependencies()
            self.logger.debug("poll_processing_updates, outputs_status: %s" % str(outputs_status))
            self.logger.debug("poll_processing_updates, task_status: %s" % str(task_status))
            self.logger.debug("poll_processing_updates, files: %s" % str(just_resolved_deps))

            content_substatus = {'finished': 0, 'unfinished': 0}
            for map_id in input_output_maps:
                outputs = input_output_maps[map_id]['outputs']
                for content in outputs:
                    key = content['name']
                    if key in outputs_status:
                        if content.get('substatus', ContentStatus.New) != outputs_status[key]:
                            updated_content = {'content_id': content['content_id'] if not DEBUG else None,
                                               'substatus': outputs_status[key]}
                            updated_contents.append(updated_content)
                            content['substatus'] = outputs_status[key]
                    if not DEBUG:
                        if content['substatus'] == ContentStatus.Available:
                            content_substatus['finished'] += 1
                        else:
                            content_substatus['unfinished'] += 1

                inputs = input_output_maps[map_id]['inputs']
                if self.jedi_semaphore(processing):
                    if inputs[0]['name'] in just_resolved_deps and \
                            task_status and task_status in ('pending', 'running', 'finished', 'defined'):

                        inputs[0]['substatus'] = ContentStatus.Available
                        updated_content = {
                            'content_id': inputs[0]['content_id'] if not DEBUG else inputs[0]['name'],
                            'substatus': ContentStatus.Available
                        }
                        updated_contents.append(updated_content)

            if task_status and task_status == 'done' and \
                    content_substatus['finished'] > 0 and content_substatus['unfinished'] == 0:
                update_processing = {'processing_id': processing['processing_id'],
                                     'parameters': {'status': ProcessingStatus.Finished}}

        self.logger.debug("poll_processing_updates, task: %i, update_processing: %s" %
                          (processing['processing_metadata']['task_id'], str(update_processing)))
        self.logger.debug("poll_processing_updates, task: %i, updated_contents: %s" %
                          (processing['processing_metadata']['task_id'], str(updated_contents)))
        return update_processing, updated_contents

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

    def syn_work_status(self, registered_input_output_maps):
        self.get_status_statistics(registered_input_output_maps)
        self.logger.debug("syn_work_status, self.active_processings: %s" % str(self.active_processings))
        self.logger.debug("syn_work_status, self.has_new_inputs(): %s" % str(self.has_new_inputs()))
        self.logger.debug("syn_work_status, coll_metadata_is_open: %s" %
                          str(self.collections[self.primary_input_collection]['coll_metadata']['is_open']))
        self.logger.debug("syn_work_status, primary_input_collection_status: %s" %
                          str(self.collections[self.primary_input_collection]['status']))

        if self.is_processings_terminated() and not self.has_new_inputs():
            keys = self.status_statistics.keys()
            if ContentStatus.New.name in keys or ContentStatus.Processing.name in keys:
                pass
            else:
                if len(keys) == 1:
                    if ContentStatus.Available.name in keys:
                        self.status = WorkStatus.Finished
                    else:
                        self.status = WorkStatus.Failed
                else:
                    self.status = WorkStatus.SubFinished
