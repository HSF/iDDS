#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023


import traceback

from idds.common import exceptions
from idds.common.constants import (ContentStatus, ContentType, ProcessingStatus)
from idds.workflowv2.work import Processing

from .domapandawork import DomaPanDAWork


class DomaPanDAESWork(DomaPanDAWork):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='lsst', exec_type='panda', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 input_collections=None,
                 primary_output_collection=None, other_output_collections=None,
                 output_collections=None, log_collections=None,
                 logger=None, dependency_map=None, task_name="",
                 task_queue=None, queue=None, processing_type=None,
                 prodSourceLabel='test', task_type='lsst',
                 maxwalltime=90000, maxattempt=5, core_count=1,
                 encode_command_line=False,
                 num_retries=5,
                 task_priority=900,
                 task_log=None,
                 task_cloud=None,
                 task_site=None,
                 task_rss=1000,
                 vo='wlcg',
                 working_group='lsst',
                 es_dependency_map=None):

        super(DomaPanDAESWork, self).__init__(executable=executable, arguments=arguments,
                                              parameters=parameters, setup=setup,
                                              work_tag=work_tag, exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                              primary_input_collection=primary_input_collection,
                                              other_input_collections=other_input_collections,
                                              primary_output_collection=primary_output_collection,
                                              other_output_collections=other_output_collections,
                                              input_collections=input_collections,
                                              output_collections=output_collections,
                                              log_collections=log_collections,
                                              logger=logger,
                                              dependency_map=dependency_map, task_name=task_name,
                                              task_queue=task_queue, queue=queue, processing_type=processing_type,
                                              prodSourceLabel=prodSourceLabel, task_type=task_type,
                                              maxwalltime=maxwalltime, maxattempt=maxattempt, core_count=core_count,
                                              encode_command_line=encode_command_line,
                                              num_retries=num_retries,
                                              task_priority=task_priority, task_log=task_log,
                                              task_cloud=task_cloud, task_site=task_site, task_rss=task_rss,
                                              vo=vo, working_group=working_group)
        self.es_dependency_map = es_dependency_map

    def with_sub_map_id(self):
        return False

    @property
    def es_dependency_map(self):
        return self._es_dependency_map

    @es_dependency_map.setter
    def es_dependency_map(self, value):
        if value:
            if type(value) not in [dict]:
                raise exceptions.IDDSException("ES dependency_map should be a dict")
            # the dumplication is already verified in DomaEventMap, not do it again here

        self._es_dependency_map = value

    def depend_on(self, work):
        self.logger.debug("checking depending on")
        if self.dependency_tasks is None:
            self.logger.debug("constructing dependency_tasks set")
            dependency_tasks = set([])
            for job_name in self.es_dependency_map:
                es_dep_map = self.es_dependency_map[job_name]
                for event_index in es_dep_map:
                    input_dependency = es_dep_map[event_index]

                    for input_d in input_dependency:
                        task_name = input_d['group_label']
                        if task_name not in dependency_tasks:
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
        for job_name in self.es_dependency_map:
            es_dep_map = self.es_dependency_map[job_name]
            for event_index in es_dep_map:
                input_dependency = es_dep_map[event_index]

                for input_d in input_dependency:
                    task_name = input_d['group_label']
                    if task_name not in tasks:
                        tasks.add(task_name)
        return list(tasks)

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

    def map_file_to_content(self, coll_id, scope, name, event_index, dep_event_index=None):
        content = {'coll_id': coll_id,
                   'scope': scope,
                   'name': name,  # or a different file name from the dataset name
                   'sub_map_id': int(event_index),
                   'bytes': 1,
                   'adler32': '12345678',
                   'min_id': int(event_index),
                   'max_id': int(event_index) + 1,
                   'content_type': ContentType.Event,
                   # 'content_relation_type': content_relation_type,
                   # here events is all events for eventservice, not used here.
                   'content_metadata': {'events': 1}}
        if dep_event_index is not None:
            content['content_metadata'] = {'events': 1, 'dep_sub_map_id': int(dep_event_index)}
            content['dep_sub_map_id'] = int(dep_event_index)
        return content

    def is_all_dependency_tasks_available(self, inputs_dependency, task_name_to_coll_map):
        for input_d in inputs_dependency:
            task_name = input_d['group_label']
            if (task_name not in task_name_to_coll_map                    # noqa: W503
               or 'outputs' not in task_name_to_coll_map[task_name]      # noqa: W503
               or not task_name_to_coll_map[task_name]['outputs']):      # noqa: W503
                return False
        return True

    def get_unmapped_jobs(self, mapped_input_output_maps={}):
        mapped_outputs = self.get_mapped_outputs(mapped_input_output_maps)
        mapped_outputs_name = [ip['name'] for ip in mapped_outputs]
        unmapped_jobs = []
        for job_name in self.es_dependency_map:
            if job_name not in mapped_outputs_name:
                unmapped_jobs.append(job_name)
        return unmapped_jobs

    def has_external_dependency(self):
        if self.es_dependency_map:
            for job_name in self.es_dependency_map:
                es_dep_map = self.es_dependency_map[job_name]
                for event_index in es_dep_map:
                    inputs_dependency = es_dep_map[event_index]
                    if inputs_dependency:
                        for input_dep in inputs_dependency:
                            dep_task_name = input_dep['group_label']
                            dep_job_name = input_dep['event_job']
                            if dep_task_name != self.orig_task_name or dep_job_name != job_name:
                                return True
        return False

    def get_parent_work_names(self):
        parent_work_names = []
        for job_name in self.es_dependency_map:
            es_dep_map = self.es_dependency_map[job_name]
            for event_index in es_dep_map:
                input_dependency = es_dep_map[event_index]

                for input_d in input_dependency:
                    task_name = input_d['group_label']
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

            for job_name in unmapped_jobs:
                es_dep_map = self.es_dependency_map[job_name]
                job_input_dependency = []
                for event_index in es_dep_map:
                    input_dependency = es_dep_map[event_index]
                    job_input_dependency += input_dependency

                if self.is_all_dependency_tasks_available(job_input_dependency, task_name_to_coll_map):
                    new_input_output_maps[next_key] = {'inputs_dependency': [],
                                                       'logs': [],
                                                       'inputs': [],
                                                       'outputs': []}
                    for event_index in sorted(list(es_dep_map.keys())):
                        input_content = self.map_file_to_content(input_coll_id, input_coll.scope, job_name, event_index)
                        output_content = self.map_file_to_content(output_coll_id, output_coll.scope, job_name, event_index)

                        new_input_output_maps[next_key]['inputs'].append(input_content)
                        new_input_output_maps[next_key]['outputs'].append(output_content)

                        uni_input_name = {}
                        input_dependency = es_dep_map[event_index]
                        for input_d in input_dependency:
                            task_name = input_d['group_label']
                            job_name = input_d['event_job']
                            dep_event_index = input_d['event_index']
                            task_name_input_name = task_name + job_name + str(dep_event_index)
                            if task_name_input_name not in uni_input_name:
                                uni_input_name[task_name_input_name] = None
                                input_d_coll = task_name_to_coll_map[task_name]['outputs'][0]
                                input_d_content = self.map_file_to_content(input_d_coll['coll_id'], input_d_coll['scope'], job_name, event_index, dep_event_index)
                                new_input_output_maps[next_key]['inputs_dependency'].append(input_d_content)
                            else:
                                self.logger.debug("get_new_input_output_maps, duplicated input dependency for job %s event_index %s: %s" % (job_name, event_index, str(input_dependency)))

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

    def create_processing(self, input_output_maps=[]):
        """
        *** Function called by Transformer agent.

        :param input_output_maps: new maps from inputs to outputs.
        """
        # avoid duplicated task name
        self.task_name = self.task_name + "_" + str(self.get_request_id()) + "_" + str(self.get_work_id())

        in_files = []
        # has_dependencies = False
        if self.es_dependency_map is None:
            self.es_dependency_map = {}
        for job_name in self.es_dependency_map:
            es_dep_map = self.es_dependency_map[job_name]
            event_indexes = list(es_dep_map.keys())
            # min_event_index = min(event_indexes)
            # max_event_index = max(event_indexes)
            # event_file_name = job_name + "%s^%s" % (min_event_index, max_event_index)
            event_file_name = job_name + "^%s" % (len(event_indexes))

            in_files.append(event_file_name)

        task_param_map = {}
        task_param_map['vo'] = self.vo
        if self.task_queue and len(self.task_queue) > 0:
            task_param_map['site'] = self.task_queue
        elif self.queue and len(self.queue) > 0:
            task_param_map['site'] = self.queue
        task_param_map['workingGroup'] = self.working_group
        task_param_map['nFilesPerJob'] = 1
        if in_files:
            if self.has_external_dependency():
                task_param_map['inputPreStaging'] = True
            task_param_map['nFiles'] = len(in_files)
            task_param_map['noInput'] = True
            task_param_map['pfnList'] = in_files
        else:
            # task_param_map['inputPreStaging'] = True
            in_files = ['pseudo_file']
            task_param_map['nFiles'] = len(in_files)
            task_param_map['noInput'] = True
            task_param_map['pfnList'] = in_files

        # enabling eventservice
        task_param_map['fineGrainedProc'] = True
        # task_param_map['eventService'] = 3

        task_param_map['taskName'] = self.task_name
        task_param_map['userName'] = self.username if self.username else 'iDDS'
        task_param_map['taskPriority'] = self.task_priority
        task_param_map['architecture'] = ''
        task_param_map['transUses'] = ''
        task_param_map['transHome'] = None

        executable = self.executable
        executable = "export IDDS_BUILD_REQUEST_ID=" + str(self.get_request_id()) + ";"
        executable += "export IDDS_BUIL_SIGNATURE=" + str(self.signature) + "; " + self.executable

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
            task_param_map['ramCount'] = self.task_rss / self.core_count if self.core_count else self.task_rss
            # task_param_map['ramUnit'] = 'MB'
            task_param_map['ramUnit'] = 'MBPerCoreFixed'

        # task_param_map['inputPreStaging'] = True
        task_param_map['prestagingRuleID'] = 123
        task_param_map['nChunksToWait'] = 1
        task_param_map['maxCpuCount'] = self.core_count
        task_param_map['maxWalltime'] = self.maxWalltime
        task_param_map['maxFailure'] = self.maxAttempt if self.maxAttempt else 5
        task_param_map['maxAttempt'] = self.maxAttempt if self.maxAttempt else 5
        if task_param_map['maxAttempt'] < self.num_retries:
            task_param_map['maxAttempt'] = self.num_retries
        if task_param_map['maxFailure'] < self.num_retries:
            task_param_map['maxFailure'] = self.num_retries
        task_param_map['log'] = self.task_log
        task_param_map['jobParameters'] = [
            {'type': 'constant',
             'value': executable,  # noqa: E501
             },
        ]

        task_param_map['reqID'] = self.get_request_id()

        processing_metadata = {'task_param': task_param_map}
        proc = Processing(processing_metadata=processing_metadata)
        proc.workload_id = None
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc.internal_id)
        return proc

    def get_unterminated_jobs(self, all_jobs_ids, input_output_maps, contents_ext):
        finished_jobs, failed_jobs = [], []

        contents_ext_dict = {content['content_id']: content for content in contents_ext}

        for map_id in input_output_maps:
            outputs = input_output_maps[map_id]['outputs']
            for content in outputs:
                if content['substatus'] in [ContentStatus.Available]:
                    if 'panda_id' in content['content_metadata']:
                        panda_id = content['content_metadata']['panda_id']
                        if content['content_id'] not in contents_ext_dict:
                            continue

                        content_ext = contents_ext_dict[content['content_id']]
                        if content['substatus'] != content_ext['status'] or panda_id != content_ext['panda_id']:
                            continue

                        if panda_id not in finished_jobs:
                            finished_jobs.append(panda_id)
                elif content['substatus'] in [ContentStatus.FinalFailed,
                                              ContentStatus.Lost, ContentStatus.Deleted,
                                              ContentStatus.Missing]:
                    if 'panda_id' in content['content_metadata']:
                        panda_id = content['content_metadata']['panda_id']
                        if content['content_id'] not in contents_ext_dict:
                            continue

                        content_ext = contents_ext_dict[content['content_id']]
                        if content['substatus'] != content_ext['status'] or panda_id != content_ext['panda_id']:
                            continue

                        if panda_id not in failed_jobs:
                            failed_jobs.append(panda_id)

        all_jobs_ids = set(all_jobs_ids)
        terminated_jobs = set(finished_jobs + failed_jobs)
        unterminated_jobs = all_jobs_ids - terminated_jobs
        return list(unterminated_jobs)

    def get_panda_job_status(self, jobids, log_prefix=''):
        self.logger.debug(log_prefix + "get_panda_job_status, jobids[:10]: %s" % str(jobids[:10]))
        try:
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
                    try:
                        ret1 = Client.getFullJobStatus(ids=left_jobids, verbose=False)
                        if ret1[0] == 0:
                            left_jobs_list = ret1[1]
                        ret_jobs = ret_jobs + left_jobs_list
                    except Exception as ex:
                        self.logger.error(str(ex))
                        self.logger.error(traceback.format_exc())
                return ret_jobs
            else:
                self.logger.warn(log_prefix + "get_panda_job_status failed: %s" % str(ret))
                return []
        except Exception as ex:
            self.logger.error(str(ex))
            self.logger.error(traceback.format_exc())
        return []

    def get_panda_event_status(self, eventids, log_prefix=''):
        self.logger.debug(log_prefix + "get_panda_event_status, eventids[:10]: %s" % str(eventids[:10]))
        try:
            from pandaclient import Client
            ret_status, events_status = Client.get_events_status(eventids)
            self.logger.info(log_prefix + "get_panda_events_status: status: %s" % ret_status)
            # self.logger.debug(log_prefix + "poll_panda_jobs, get_events_status: event status: %s" % event_status)
            if ret_status != 0:
                self.logger.error(log_prefix + "get_panda_events_status: event status: %s" % events_status)
            else:
                # self.logger.error(log_prefix + "get_panda_events_status: event status: %s" % events_status)
                return events_status
        except Exception as ex:
            self.logger.error(str(ex))
            self.logger.error(traceback.format_exc())
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
            self_maxAttempt = int(self.maxAttempt) if self.maxAttempt else 0
            if (attempt_nr >= max_attempt) and (attempt_nr >= self_maxAttempt):
                return ContentStatus.FinalFailed
            else:
                return ContentStatus.Failed
        elif jobstatus in ['activated']:
            return ContentStatus.Activated
        else:
            return ContentStatus.Processing

    def poll_panda_jobs(self, task_id, job_ids, log_prefix=''):
        terminated_jobs = {}
        job_status_info = {}
        self.logger.debug(log_prefix + "poll_panda_jobs, poll_panda_jobs_chunk_size: %s, job_ids[:10]: %s" % (self.poll_panda_jobs_chunk_size, str(job_ids[:10])))
        chunksize = self.poll_panda_jobs_chunk_size
        chunks = [job_ids[i:i + chunksize] for i in range(0, len(job_ids), chunksize)]
        for chunk in chunks:
            # jobs_list = Client.getJobStatus(chunk, verbose=0)[1]
            jobs_list = self.get_panda_job_status(chunk, log_prefix=log_prefix)
            if jobs_list:
                self.logger.debug(log_prefix + "poll_panda_jobs, input jobs: %s, output_jobs: %s" % (len(chunk), len(jobs_list)))
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
                            job_status_info[input_file] = {'panda_id': job_info.PandaID, 'status': job_status, 'job_info': job_info}

                            if job_status in [ContentStatus.Available, ContentStatus.Failed, ContentStatus.FinalFailed,
                                              ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                                if job_info.PandaID not in terminated_jobs:
                                    terminated_jobs[job_info.PandaID] = []
                                terminated_jobs[job_info.PandaID].append(input_file)
            else:
                self.logger.warn(log_prefix + "poll_panda_jobs, input jobs: %s, output_jobs: %s" % (len(chunk), jobs_list))

        # poll event status
        terminated_job_ids = list(terminated_jobs.keys())
        chunks = [terminated_job_ids[i:i + chunksize] for i in range(0, len(terminated_job_ids), chunksize)]
        for chunk in chunks:
            chunk_ids = [{'task_id': task_id, 'panda_id': panda_id} for panda_id in chunk]
            events_status = self.get_panda_event_status(chunk_ids)
            if events_status is None:
                pass
            else:
                for panda_id in events_status:
                    input_files = terminated_jobs[int(panda_id)]
                    for input_file in input_files:
                        job_status_info[input_file]['events_status'] = events_status[str(panda_id)]
        return job_status_info

    def get_update_contents(self, unterminated_jobs_status, input_output_maps, contents_ext, job_info_maps, abort=False, log_prefix=''):
        inputname_to_map_id_outputs = {}
        for map_id in input_output_maps:
            inputs = input_output_maps[map_id]['inputs']
            outputs = input_output_maps[map_id]['outputs']
            for content in inputs:
                inputname_to_map_id_outputs[content['name']] = {'map_id': map_id, 'outputs': outputs}

        contents_ext_dict = {content['content_id']: content for content in contents_ext}

        update_contents, update_contents_full = [], []
        new_contents_ext, update_contents_ext = [], []
        for input_file in unterminated_jobs_status:
            panda_job_status = unterminated_jobs_status[input_file]
            panda_id = panda_job_status['panda_id']
            job_status = panda_job_status['status']
            job_info = panda_job_status['job_info']
            events_status = None
            if 'events_status' in panda_job_status:
                events_status = panda_job_status['events_status']

            if events_status is None or job_status not in [ContentStatus.Failed, ContentStatus.FinalFailed,
                                                           ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                continue

            output_contents = inputname_to_map_id_outputs[input_file]['outputs']
            output_contents_sub_map = {}
            for content in output_contents:
                if content['sub_map_id'] not in output_contents_sub_map:
                    output_contents_sub_map[content['sub_map_id']] = []
                output_contents_sub_map[content['sub_map_id']].append(content)

            for sub_map_id in output_contents_sub_map:
                for content in output_contents_sub_map[sub_map_id]:
                    event_status = events_status.get(str(sub_map_id), None)
                    update_content = None
                    if not event_status:
                        if job_status in [ContentStatus.Failed, ContentStatus.FinalFailed,
                                          ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                            event_status = job_status
                    if event_status:
                        # content['substatus'] = panda_status
                        content['substatus'] = event_status
                        update_contents_full.append(content)
                        update_content = {'content_id': content['content_id'],
                                          # 'status': panda_status,
                                          # 'substatus': panda_status,
                                          'substatus': event_status,
                                          'external_event_status': event_status}

                    if 'panda_id' in content['content_metadata'] and content['content_metadata']['panda_id']:
                        if content['content_metadata']['panda_id'] < panda_id:
                            # new panda id is the bigger one.
                            if 'old_panda_id' not in content['content_metadata']:
                                content['content_metadata']['old_panda_id'] = []
                            if content['content_metadata']['panda_id'] not in content['content_metadata']['old_panda_id']:
                                content['content_metadata']['old_panda_id'].append(content['content_metadata']['panda_id'])
                            content['content_metadata']['panda_id'] = panda_id
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
                    else:
                        content['content_metadata']['panda_id'] = panda_id
                        update_content['content_metadata'] = content['content_metadata']

                    if update_content:
                        update_contents.append(update_content)

                    if job_status in [ContentStatus.Available, ContentStatus.Failed, ContentStatus.FinalFailed,
                                      ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                        if content['content_id'] not in contents_ext_dict:
                            new_content_ext = {'content_id': content['content_id'],
                                               'request_id': content['request_id'],
                                               'transform_id': content['transform_id'],
                                               'workload_id': content['workload_id'],
                                               'coll_id': content['coll_id'],
                                               'map_id': content['map_id'],
                                               'status': event_status}
                            for job_info_item in job_info_maps:
                                new_content_ext[job_info_item] = getattr(job_info, job_info_maps[job_info_item])
                                if new_content_ext[job_info_item] == 'NULL':
                                    new_content_ext[job_info_item] = None
                                if new_content_ext[job_info_item] is None:
                                    del new_content_ext[job_info_item]
                            new_contents_ext.append(new_content_ext)
                        else:
                            update_content_ext = {'content_id': content['content_id'],
                                                  'status': event_status}
                            for job_info_item in job_info_maps:
                                update_content_ext[job_info_item] = getattr(job_info, job_info_maps[job_info_item])
                                if update_content_ext[job_info_item] == 'NULL':
                                    update_content_ext[job_info_item] = None
                                if update_content_ext[job_info_item] is None:
                                    del update_content_ext[job_info_item]
                            update_contents_ext.append(update_content_ext)

        self.logger.debug("get_update_contents, num_update_contents: %s" % (len(update_contents)))
        self.logger.debug("get_update_contents, update_contents[:3]: %s" % (str(update_contents[:3])))
        self.logger.debug("get_update_contents, new_contents_ext[:1]: %s" % (str(new_contents_ext[:1])))
        self.logger.debug("get_update_contents, update_contents_ext[:1]: %s" % (str(update_contents_ext[:1])))

        return update_contents, update_contents_full, new_contents_ext, update_contents_ext

    def poll_panda_task(self, processing=None, input_output_maps=None, contents_ext=None, job_info_maps={}, log_prefix=''):
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
                        return ProcessingStatus.Running, [], [], [], []

                    task_info = task_info[1]

                    processing_status = self.get_processing_status_from_panda_status(task_info["status"])
                    self.logger.info(log_prefix + "poll_panda_task processing_status: %s" % processing_status)

                    all_jobs_ids = task_info['PandaID']

                    unterminated_jobs = self.get_unterminated_jobs(all_jobs_ids, input_output_maps, contents_ext)
                    self.logger.debug(log_prefix + "poll_panda_task, task_id: %s, all jobs: %s, unterminated_jobs: %s" % (str(task_id), len(all_jobs_ids), len(unterminated_jobs)))

                    unterminated_jobs_status = self.poll_panda_jobs(task_id, unterminated_jobs, log_prefix=log_prefix)
                    abort_status = False
                    if processing_status in [ProcessingStatus.Cancelled]:
                        abort_status = True
                    ret_contents = self.get_update_contents(unterminated_jobs_status, input_output_maps, contents_ext, job_info_maps, abort=abort_status, log_prefix=log_prefix)
                    updated_contents, update_contents_full, new_contents_ext, update_contents_ext = ret_contents

                    return processing_status, updated_contents, update_contents_full, new_contents_ext, update_contents_ext
                else:
                    self.logger.error("poll_panda_task, task_id (%s) cannot be found" % task_id)
                    return ProcessingStatus.Failed, [], [], [], []
        except Exception as ex:
            msg = "Failed to check the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            self.logger.error(log_prefix + msg)
            self.logger.error(log_prefix + str(ex))
            self.logger.error(traceback.format_exc())
            # raise exceptions.IDDSException(msg)
        return ProcessingStatus.Running, [], [], [], []

    def poll_processing_updates(self, processing, input_output_maps, contents_ext=None, job_info_maps={}, log_prefix=''):
        """
        *** Function called by Carrier agent.
        """
        update_contents = []
        update_contents_full = []
        self.logger.debug(log_prefix + "poll_processing_updates, input_output_maps.keys[:3]: %s" % str(list(input_output_maps.keys())[:3]))

        if processing:
            proc = processing['processing_metadata']['processing']

            ret_poll_panda_task = self.poll_panda_task(processing=processing,
                                                       input_output_maps=input_output_maps,
                                                       contents_ext=contents_ext,
                                                       job_info_maps=job_info_maps,
                                                       log_prefix=log_prefix)

            processing_status, update_contents, update_contents_full, new_contents_ext, update_contents_ext = ret_poll_panda_task
            self.logger.debug(log_prefix + "poll_processing_updates, processing_status: %s" % str(processing_status))
            self.logger.debug(log_prefix + "poll_processing_updates, update_contents[:3]: %s" % str(update_contents[:3]))

            if update_contents:
                proc.has_new_updates()
        return processing_status, update_contents, {}, update_contents_full, {}, new_contents_ext, update_contents_ext
