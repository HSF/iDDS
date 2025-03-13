#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2024
# - Sergey Padolski, <spadolski@bnl.gov>, 2020


try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

import concurrent.futures
import datetime
import json
import os
import time
import traceback

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionStatus, CollectionType,
                                   ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
from idds.common.utils import get_list_chunks, split_chunks_not_continous
from idds.workflowv2.work import Work, Processing
from idds.workflowv2.workflow import Condition


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
                 prodSourceLabel='test', task_type='lsst',
                 maxwalltime=90000, maxattempt=5, core_count=1,
                 encode_command_line=False,
                 num_retries=5,
                 task_priority=900,
                 task_log=None,
                 task_cloud=None,
                 task_site=None,
                 task_rss=1000,
                 task_rss_retry_offset=0,
                 task_rss_retry_step=0,
                 task_rss_max=None,
                 vo='wlcg',
                 es=False,
                 es_label=None,
                 max_events_per_job=40,
                 max_name_length=4000,
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

        self.max_name_length = max_name_length
        if "IDDS_MAX_NAME_LENGTH" in os.environ:
            try:
                self.max_name_length = int(os.environ['IDDS_MAX_NAME_LENGTH'])
            except Exception as ex:
                self.logger.warn('IDDS_MAX_NAME_LENGTH is not defined correctly: %s' % str(ex))

        # self.logger.setLevel(logging.DEBUG)

        self.task_name = task_name
        self.orig_task_name = self.task_name
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
        self.task_rss_retry_offset = task_rss_retry_offset
        self.task_rss_retry_step = task_rss_retry_step
        self.task_rss_max = task_rss_max
        self.task_priority = task_priority

        self.vo = vo
        self.working_group = working_group

        self.retry_number = 0
        self.num_retries = num_retries

        self.poll_panda_jobs_chunk_size = 2000

        self.load_panda_urls()

        self.dependency_tasks = None

        self.es = es
        self.es_label = es_label
        self.max_events_per_job = max_events_per_job
        self.es_files = {}

        self.dependency_map = dependency_map
        if self.dependency_map is None:
            self.dependency_map = {}
        self.dependency_map_deleted = []

        self.additional_task_parameters = {}
        self.additional_task_parameters_per_site = {}

        self.core_to_queues = {}

        self.zip_items = ['_dependency_map']
        self.not_auto_unzip_items = ['_dependency_map']

    def my_condition(self):
        if self.is_finished():
            return True
        return False

    def get_site(self):
        if self.task_site:
            return self.task_site
        if self.task_queue:
            return self.task_queue
        if self.queue:
            return self.queue
        return self.task_cloud

    @property
    def num_inputs(self):
        num = self.get_metadata_item('num_inputs', None)
        return num

    @num_inputs.setter
    def num_inputs(self, value):
        self.add_metadata_item('num_inputs', value)

    @property
    def num_dependencies(self):
        num = self.get_metadata_item('num_dependencies', None)
        return num

    @num_dependencies.setter
    def num_dependencies(self, value):
        self.add_metadata_item('num_dependencies', value)

    def count_dependencies(self, data):
        if self.num_dependencies is not None and self.num_inputs is not None:
            return self.num_inputs, self.num_dependencies

        num_inputs = 0
        num_dependencies = 0
        try:
            for item in data:
                # item_name = item['name']
                inputs_dependency = item["dependencies"]
                num_dependencies += len(inputs_dependency)
                num_inputs += 1
        except Exception as ex:
            self.logger.warn(f"Failed to count dependencies: {ex}")
        return num_inputs, num_dependencies

    @property
    def dependency_map(self):
        if self.should_unzip('_dependency_map'):
            data = self.unzip_data(self._dependency_map)
            num_inputs, num_dependencies = self.count_dependencies(data)
            self.num_inputs = num_inputs
            self.num_dependencies = num_dependencies
            return data

        num_inputs, num_dependencies = self.count_dependencies(self._dependency_map)
        self.num_inputs = num_inputs
        self.num_dependencies = num_dependencies
        return self._dependency_map

    @dependency_map.setter
    def dependency_map(self, value):
        num_dependencies = 0
        num_inputs = 0
        if value:
            if type(value) not in [list, tuple]:
                raise exceptions.IDDSException("dependency_map should be a list or tuple")
            item_names = {}
            for item in value:
                item_name = item['name']
                if len(item_name) > self.max_name_length:
                    raise exceptions.IDDSException("The file name is long (%s), which is bigger than the maximum name length (%s)" % (len(item_name), self.max_name_length))
                inputs_dependency = item["dependencies"]
                num_inputs += 1
                num_dependencies += len(inputs_dependency)
                if item_name not in item_names:
                    item_names[item_name] = item
                else:
                    raise exceptions.IDDSException("duplicated item with the same name: %s" % item_name)

                uni_input_name = {}
                for input_d in inputs_dependency:
                    task_name = input_d['task']
                    input_name = input_d['inputname']
                    task_name_input_name = task_name + input_name
                    if task_name_input_name not in uni_input_name:
                        uni_input_name[task_name_input_name] = None
                    else:
                        raise exceptions.IDDSException("duplicated input dependency for item %s: %s" % (item_name, inputs_dependency))

        self._dependency_map = value

        self.num_inputs = num_inputs
        self.num_dependencies = num_dependencies

        if self.es:
            self.construct_es_files()

    def construct_es_files(self):
        # job's order id must not be skipped/duplicated
        order_id_map = {}
        local_order_id = False
        for job in self._dependency_map:
            if job.get("order_id", None) is None:
                local_order_id = True
                self.logger.warn("order_id is not set, user local order id")
                break
            order_id = job.get("order_id", None)
            if order_id in order_id_map:
                local_order_id = True
                self.logger.warn("order_id has duplications, user local order id")
                break
            order_id_map[order_id] = job

        if local_order_id:
            self.logger.warn("order_id is not set correctly. With EventService, it will not be able to mapping jobs to events correctly. Disable EventService.")
            self.es = False
            return

        order_id_map = {}
        order_id_group_map = {}
        if local_order_id:
            order_id = 0
            for job in self._dependency_map:
                groups = job.get("groups", "es_default")
                if groups not in order_id_group_map:
                    order_id_group_map[groups] = {}
                order_id_group_map[groups][order_id] = job
                order_id_map[order_id] = job
                order_id += 1
        else:
            for job in self._dependency_map:
                groups = job.get("groups", "es_default")
                if groups not in order_id_group_map:
                    order_id_group_map[groups] = {}
                order_id = int(job.get("order_id"))
                order_id_group_map[groups][order_id] = job
                order_id_map[order_id] = job

        final_chunks = []
        for groups in order_id_group_map:
            order_id_list = sorted(list(order_id_group_map[groups].keys()))
            order_id_list_chunks = split_chunks_not_continous(order_id_list)
            for chunk in order_id_list_chunks:
                sub_chunks = get_list_chunks(chunk, bulk_size=self.max_events_per_job)
            final_chunks = final_chunks + sub_chunks

        for map_id, chunk in enumerate(final_chunks):
            order_id_start = chunk[0]
            order_id_end = chunk[-1]
            num_events = order_id_end - order_id_start + 1
            eventservice_file_name = "%s:eventservice_%s^%s" % (self.es_label, order_id_start, num_events)

            has_dependencies = False
            # sub_maps = {}
            for order_id in chunk:
                job = order_id_map[order_id]
                # output_name = job['name']
                inputs_dependency = job["dependencies"]
                if inputs_dependency:
                    has_dependencies = True
                sub_map_id = order_id - order_id_start
                # sub_maps[sub_map_id] = {'order_id': order_id,
                #                         'sub_map_id': sub_map_id,
                #                         'job': job}

                # set the job information
                # order_id should be already there
                # job["order_id"] = order_id
                job["map_id"] = map_id
                job["sub_map_id"] = sub_map_id
                job["es_name"] = eventservice_file_name

            # self.es_files[eventservice_file_name] = {'has_dependencies': has_dependencies, 'sub_maps': sub_maps}
            self.es_files[eventservice_file_name] = {'has_dependencies': has_dependencies, 'map_id': map_id, 'order_ids': chunk}

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
        if self.class_name in attrs and ('life_time' not in attrs[self.class_name] or int(attrs[self.class_name]['life_time']) <= 0):
            attrs['life_time'] = None
        super(DomaPanDAWork, self).set_agent_attributes(attrs)
        if 'num_retries' in self.agent_attributes and self.agent_attributes['num_retries']:
            self.num_retries = int(self.agent_attributes['num_retries'])
        if 'poll_panda_jobs_chunk_size' in self.agent_attributes and self.agent_attributes['poll_panda_jobs_chunk_size']:
            self.poll_panda_jobs_chunk_size = int(self.agent_attributes['poll_panda_jobs_chunk_size'])
        if 'additional_task_parameters' in self.agent_attributes and self.agent_attributes['additional_task_parameters']:
            if not self.additional_task_parameters:
                self.additional_task_parameters = {}
            try:
                self.agent_attributes['additional_task_parameters'] = json.loads(self.agent_attributes['additional_task_parameters'])
                for key, value in self.agent_attributes['additional_task_parameters'].items():
                    if key not in self.additional_task_parameters:
                        self.additional_task_parameters[key] = value
            except Exception as ex:
                self.logger.warn(f"Failed to set additional_task_parameters: {ex}")

        if 'additional_task_parameters_per_site' in self.agent_attributes and self.agent_attributes['additional_task_parameters_per_site']:
            if not self.additional_task_parameters_per_site:
                self.additional_task_parameters_per_site = {}
            try:
                self.agent_attributes['additional_task_parameters_per_site'] = json.loads(self.agent_attributes['additional_task_parameters_per_site'])
                for site in self.agent_attributes['additional_task_parameters_per_site']:
                    if site not in self.additional_task_parameters_per_site:
                        self.additional_task_parameters_per_site[site] = {}
                    for key, value in self.agent_attributes['additional_task_parameters_per_site'][site].items():
                        if key not in self.additional_task_parameters_per_site[site]:
                            self.additional_task_parameters_per_site[site][key] = value
            except Exception as ex:
                self.logger.warn(f"Failed to set additional_task_parameters_per_site: {ex}")

        if 'core_to_queues' in self.agent_attributes and self.agent_attributes['core_to_queues']:
            try:
                self.agent_attributes['core_to_queues'] = json.loads(self.agent_attributes['core_to_queues'])
                self.core_to_queues = self.agent_attributes['core_to_queues']
            except Exception as ex:
                self.logger.warn(f"Failed to set core_to_queues: {ex}")

    def depend_on(self, work):
        self.logger.debug("checking depending on")
        if self.dependency_tasks is None:
            self.logger.debug("constructing dependency_tasks set")
            dependency_tasks = set([])
            for job in self.dependency_map:
                inputs_dependency = job["dependencies"]

                for input_d in inputs_dependency:
                    task_name = input_d['task']
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
            # primary_input = inputs[0]
            for ip in inputs:
                # if 'primary' in ip['content_metadata'] and ip['content_metadata']['primary']:
                #     primary_input = ip
                # ret.append(primary_input)
                ret.append(ip)
        return ret

    def get_mapped_outputs(self, mapped_input_output_maps):
        ret = []
        for map_id in mapped_input_output_maps:
            outputs = mapped_input_output_maps[map_id]['outputs']

            # if 'primary' is not set, the first one is the primary input.
            # primary_output = outputs[0]
            for ip in outputs:
                # if 'primary' in ip['content_metadata'] and ip['content_metadata']['primary']:
                #     primary_output = ip
                # ret.append(primary_output)
                ret.append(ip)
        return ret

    def map_file_to_content(self, coll_id, scope, name, order_id=None, sub_map_id=None, es_name=None):
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
        if order_id is not None:
            content['min_id'] = int(order_id)
            content['max_id'] = int(order_id) + 1
        if sub_map_id is not None:
            content['sub_map_id'] = sub_map_id
        if es_name is not None:
            content['path'] = es_name.split('^')[0]
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
        if self.dependency_map:
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
            input_coll = self.get_input_collections()[0]
            input_coll_id = input_coll.coll_id
            output_coll = self.get_output_collections()[0]
            output_coll_id = output_coll.coll_id

            task_name_to_coll_map = self.get_work_name_to_coll_map()

            if not self.es:
                mapped_keys = mapped_input_output_maps.keys()
                if mapped_keys:
                    next_key = max(mapped_keys) + 1
                else:
                    next_key = 1

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

                        uni_input_name = {}
                        for input_d in inputs_dependency:
                            task_name = input_d['task']
                            input_name = input_d['inputname']
                            task_name_input_name = task_name + input_name
                            if task_name_input_name not in uni_input_name:
                                uni_input_name[task_name_input_name] = None
                                input_d_coll = task_name_to_coll_map[task_name]['outputs'][0]
                                input_d_content = self.map_file_to_content(input_d_coll['coll_id'], input_d_coll['scope'], input_name)
                                new_input_output_maps[next_key]['inputs_dependency'].append(input_d_content)
                            else:
                                self.logger.debug("get_new_input_output_maps, duplicated input dependency for job %s: %s" % (job['name'], str(job["dependencies"])))

                        # all inputs are parsed. move it to dependency_map_deleted
                        # self.dependency_map_deleted.append(job)
                        next_key += 1
                    else:
                        # not all inputs for this job can be parsed.
                        # self.dependency_map.append(job)
                        pass
            else:
                order_id_map = {}
                for job in unmapped_jobs:
                    order_id = job["order_id"]
                    order_id_map[order_id] = job
                for es_name in self.es_files:
                    order_ids = self.es_files[es_name]["order_ids"]
                    not_filled = False
                    for order_id in order_ids:
                        if order_id in order_id_map:
                            not_filled = True

                    if not not_filled:
                        continue

                    # order_id_start = order_ids[0]
                    # order_id_end = order_ids[-1]
                    # num_events = order_id_end - order_id_start + 1
                    # eventservice_file_name = es_name
                    next_key = self.es_files[es_name]["map_id"]

                    all_inputs_dependency = []
                    for order_id in order_ids:
                        job = order_id_map[order_id]
                        # output_name = job['name']
                        inputs_dependency = job["dependencies"]
                        all_inputs_dependency = all_inputs_dependency + inputs_dependency

                    if self.is_all_dependency_tasks_available(all_inputs_dependency, task_name_to_coll_map):
                        new_input_output_maps[next_key] = {"sub_maps": []}

                        for order_id in order_ids:
                            job = order_id_map[order_id]
                            output_name = job['name']
                            inputs_dependency = job["dependencies"]
                            sub_map_id = job["sub_map_id"]
                            input_content = self.map_file_to_content(input_coll_id, input_coll.scope, output_name,
                                                                     order_id=order_id, sub_map_id=sub_map_id, es_name=es_name)
                            output_content = self.map_file_to_content(output_coll_id, output_coll.scope, output_name,
                                                                      order_id=order_id, sub_map_id=sub_map_id, es_name=es_name)
                            sub_map = {'order_id': order_id,
                                       'sub_map_id': sub_map_id,
                                       'inputs_dependency': [],
                                       'logs': [],
                                       'inputs': [input_content],
                                       'outputs': [output_content]}

                            uni_input_name = {}
                            for input_d in inputs_dependency:
                                task_name = input_d['task']
                                input_name = input_d['inputname']
                                task_name_input_name = task_name + input_name
                                if task_name_input_name not in uni_input_name:
                                    uni_input_name[task_name_input_name] = None
                                    input_d_coll = task_name_to_coll_map[task_name]['outputs'][0]
                                    input_d_content = self.map_file_to_content(input_d_coll['coll_id'], input_d_coll['scope'], input_name)
                                    sub_map['inputs_dependency'].append(input_d_content)
                                else:
                                    self.logger.debug("get_new_input_output_maps, duplicated input dependency for job %s: %s" % (job['name'], str(job["dependencies"])))

                            new_input_output_maps[next_key]["sub_maps"].append(sub_map)

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
        has_dependencies = False
        if self.dependency_map is None:
            self.dependency_map = {}
        if not self.es:
            for job in self.dependency_map:
                in_files.append(job['name'])
                if not has_dependencies and "dependencies" in job and job['dependencies']:
                    has_dependencies = True
        else:
            for es_file in self.es_files:
                has_dependencies = self.es_files[es_file]['has_dependencies']
                in_files.append(es_file)

        task_param_map = {}
        task_param_map['vo'] = self.vo
        if self.task_queue and len(self.task_queue) > 0:
            task_param_map['site'] = self.task_queue
        elif self.queue and len(self.queue) > 0:
            task_param_map['site'] = self.queue
        task_param_map['workingGroup'] = self.working_group
        task_param_map['nFilesPerJob'] = 1
        if in_files:
            if has_dependencies:
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

        if self.es:
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
        if self.task_rss_retry_offset:
            task_param_map['retryRamOffset'] = self.task_rss_retry_offset / self.core_count if self.core_count else self.task_rss_retry_offset
        if self.task_rss_retry_step:
            task_param_map['retryRamStep'] = self.task_rss_retry_step / self.core_count if self.core_count else self.task_rss_retry_step
        if self.task_rss_max:
            # todo: until PanDA supports it
            # taskParamMap['maxRamCount'] = self.task_rss_max
            pass

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

    def get_site_from_cloud(self, site):
        try:
            func_site_to_cloud = self.get_func_site_to_cloud()
            if func_site_to_cloud:
                cloud = func_site_to_cloud(site)
                return cloud
        except Exception as ex:
            self.logger.error(ex)
            return None
        return None

    def submit_panda_task(self, processing):
        try:
            from pandaclient import Client

            proc = processing['processing_metadata']['processing']
            task_param = proc.processing_metadata['task_param']
            if 'new_retries' in processing and processing['new_retries']:
                new_retries = int(processing['new_retries'])
                task_param['taskName'] = task_param['taskName'] + "_" + str(new_retries)
            cloud = self.get_site_from_cloud(task_param['PandaSite'])
            if cloud and cloud != task_param['cloud']:
                self.logger.info(f"Task cloud was set to {task_param['cloud']}, which is different from {cloud}, reset it to {cloud}")
                task_param['cloud'] = cloud

            if self.additional_task_parameters:
                try:
                    for key, value in self.additional_task_parameters.items():
                        if key not in task_param:
                            task_param[key] = value
                except Exception as ex:
                    self.logger.warn(f"failed to set task parameter map with additional_task_parameters: {ex}")
            if self.additional_task_parameters_per_site:
                try:
                    for site in self.additional_task_parameters_per_site:
                        if ('PandaSite' in task_param and task_param['PandaSite'] and site in task_param['PandaSite']) or ('site' in task_param and task_param['site'] and site in task_param['site']):
                            for key, value in self.additional_task_parameters_per_site[site].items():
                                if key not in task_param:
                                    task_param[key] = value
                except Exception as ex:
                    self.logger.warn(f"failed to set task parameter map with additional_task_parameters_per_site: {ex}")

            if self.core_to_queues:
                try:
                    # core_to_queues = {"1": {"queues": ["Rubin", "Rubin_Extra_Himem"], "processing_type": ""},
                    #                   "Rubin_Multi": {"queues": ["Rubin_Multi"], "processing_type": "Rubin_Multi"},
                    #                   "Rubin_Merge": {"queues": ["Rubin_Merge"], "processing_type": "Rubin_Merge"},
                    #                   "any": {"queues": ["Rubin_Multi"], "processing_type": "Rubin_Multi"}}

                    if task_param['processingType']:
                        msg = f"processingType {task_param['processingType']} is already set, do nothing"
                        self.logger.debug(msg)
                    else:
                        num_cores = []
                        queue_processing_type = {}
                        for k in self.core_to_queues:
                            key = str(k)
                            num_cores.append(key)
                            if key not in ['any']:
                                queues = self.core_to_queues[k].get('queues', [])
                                processing_type = self.core_to_queues[k].get('processing_type', '')
                                for q in queues:
                                    queue_processing_type[q] = processing_type

                        if str(task_param['coreCount']) in num_cores:
                            p_type = self.core_to_queues.get(str(task_param['coreCount']), {}).get('processing_type', None)
                            if p_type and not task_param['processingType']:
                                msg = f"processingType is not defined, set it to {p_type} based on coreCount {task_param['coreCount']}"
                                task_param['processingType'] = p_type
                                self.logger.warn(msg)
                            if 'site' in task_param and task_param['site']:
                                for q in queue_processing_type:
                                    if task_param['site'] in q or q in task_param['site']:
                                        p_type = queue_processing_type[q]
                                        if p_type:
                                            msg = f"processingType is not defined, set it to {p_type} based on site {task_param['site']}"
                                            task_param['processingType'] = p_type
                                            self.logger.debug(msg)
                        else:
                            if 'site' in task_param and task_param['site']:
                                for q in queue_processing_type:
                                    if task_param['site'] in q or q in task_param['site']:
                                        p_type = queue_processing_type[q]
                                        if p_type:
                                            msg = f"processingType is not defined, set it to {p_type} based on site {task_param['site']}"
                                            task_param['processingType'] = p_type
                                            self.logger.debug(msg)
                            else:
                                site = 'any'
                                p_type = self.core_to_queues.get(site, {}).get('processing_type', None)
                                if p_type:
                                    msg = f"processingType is not defined, set it to {p_type} based on site 'any'"
                                    task_param['processingType'] = p_type
                                    self.logger.debug(msg)
                except Exception as ex:
                    self.logger.warn(f"failed to set task parameter map with core_to_queues: {ex}")

            if self.has_dependency():
                parent_tid = None
                self.logger.info("parent_workload_id: %s" % self.parent_workload_id)
                if self.parent_workload_id and int(self.parent_workload_id) < time.time() - 604800:
                    parent_tid = self.parent_workload_id
                    parent_tid = None       # disable parent_tid for now
                return_code = Client.insertTaskParams(task_param, verbose=True, parent_tid=parent_tid)
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
        # if proc.workload_id:
        if False:
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

            # proc = processing['processing_metadata']['processing']
            status, task_status = Client.getTaskStatus(processing['workload_id'])
            if status == 0:
                return task_status
        else:
            return 'failed'
        return None

    def get_processing_status_from_panda_status(self, task_status):
        if task_status in ['registered', 'defined', 'assigning']:
            processing_status = ProcessingStatus.Submitting
        elif task_status in ['ready', 'scouting', 'scouted', 'prepared', 'topreprocess', 'preprocessing']:
            processing_status = ProcessingStatus.Submitting
        elif task_status in ['pending']:
            processing_status = ProcessingStatus.Submitted
        elif task_status in ['running', 'toretry', 'toincexec', 'throttled']:
            processing_status = ProcessingStatus.Running
        elif task_status in ['done']:
            processing_status = ProcessingStatus.Finished
        elif task_status in ['finished', 'paused']:
            # finished, finishing, waiting it to be done
            processing_status = ProcessingStatus.SubFinished
        elif task_status in ['failed', 'exhausted']:
            # aborting, tobroken
            processing_status = ProcessingStatus.Failed
        elif task_status in ['aborted']:
            # aborting, tobroken
            processing_status = ProcessingStatus.Cancelled
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
                                      'request_id': content['request_id'],
                                      'status': ContentStatus.New,
                                      'substatus': ContentStatus.New}
                    updated_contents.append(update_content)
                for content in inputs_dependency:
                    if content['status'] not in [ContentStatus.Available]:
                        update_content = {'content_id': content['content_id'],
                                          'request_id': content['request_id'],
                                          'status': ContentStatus.New,
                                          'substatus': ContentStatus.New}
                        updated_contents.append(update_content)
        return updated_contents

    def get_content_status_from_panda_status(self, job_info):
        if job_info is None:
            return ContentStatus.Processing

        jobstatus = job_info.jobStatus
        if not job_info.eventService or job_info.eventService in ['NULL', 'None']:
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
        else:
            # job_info.eventService is 6
            jobsubstatus = job_info.jobSubStatus
            if jobstatus in ['finished', 'merging']:
                if jobsubstatus in ['fg_done']:
                    return ContentStatus.Available
                elif jobsubstatus in ['fg_partial']:
                    attempt_nr = int(job_info.attemptNr) if job_info.attemptNr else 0
                    max_attempt = int(job_info.maxAttempt) if job_info.maxAttempt else 0
                    self_maxAttempt = int(self.maxAttempt) if self.maxAttempt else 0
                    if (attempt_nr >= max_attempt) and (attempt_nr >= self_maxAttempt):
                        return ContentStatus.FinalSubAvailable
                    else:
                        return ContentStatus.SubAvailable
                else:
                    attempt_nr = int(job_info.attemptNr) if job_info.attemptNr else 0
                    max_attempt = int(job_info.maxAttempt) if job_info.maxAttempt else 0
                    self_maxAttempt = int(self.maxAttempt) if self.maxAttempt else 0
                    if (attempt_nr >= max_attempt) and (attempt_nr >= self_maxAttempt):
                        return ContentStatus.FinalSubAvailable
                    else:
                        return ContentStatus.SubAvailable
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

    def get_job_status_from_contents(self, contents, contents_ext_dict):
        all_finished, all_terminated, has_finished, panda_id = True, True, False, None
        for content in contents:
            if content['substatus'] in [ContentStatus.Available]:
                has_finished = True
            else:
                all_finished = False
                if content['substatus'] in [ContentStatus.FinalFailed,
                                            ContentStatus.Lost,
                                            ContentStatus.Deleted,
                                            ContentStatus.Missing]:
                    pass
                else:
                    all_terminated = False
                    break

            if 'panda_id' in content['content_metadata']:
                panda_id = content['content_metadata']['panda_id']
            else:
                all_finished = False
                all_terminated = False
                break

            if content['content_id'] not in contents_ext_dict:
                all_finished = False
                all_terminated = False
                break

            content_ext = contents_ext_dict[content['content_id']]
            if content['substatus'] != content_ext['status'] or str(panda_id) != str(content_ext['panda_id']):
                all_finished = False
                all_terminated = False
                break

        return all_finished, all_terminated, has_finished, panda_id

    def get_unterminated_jobs(self, all_jobs_ids, input_output_maps, contents_ext):
        finished_jobs, sub_finished_jobs, failed_jobs = [], [], []

        contents_ext_dict = {content['content_id']: content for content in contents_ext}

        for map_id in input_output_maps:
            outputs = input_output_maps[map_id]['outputs']
            all_finished, all_terminated, has_finished, panda_id = self.get_job_status_from_contents(outputs, contents_ext_dict)
            if all_finished:
                if panda_id not in finished_jobs:
                    finished_jobs.append(panda_id)
            else:
                if all_terminated:
                    if has_finished:
                        if panda_id not in sub_finished_jobs:
                            sub_finished_jobs.append(panda_id)
                    else:
                        if panda_id not in failed_jobs:
                            failed_jobs.append(panda_id)

        all_jobs_ids = set(all_jobs_ids)
        terminated_jobs = finished_jobs + failed_jobs + sub_finished_jobs
        terminated_jobs_final = []
        for job_id in terminated_jobs:
            job_ids = [int(i) for i in str(job_id).split(",")]
            terminated_jobs_final.extend(job_ids)
        terminated_jobs_final = set(terminated_jobs_final)
        unterminated_jobs = all_jobs_ids - terminated_jobs_final
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

    def get_last_job_info(self, jobs):
        # job = {'panda_id': job_info.PandaID, 'status': job_status, 'job_info': job_info}
        panda_ids = []
        job_status, last_job_info = None, None
        for job in jobs:
            panda_id = job['panda_id']
            status = job['status']
            job_info = job['job_info']
            if status in [ContentStatus.Available, ContentStatus.FinalSubAvailable, ContentStatus.FinalFailed]:
                if panda_id not in panda_ids:
                    panda_ids.append(panda_id)
                if job_status is None:
                    job_status = status
                    last_job_info = job_info
                else:
                    if status in [ContentStatus.Available]:
                        job_status = status
                        last_job_info = job_info
                    elif job_status in [ContentStatus.Available]:
                        pass
                    elif status in [ContentStatus.FinalSubAvailable]:
                        job_status = status
                        last_job_info = job_info
                    elif job_status in [ContentStatus.FinalSubAvailable]:
                        pass
                    elif status in [ContentStatus.FinalFailed]:
                        job_status = status
                        last_job_info = job_info
                    elif job_status in [ContentStatus.FinalFailed]:
                        pass
        return sorted(panda_ids), job_status, last_job_info

    def get_panda_event_status(self, jobids, log_prefix=''):
        self.logger.debug(log_prefix + "get_panda_event_status, jobids[:3]: %s" % str(jobids[:3]))
        try:
            from pandaclient import Client
            ret = Client.get_events_status(jobids, verbose=True)
            if ret[0] == 0:
                job_events_status = ret[1]
                return job_events_status
        except Exception as ex:
            self.logger.error(str(ex))
            self.logger.error(traceback.format_exc())
        return {}

    def poll_panda_events(self, event_ids, log_prefix=''):
        self.logger.debug(log_prefix + "poll_panda_events, poll_panda_jobs_chunk_size: %s, event_ids[:3]: %s" % (self.poll_panda_jobs_chunk_size, str(event_ids[:3])))
        chunksize = self.poll_panda_jobs_chunk_size
        chunks = [event_ids[i:i + chunksize] for i in range(0, len(event_ids), chunksize)]
        jobs_event_status = {}
        for chunk in chunks:
            job_event_status = self.get_panda_event_status(chunk, log_prefix=log_prefix)
            jobs_event_status.update(job_event_status)
        return jobs_event_status

    def poll_panda_jobs(self, job_ids, executors=None, log_prefix=''):
        job_status_info = {}
        self.logger.debug(log_prefix + "poll_panda_jobs, poll_panda_jobs_chunk_size: %s, job_ids[:10]: %s" % (self.poll_panda_jobs_chunk_size, str(job_ids[:10])))
        chunksize = self.poll_panda_jobs_chunk_size
        chunks = [job_ids[i:i + chunksize] for i in range(0, len(job_ids), chunksize)]
        if executors is None:
            for chunk in chunks:
                # jobs_list = Client.getJobStatus(chunk, verbose=0)[1]
                jobs_list = self.get_panda_job_status(chunk, log_prefix=log_prefix)
                if jobs_list:
                    self.logger.debug(log_prefix + "poll_panda_jobs, input jobs: %s, output_jobs: %s" % (len(chunk), len(jobs_list)))
                    for job_info in jobs_list:
                        job_set_id = job_info.jobsetID
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
                                # job_status_info[input_file] = {'panda_id': job_info.PandaID, 'status': job_status, 'job_info': job_info}
                                if input_file not in job_status_info:
                                    job_status_info[input_file] = {'job_set_id': job_set_id, 'jobs': []}
                                job_status_info[input_file]['jobs'].append({'panda_id': job_info.PandaID, 'status': job_status, 'job_info': job_info})
                else:
                    self.logger.warn(log_prefix + "poll_panda_jobs, input jobs: %s, output_jobs: %s" % (len(chunk), jobs_list))
        else:
            ret_futures = set()
            for chunk in chunks:
                f = executors.submit(self.get_panda_job_status, chunk, log_prefix)
                ret_futures.add(f)
            # Wait for all subprocess to complete
            steps = 0
            while True:
                steps += 1
                # Wait for all subprocess to complete in 3 minutes
                completed, _ = concurrent.futures.wait(ret_futures, timeout=180, return_when=concurrent.futures.ALL_COMPLETED)
                for f in completed:
                    jobs_list = f.result()
                    if jobs_list:
                        self.logger.debug(log_prefix + "poll_panda_jobs thread, input jobs: %s, output_jobs: %s" % (len(chunk), len(jobs_list)))
                        for job_info in jobs_list:
                            job_set_id = job_info.jobsetID
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
                                    # job_status_info[input_file] = {'panda_id': job_info.PandaID, 'status': job_status, 'job_info': job_info}
                                    if input_file not in job_status_info:
                                        job_status_info[input_file] = {'job_set_id': job_set_id, 'jobs': []}
                                    job_status_info[input_file]['jobs'].append({'panda_id': job_info.PandaID, 'status': job_status, 'job_info': job_info})
                    else:
                        self.logger.warn(log_prefix + "poll_panda_jobs thread, input jobs: %s, output_jobs: %s" % (len(chunk), jobs_list))

                ret_futures = ret_futures - completed
                if len(ret_futures) > 0:
                    self.logger.debug(log_prefix + "poll_panda_jobs thread: %s threads has been running for more than %s minutes" % (len(ret_futures), steps * 3))
                else:
                    break

        if not self.es:
            for filename in job_status_info:
                job_set_id = job_status_info[filename]['job_set_id']
                jobs = job_status_info[filename]['jobs']
                panda_ids, status, job_info = self.get_last_job_info(jobs)
                if status:
                    job_status_info[filename]['status'] = status
                    job_status_info[filename]['job_info'] = job_info
                    job_status_info[filename]['panda_id'] = panda_ids
        else:
            es_job_ids = []
            self.logger.debug("job_status_info: %s" % (job_status_info))
            for filename in job_status_info:
                job_set_id = job_status_info[filename]['job_set_id']
                jobs = job_status_info[filename]['jobs']
                panda_ids, status, job_info = self.get_last_job_info(jobs)
                if status:
                    job_status_info[filename]['status'] = status
                    job_status_info[filename]['job_info'] = job_info
                    job_status_info[filename]['panda_id'] = panda_ids
                if status in [ContentStatus.FinalSubAvailable, ContentStatus.FinalFailed]:
                    task_id = job_info.jediTaskID
                    es_job_id = {'task_id': task_id, 'panda_id': job_set_id}
                    es_job_ids.append(es_job_id)
                    for panda_id in panda_ids:
                        es_job_id = {'task_id': task_id, 'panda_id': panda_id}
                        es_job_ids.append(es_job_id)
            job_events_status = self.poll_panda_events(es_job_ids)
            self.logger.debug("poll_panda_events, es_job_ids: %s, job_events_status: %s" % (str(es_job_ids), job_events_status))
            for filename in job_status_info:
                jobs = job_status_info[filename]['jobs']
                for job in jobs:
                    panda_id = job['panda_id']
                    events = job_events_status.get(str(panda_id), {})
                    job['events'] = events
                job_set_id = job_status_info[filename]['job_set_id']
                job_status_info[filename]['job_set_events'] = job_events_status.get(str(job_set_id), {})
            self.logger.debug("job_status_info: %s" % (job_status_info))
        return job_status_info

    def get_event_job(self, sub_map_id, panda_jobs, job_set_events):
        ret_event, ret_job = {}, None
        sub_map_id_jobs = {}
        for panda_job in panda_jobs:
            events = panda_job.get('events', {})
            for event_id in events:
                event_index = int(event_id.split('-')[3]) - 1
                if event_index == sub_map_id:
                    event_result = events[event_id]
                    if type(event_result) in [dict]:
                        # new version of panda result
                        event_status = event_result.get('status', None)
                        event_error = event_result.get('error', None)
                        event_diag = event_result.get('diag', None)
                    else:
                        event_status = event_result
                        event_error, event_diag = None, None
                    if event_status not in sub_map_id_jobs:
                        sub_map_id_jobs[event_status] = []
                    # todo: get the event error code and error diag
                    item = {'status': event_status, 'error_code': event_error, 'error_diag': event_diag, 'job': panda_job}
                    sub_map_id_jobs[event_status].append(item)

        if not ret_event:
            for event_id in job_set_events:
                event_index = int(event_id.split('-')[3]) - 1
                if event_index == sub_map_id:
                    event_result = job_set_events[event_id]
                    if type(event_result) in [dict]:
                        # new version of panda result
                        event_status = event_result.get('status', None)
                        event_error = event_result.get('error', None)
                        event_diag = event_result.get('diag', None)
                    else:
                        event_status = event_result
                        event_error, event_diag = None, None
                    if event_status not in sub_map_id_jobs:
                        sub_map_id_jobs[event_status] = []
                    # todo: get the event error code and error diag
                    item = {'status': event_status, 'error_code': event_error, 'error_diag': event_diag}
                    sub_map_id_jobs[event_status].append(item)

        final_event_status = None
        for event_status in sub_map_id_jobs:
            if event_status in ['finished', 'done', 'merged']:
                final_event_status = event_status
                break
            elif event_status in ['failed', 'fatal', 'cancelled', 'discarded', 'corrupted']:
                final_event_status = event_status
            else:
                if final_event_status is None:
                    final_event_status = event_status
        if final_event_status:
            item = sub_map_id_jobs[final_event_status][0]
            ret_event['status'] = item['status']
            # todo: get the event error code and error diag
            ret_event['error_code'] = item['error_code']
            ret_event['error_diag'] = item['error_diag']
            ret_job = item.get('job', None)
        return ret_event, ret_job

    def get_update_contents(self, unterminated_jobs_status, input_output_maps, contents_ext, job_info_maps, abort=False, terminated_status=False, log_prefix=''):
        inputname_to_map_id_outputs = {}
        for map_id in input_output_maps:
            inputs = input_output_maps[map_id]['inputs']
            outputs = input_output_maps[map_id]['outputs']
            if not self.es:
                for content in inputs:
                    if content['name'] not in inputname_to_map_id_outputs:
                        inputname_to_map_id_outputs[content['name']] = []
                    inputname_to_map_id_outputs[content['name']].append({'map_id': map_id, 'outputs': outputs})
            else:
                # es_name = input_output_maps[map_id]['es_name']
                # sub_maps = input_output_maps[map_id]['sub_maps']
                if inputs:
                    es_name = inputs[0]['path']
                    es_name = es_name.split("^")[0]
                    if es_name not in inputname_to_map_id_outputs:
                        inputname_to_map_id_outputs[es_name] = []
                    inputname_to_map_id_outputs[es_name].append({'map_id': map_id, 'outputs': outputs, 'inputs': inputs})

        contents_ext_dict = {content['content_id']: content for content in contents_ext}

        update_contents, update_contents_full = [], []
        new_contents_ext, update_contents_ext = [], []
        update_contents_dict, new_contents_ext_dict = {}, {}

        if not self.es:
            for input_file in unterminated_jobs_status:
                # job_set_id = unterminated_jobs_status[input_file]['job_set_id']
                panda_jobs = unterminated_jobs_status[input_file]['jobs']
                if 'status' not in unterminated_jobs_status[input_file]:
                    continue
                panda_status = unterminated_jobs_status[input_file]['status']
                panda_ids = unterminated_jobs_status[input_file]['panda_id']
                panda_id = ",".join([str(i) for i in panda_ids])
                job_info = unterminated_jobs_status[input_file]['job_info']

                if input_file not in inputname_to_map_id_outputs:
                    continue

                # output_contents = inputname_to_map_id_outputs[input_file]['outputs']
                map_id_outputs = inputname_to_map_id_outputs[input_file]
                for map_id_output in map_id_outputs:
                    # map_id = map_id_output['map_id']
                    output_contents = map_id_output['outputs']

                    for content in output_contents:
                        content['status'] = panda_status
                        content['substatus'] = panda_status
                        update_contents_full.append(content)
                        update_content = {'content_id': content['content_id'],
                                          'request_id': content['request_id'],
                                          'status': panda_status,
                                          'substatus': panda_status}

                        if 'panda_id' in content['content_metadata'] and content['content_metadata']['panda_id']:
                            if str(content['content_metadata']['panda_id']) < str(panda_id):
                                # new panda id is the bigger one.
                                if 'old_panda_id' not in content['content_metadata']:
                                    content['content_metadata']['old_panda_id'] = []
                                if content['content_metadata']['panda_id'] not in content['content_metadata']['old_panda_id']:
                                    content['content_metadata']['old_panda_id'].append(content['content_metadata']['panda_id'])
                                content['content_metadata']['panda_id'] = str(panda_id)
                                update_content['content_metadata'] = content['content_metadata']
                            elif str(content['content_metadata']['panda_id']) > str(panda_id):
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
                            content['content_metadata']['panda_id'] = str(panda_id)
                            update_content['content_metadata'] = content['content_metadata']

                        update_contents.append(update_content)
                        update_contents_dict[update_content['content_id']] = update_content

                        if panda_status in [ContentStatus.Available, ContentStatus.Failed, ContentStatus.FinalFailed,
                                            ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                            if content['content_id'] not in contents_ext_dict:
                                new_content_ext = {'content_id': content['content_id'],
                                                   'request_id': content['request_id'],
                                                   'transform_id': content['transform_id'],
                                                   'workload_id': content['workload_id'],
                                                   'coll_id': content['coll_id'],
                                                   'map_id': content['map_id'],
                                                   'status': panda_status}
                                for job_info_item in job_info_maps:
                                    new_content_ext[job_info_item] = getattr(job_info, job_info_maps[job_info_item])
                                    if new_content_ext[job_info_item] == 'NULL':
                                        new_content_ext[job_info_item] = None
                                    if new_content_ext[job_info_item] is None:
                                        del new_content_ext[job_info_item]
                                new_contents_ext.append(new_content_ext)
                                new_contents_ext_dict[new_content_ext['content_id']] = new_content_ext
                            else:
                                update_content_ext = {'content_id': content['content_id'],
                                                      'request_id': content['request_id'],
                                                      'status': panda_status}
                                for job_info_item in job_info_maps:
                                    update_content_ext[job_info_item] = getattr(job_info, job_info_maps[job_info_item])
                                    if update_content_ext[job_info_item] == 'NULL':
                                        update_content_ext[job_info_item] = None
                                    if update_content_ext[job_info_item] is None:
                                        del update_content_ext[job_info_item]
                                update_contents_ext.append(update_content_ext)
        else:
            # ES jobs
            for input_file in unterminated_jobs_status:
                # job_set_id = unterminated_jobs_status[input_file]['job_set_id']
                panda_jobs = unterminated_jobs_status[input_file]['jobs']
                job_set_events = unterminated_jobs_status[input_file]['job_set_events']
                if 'status' not in unterminated_jobs_status[input_file]:
                    continue

                panda_status = unterminated_jobs_status[input_file]['status']
                panda_ids = unterminated_jobs_status[input_file]['panda_id']
                panda_id = ",".join([str(i) for i in panda_ids])
                job_info = unterminated_jobs_status[input_file]['job_info']

                if input_file not in inputname_to_map_id_outputs:
                    continue

                if panda_status in [ContentStatus.Available, ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                    # output_contents = inputname_to_map_id_outputs[input_file]['outputs']
                    map_id_outputs = inputname_to_map_id_outputs[input_file]
                    for map_id_output in map_id_outputs:
                        # map_id = map_id_output['map_id']
                        output_contents = map_id_output['outputs']

                        for content in output_contents:
                            content['status'] = panda_status
                            content['substatus'] = panda_status
                            update_contents_full.append(content)
                            update_content = {'content_id': content['content_id'],
                                              'request_id': content['request_id'],
                                              'status': panda_status,
                                              'substatus': panda_status}

                            if 'panda_id' in content['content_metadata'] and content['content_metadata']['panda_id']:
                                if str(content['content_metadata']['panda_id']) < str(panda_id):
                                    # new panda id is the bigger one.
                                    if 'old_panda_id' not in content['content_metadata']:
                                        content['content_metadata']['old_panda_id'] = []
                                    if content['content_metadata']['panda_id'] not in content['content_metadata']['old_panda_id']:
                                        content['content_metadata']['old_panda_id'].append(content['content_metadata']['panda_id'])
                                    content['content_metadata']['panda_id'] = str(panda_id)
                                    update_content['content_metadata'] = content['content_metadata']
                                elif str(content['content_metadata']['panda_id']) > str(panda_id):
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
                                content['content_metadata']['panda_id'] = str(panda_id)
                                update_content['content_metadata'] = content['content_metadata']

                            update_contents.append(update_content)
                            update_contents_dict[update_content['content_id']] = update_content

                            if content['content_id'] not in contents_ext_dict:
                                new_content_ext = {'content_id': content['content_id'],
                                                   'request_id': content['request_id'],
                                                   'transform_id': content['transform_id'],
                                                   'workload_id': content['workload_id'],
                                                   'coll_id': content['coll_id'],
                                                   'map_id': content['map_id'],
                                                   'status': panda_status}
                                for job_info_item in job_info_maps:
                                    new_content_ext[job_info_item] = getattr(job_info, job_info_maps[job_info_item])
                                    if new_content_ext[job_info_item] == 'NULL':
                                        new_content_ext[job_info_item] = None
                                    if new_content_ext[job_info_item] is None:
                                        del new_content_ext[job_info_item]
                                new_contents_ext.append(new_content_ext)
                                new_contents_ext_dict[new_content_ext['content_id']] = new_content_ext
                            else:
                                update_content_ext = {'content_id': content['content_id'],
                                                      'request_id': content['request_id'],
                                                      'status': panda_status}
                                for job_info_item in job_info_maps:
                                    update_content_ext[job_info_item] = getattr(job_info, job_info_maps[job_info_item])
                                    if update_content_ext[job_info_item] == 'NULL':
                                        update_content_ext[job_info_item] = None
                                    if update_content_ext[job_info_item] is None:
                                        del update_content_ext[job_info_item]
                                update_contents_ext.append(update_content_ext)
                elif panda_status in [ContentStatus.FinalSubAvailable, ContentStatus.FinalFailed]:
                    # partly finished or all failed, needs to check the event status
                    # output_contents = inputname_to_map_id_outputs[input_file]['outputs']
                    map_id_outputs = inputname_to_map_id_outputs[input_file]
                    for map_id_output in map_id_outputs:
                        # map_id = map_id_output['map_id']
                        output_contents = map_id_output['outputs']

                        for content in output_contents:
                            sub_map_id = content['sub_map_id']
                            # min_id = content['min_id']  # min_id should be the same as sub_map_id here
                            event, event_panda_job = self.get_event_job(sub_map_id, panda_jobs, job_set_events)
                            self.logger.debug("sub_map_id: %s, panda_jobs: %s, job_set_events: %s, event: %s, event_panda_job: %s" % (sub_map_id, panda_jobs, job_set_events, event, event_panda_job))
                            if event:
                                event_status = event['status']
                                # 'ready', 'sent', 'running', 'finished', 'cancelled', 'discarded', 'done', 'failed',
                                # 'fatal', 'merged', 'corrupted', 'reserved_fail', 'reserved_get'
                                if event_status in ['finished', 'done', 'merged']:
                                    event_status = ContentStatus.Available
                                    event_error_code = event['error_code']
                                    event_error_diag = event['error_diag']
                                    if event_panda_job:
                                        panda_id = event_panda_job['panda_id']
                                        job_info = event_panda_job['job_info']
                                    else:
                                        panda_ids = unterminated_jobs_status[input_file]['panda_id']
                                        panda_id = ",".join([str(i) for i in panda_ids])
                                        job_info = unterminated_jobs_status[input_file]['job_info']
                                elif event_status in ['failed', 'fatal', 'cancelled', 'discarded', 'corrupted']:
                                    event_status = ContentStatus.FinalFailed
                                    event_error_code = event['error_code']
                                    event_error_diag = event['error_diag']
                                    if event_panda_job:
                                        panda_id = event_panda_job['panda_id']
                                        job_info = event_panda_job['job_info']
                                    else:
                                        panda_ids = unterminated_jobs_status[input_file]['panda_id']
                                        panda_id = ",".join([str(i) for i in panda_ids])
                                        job_info = unterminated_jobs_status[input_file]['job_info']
                                else:
                                    event_status = panda_status
                                    event_error_code = None
                                    event_error_diag = None
                                    panda_ids = unterminated_jobs_status[input_file]['panda_id']
                                    panda_id = ",".join([str(i) for i in panda_ids])
                                    job_info = unterminated_jobs_status[input_file]['job_info']
                            else:
                                event_status = panda_status
                                event_error_code = None
                                event_error_diag = None

                                panda_ids = unterminated_jobs_status[input_file]['panda_id']
                                panda_id = ",".join([str(i) for i in panda_ids])
                                job_info = unterminated_jobs_status[input_file]['job_info']

                            content['status'] = event_status
                            content['substatus'] = event_status
                            update_contents_full.append(content)
                            update_content = {'content_id': content['content_id'],
                                              'request_id': content['request_id'],
                                              'status': panda_status,
                                              'substatus': event_status}

                            if 'panda_id' in content['content_metadata'] and content['content_metadata']['panda_id']:
                                if str(content['content_metadata']['panda_id']) < str(panda_id):
                                    # new panda id is the bigger one.
                                    if 'old_panda_id' not in content['content_metadata']:
                                        content['content_metadata']['old_panda_id'] = []
                                    if content['content_metadata']['panda_id'] not in content['content_metadata']['old_panda_id']:
                                        content['content_metadata']['old_panda_id'].append(content['content_metadata']['panda_id'])
                                    content['content_metadata']['panda_id'] = str(panda_id)
                                    update_content['content_metadata'] = content['content_metadata']
                                elif str(content['content_metadata']['panda_id']) > str(panda_id):
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
                                content['content_metadata']['panda_id'] = str(panda_id)
                                update_content['content_metadata'] = content['content_metadata']

                            update_contents.append(update_content)
                            update_contents_dict[update_content['content_id']] = update_content

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
                                if event_error_code is not None:
                                    new_content_ext['trans_exit_code'] = event_error_code
                                    new_content_ext['exe_exit_code'] = event_error_code
                                if event_error_diag is not None:
                                    new_content_ext['exe_exit_diag'] = event_error_diag
                                new_contents_ext.append(new_content_ext)
                                new_contents_ext_dict[new_content_ext['content_id']] = new_content_ext
                            else:
                                update_content_ext = {'content_id': content['content_id'],
                                                      'request_id': content['request_id'],
                                                      'status': event_status}
                                for job_info_item in job_info_maps:
                                    update_content_ext[job_info_item] = getattr(job_info, job_info_maps[job_info_item])
                                    if update_content_ext[job_info_item] == 'NULL':
                                        update_content_ext[job_info_item] = None
                                    if update_content_ext[job_info_item] is None:
                                        del update_content_ext[job_info_item]
                                if event_error_code is not None:
                                    update_content_ext['trans_exit_code'] = event_error_code
                                    update_content_ext['exe_exit_code'] = event_error_code
                                if event_error_diag is not None:
                                    update_content_ext['exe_exit_diag'] = event_error_diag
                                update_contents_ext.append(update_content_ext)

        if abort or terminated_status:
            for map_id in input_output_maps:
                outputs = input_output_maps[map_id]['outputs']
                for content in outputs:
                    if content['substatus'] not in [ContentStatus.Available, ContentStatus.Failed, ContentStatus.FinalFailed,
                                                    ContentStatus.Lost, ContentStatus.Deleted, ContentStatus.Missing]:
                        if content['content_id'] not in update_contents_dict:
                            update_content = {'content_id': content['content_id'],
                                              'request_id': content['request_id'],
                                              'status': ContentStatus.Missing,
                                              'substatus': ContentStatus.Missing}
                            update_contents.append(update_content)
                        if content['content_id'] not in contents_ext_dict and content['content_id'] not in new_contents_ext_dict:
                            new_content_ext = {'content_id': content['content_id'],
                                               'request_id': content['request_id'],
                                               'transform_id': content['transform_id'],
                                               'workload_id': content['workload_id'],
                                               'coll_id': content['coll_id'],
                                               'map_id': content['map_id'],
                                               'status': ContentStatus.Missing}
                            new_contents_ext.append(new_content_ext)

        self.logger.debug("get_update_contents, num_update_contents: %s" % (len(update_contents)))
        self.logger.debug("get_update_contents, update_contents[:3]: %s" % (str(update_contents[:3])))
        self.logger.debug("get_update_contents, new_contents_ext[:1]: %s" % (str(new_contents_ext[:1])))
        self.logger.debug("get_update_contents, update_contents_ext[:1]: %s" % (str(update_contents_ext[:1])))

        return update_contents, update_contents_full, new_contents_ext, update_contents_ext

    def poll_panda_task(self, processing=None, input_output_maps=None, contents_ext=None, job_info_maps={}, executors=None, log_prefix=''):
        task_id = None
        try:
            from pandaclient import Client

            if processing:
                # proc = processing['processing_metadata']['processing']
                # task_id = proc.workload_id
                task_id = processing['workload_id']
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

                    unterminated_jobs_status = self.poll_panda_jobs(unterminated_jobs, executors=executors, log_prefix=log_prefix)
                    self.logger.debug(log_prefix + "unterminated_jobs_status: %s" % str(unterminated_jobs_status))

                    abort_status = False
                    if processing_status in [ProcessingStatus.Cancelled]:
                        abort_status = True
                    terminated_status = False
                    if processing_status in [ProcessingStatus.Cancelled, ProcessingStatus.Failed, ProcessingStatus.Broken]:
                        terminated_status = True
                    ret_contents = self.get_update_contents(unterminated_jobs_status, input_output_maps, contents_ext, job_info_maps,
                                                            abort=abort_status, terminated_status=terminated_status, log_prefix=log_prefix)
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

    def kill_processing(self, processing, log_prefix=''):
        try:
            if processing:
                from pandaclient import Client
                # proc = processing['processing_metadata']['processing']
                # task_id = proc.workload_id
                task_id = processing['workload_id']
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
                # proc = processing['processing_metadata']['processing']
                # task_id = proc.workload_id
                task_id = processing['workload_id']
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
                # proc = processing['processing_metadata']['processing']
                # task_id = proc.workload_id
                task_id = processing['workload_id']

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
        try:
            has_task = False
            if processing:
                # proc = processing['processing_metadata']['processing']
                # task_id = proc.workload_id
                task_id = processing['workload_id']
                if task_id:
                    has_task = True
                    self.kill_processing_force(processing, log_prefix=log_prefix)

            if not has_task:
                self.status = WorkStatus.Failed
        except Exception as ex:
            msg = "Failed to abort the processing (%s) status: %s" % (str(processing['processing_id']), str(ex))
            # raise exceptions.IDDSException(msg)
            self.logger.error(log_prefix + msg)

    def resume_processing(self, processing, log_prefix=''):
        self.reactivate_processing(processing, log_prefix=log_prefix)

    def require_ext_contents(self):
        return True

    def has_external_content_id(self):
        return True

    def get_external_content_ids(self, processing, log_prefix=''):
        if processing:
            from pandaclient import Client
            # proc = processing['processing_metadata']['processing']
            # task_id = proc.workload_id
            task_id = processing['workload_id']
            status, output = Client.get_files_in_datasets(task_id, verbose=False)
            if status == 0:
                return output
        return []

    def poll_processing_updates(self, processing, input_output_maps, contents_ext=None, job_info_maps={}, executors=None, log_prefix=''):
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
                                                       executors=executors,
                                                       log_prefix=log_prefix)

            processing_status, update_contents, update_contents_full, new_contents_ext, update_contents_ext = ret_poll_panda_task
            self.logger.debug(log_prefix + "poll_processing_updates, processing_status: %s" % str(processing_status))
            self.logger.debug(log_prefix + "poll_processing_updates, update_contents[:3]: %s" % str(update_contents[:3]))

            if update_contents:
                proc.has_new_updates()
        return processing_status, update_contents, {}, update_contents_full, {}, new_contents_ext, update_contents_ext

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
