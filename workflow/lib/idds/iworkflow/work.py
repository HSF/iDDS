#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2025
# - Lino Oscar Gerlach, <lino.oscar.gerlach@cern.ch>, 2024

import base64
import copy
import datetime
import functools
import json
import logging
import os
import pickle
import time
import traceback
import zlib

from idds.common import exceptions
from idds.common.constants import WorkflowType, TransformStatus, AsyncResultStatus
from idds.common.imports import get_func_name
from idds.common.utils import setup_logging, json_dumps, json_loads, encode_base64, modified_environ, is_panda_client_verbose
from .asyncresult import AsyncResult, MapResult
from .base import Base, Context
from .workflow import WorkflowCanvas

setup_logging(__name__)


class WorkContext(Context):

    def __init__(self, name=None, workflow_context=None, source_dir=None, init_env=None, container_options=None):
        super(WorkContext, self).__init__()
        self._workflow_context = workflow_context
        self._transform_id = None
        self._processing_id = None
        self._workflow_type = WorkflowType.iWork

        self._name = name
        self._site = None
        self._queue = None

        self._priority = 500
        self._core_count = 1
        self._total_memory = None        # MB
        self._max_walltime = 7 * 24 * 3600
        self._max_attempt = 5

        self._map_results = False

        self.init_env = init_env
        self.container_options = container_options

        self._workload_id = None
        self._parent_workload_id = None
        self._no_wait_parent = None

        self._other_attributes = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_service(self):
        return self._workflow_context.service

    @property
    def distributed(self):
        return self._workflow_context.distributed

    @distributed.setter
    def distributed(self, value):
        self._workflow_context.distributed = value

    @property
    def service(self):
        return self._workflow_context.service

    @service.setter
    def service(self, value):
        self._workflow_context.service = value

    @property
    def vo(self):
        return self._workflow_context.vo

    @vo.setter
    def vo(self, value):
        self._workflow_context.vo = value

    @property
    def site(self):
        if self._site:
            return self._site
        return self._workflow_context.site

    @site.setter
    def site(self, value):
        self._site = value

    @property
    def queue(self):
        if self._queue:
            return self._queue
        return self._workflow_context.queue

    @queue.setter
    def queue(self, value):
        self._queue = value

    @property
    def cloud(self):
        return self._workflow_context.cloud

    @cloud.setter
    def cloud(self, value):
        self._workflow_context.cloud = value

    @property
    def working_group(self):
        return self._workflow_context.working_group

    @working_group.setter
    def working_group(self, value):
        self._workflow_context.working_group = value

    @property
    def priority(self):
        if self._priority:
            return self._priority
        return self._workflow_context.priority

    @priority.setter
    def priority(self, value):
        self._priority = value

    @property
    def core_count(self):
        if self._core_count:
            return self._core_count
        return self._workflow_context.core_count

    @core_count.setter
    def core_count(self, value):
        self._core_count = value

    @property
    def total_memory(self):
        if self._total_memory:
            return self._total_memory
        return self._workflow_context.total_memory

    @total_memory.setter
    def total_memory(self, value):
        self._total_memory = value

    @property
    def max_walltime(self):
        if self._max_walltime:
            return self._max_walltime
        return self._workflow_context.max_walltime

    @max_walltime.setter
    def max_walltime(self, value):
        self._max_walltime = value

    @property
    def max_attempt(self):
        if self._max_attempt:
            return self._max_attempt
        return self._workflow_context.max_attempt

    @max_attempt.setter
    def max_attempt(self, value):
        self._max_attempt = value

    @property
    def username(self):
        return self._workflow_context.username

    @username.setter
    def username(self, value):
        self._workflow_context.username = value

    @property
    def userdn(self):
        return self._workflow_context.userdn

    @userdn.setter
    def userdn(self, value):
        self._workflow_context.userdn = value

    @property
    def workflow_type(self):
        return self._workflow_type

    @workflow_type.setter
    def workflow_type(self, value):
        self._workflow_type = value

    @property
    def lifetime(self):
        return self._workflow_context.lifetime

    @lifetime.setter
    def lifetime(self, value):
        self._workflow_context.lifetime = value

    @property
    def request_id(self):
        return self._workflow_context.request_id

    @request_id.setter
    def request_id(self, value):
        self._workflow_context.request_id = value

    @property
    def workload_id(self):
        return self._workload_id

    @workload_id.setter
    def workload_id(self, value):
        self._workload_id = value

    @property
    def parent_workload_id(self):
        return self._parent_workload_id

    @parent_workload_id.setter
    def parent_workload_id(self, value):
        self._parent_workload_id = value

    @property
    def no_wait_parent(self):
        return self._no_wait_parent

    @no_wait_parent.setter
    def no_wait_parent(self, value):
        self._no_wait_parent = value

    @property
    def other_attributes(self):
        return self._other_attributes

    @other_attributes.setter
    def other_attributes(self, value):
        self._other_attributes = value

    @property
    def transform_id(self):
        return self._transform_id

    @transform_id.setter
    def transform_id(self, value):
        self._transform_id = int(value)

    @property
    def processing_id(self):
        return self._processing_id

    @processing_id.setter
    def processing_id(self, value):
        self._processing_id = value

    @property
    def enable_separate_log(self):
        if self._workflow_context:
            return self._workflow_context.enable_separate_log

    @enable_separate_log.setter
    def enable_separate_log(self, value):
        if self._workflow_context:
            self._workflow_context.enable_separate_log = value

    @property
    def brokers(self):
        return self._workflow_context.brokers

    @brokers.setter
    def brokers(self, value):
        self._workflow_context.brokers = value

    @property
    def broker_timeout(self):
        return self._workflow_context.broker_timeout

    @broker_timeout.setter
    def broker_timeout(self, value):
        self._workflow_context.broker_timeout = value

    @property
    def broker_username(self):
        return self._workflow_context.broker_username

    @broker_username.setter
    def broker_username(self, value):
        self._workflow_context.broker_username = value

    @property
    def broker_password(self):
        return self._workflow_context.broker_password

    @broker_password.setter
    def broker_password(self, value):
        self._workflow_context.broker_password = value

    @property
    def broker_destination(self):
        return self._workflow_context.broker_destination

    @broker_destination.setter
    def broker_destination(self, value):
        self._workflow_context.broker_destination = value

    def get_source_dir(self):
        if self._workflow_context:
            return self._workflow_context.get_source_dir()
        return None

    @property
    def token(self):
        return self._workflow_context.token

    @token.setter
    def token(self, value):
        self._workflow_context.token = value

    @property
    def map_results(self):
        return self._map_results

    @map_results.setter
    def map_results(self, value):
        self._map_results = value

    @property
    def init_env(self):
        return self._init_env

    @init_env.setter
    def init_env(self, value):
        self._init_env = value
        if self._init_env:
            self._init_env = self._init_env + " "

    @property
    def container_options(self):
        if self._container_options:
            return self._container_options
        return self._workflow_context.container_options

    @container_options.setter
    def container_options(self, value):
        self._container_options = value

    def get_idds_server(self):
        return self._workflow_context.get_idds_server()

    def init_brokers(self):
        return self._workflow_context.init_brokers()

    def initialize(self):
        return self._workflow_context.initialize()

    def setup_source_files(self):
        """
        Setup source files.
        """
        return self._workflow_context.setup_source_files()

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        if not self.init_env:
            return self._workflow_context.setup()

        global_set_up = self._workflow_context.global_setup()
        init_env = self.init_env
        ret = None
        if global_set_up:
            ret = global_set_up
        if init_env:
            if ret:
                ret = ret + "; " + init_env
            else:
                ret = init_env
        return ret

    def get_clean_env(self):
        return self._workflow_context.get_clean_env()


class Work(Base):

    def __init__(self, func=None, workflow_context=None, context=None, pre_kwargs=None, args=None, kwargs=None, multi_jobs_kwargs_list=None,
                 current_job_kwargs=None, map_results=False, source_dir=None, init_env=None, is_unique_func_name=False, name=None,
                 parent_workload_id=None, no_wait_parent=False, container_options=None, input_datasets=None, output_file_name=None,
                 output_dataset_name=None, num_events=None, num_events_per_job=None, parent_transform_id=None, parent_internal_id=None,
                 log_dataset_name=None, inputs=None, input_map=None, inputs_group=None, enable_separate_log=False, job_key=None,
                 json_load=False):
        """
        Init a workflow.
        """
        super(Work, self).__init__()
        self.prepared = False
        self.logger = logging.getLogger(self.__class__.__name__)

        self._current_job_kwargs = current_job_kwargs
        if self._current_job_kwargs:
            self._current_job_kwargs = base64.b64encode(zlib.compress(pickle.dumps(self._current_job_kwargs))).decode("utf-8")

        if name:
            self._name = name
        else:
            self._name = func.__name__ if func else None
            if self._name:
                self._name = self._name.replace('__main__:', '').replace('.py', '').replace(':', '.')
                self._name = self._name.replace("/", "_").replace(".", "_").replace(":", "_")
            if not is_unique_func_name:
                if self._name:
                    self._name = self._name + "_" + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")

        if context:
            self._context = context
        else:
            self._context = WorkContext(name=self._name, workflow_context=workflow_context, init_env=init_env, container_options=container_options)

        # self._func = func
        self._func, self._func_name_and_args, self._multi_jobs_kwargs_list = self.get_func_name_and_args(
            func=func,
            pre_kwargs=pre_kwargs,
            args=args,
            kwargs=kwargs,
            base_dir=self._context.get_source_dir(),
            multi_jobs_kwargs_list=multi_jobs_kwargs_list
        )
        self._func = None
        if not json_load:
            self.logger.info(f"func: {self._func}, func_name_and_args: {self._func_name_and_args}, multi_jobs_kwargs_list: {self._multi_jobs_kwargs_list}")

        self._async_ret = None

        self.enable_separate_log = enable_separate_log
        self.map_results = map_results
        self._results = None
        self._async_result_initialized = False
        self._async_result_status = None

        self.parent_workload_id = parent_workload_id
        self.no_wait_parent = no_wait_parent

        self.other_attributes = {'input_datasets': input_datasets,
                                 'output_file_name': output_file_name,
                                 'output_dataset_name': output_dataset_name,
                                 'log_dataset_name': log_dataset_name,
                                 'num_events': num_events,
                                 'num_events_per_job': num_events_per_job,
                                 'parent_transform_id': parent_transform_id,
                                 'parent_internal_id': parent_internal_id,
                                 'job_key': job_key}
        self.inputs = inputs
        self.input_map = input_map
        self.inputs_group = inputs_group

        self.num_checks = 0

    @property
    def internal_id(self):
        return self._context.internal_id

    @internal_id.setter
    def internal_id(self, value):
        self._context.internal_id = value

    @property
    def service(self):
        return self._context.service

    @service.setter
    def service(self, value):
        self._context.service = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def request_id(self):
        return self._context.request_id

    @request_id.setter
    def request_id(self, value):
        self._context.request_id = value

    @property
    def transform_id(self):
        return self._context.transform_id

    @transform_id.setter
    def transform_id(self, value):
        self._context.transform_id = int(value)

    @property
    def processing_id(self):
        return self._context.processing_id

    @processing_id.setter
    def processing_id(self, value):
        self._context.processing_id = value

    @property
    def vo(self):
        return self._context.vo

    @vo.setter
    def vo(self, value):
        self._context.vo = value

    @property
    def queue(self):
        return self._context.queue

    @queue.setter
    def queue(self, value):
        self._context.queue = value

    @property
    def site(self):
        return self._context.site

    @site.setter
    def site(self, value):
        self._context.site = value

    def get_site(self):
        return self.site

    @property
    def cloud(self):
        return self._context.cloud

    @cloud.setter
    def cloud(self, value):
        self._context.cloud = value

    @property
    def working_group(self):
        return self._context.working_group

    @working_group.setter
    def working_group(self, value):
        self._context.working_group = value

    @property
    def priority(self):
        return self._context.priority

    @priority.setter
    def priority(self, value):
        self._context.priority = value

    @property
    def core_count(self):
        return self._context.core_count

    @core_count.setter
    def core_count(self, value):
        self._context.core_count = value

    @property
    def total_memory(self):
        return self._context.total_memory

    @total_memory.setter
    def total_memory(self, value):
        self._context.total_memory = value

    @property
    def max_walltime(self):
        return self._context.max_walltime

    @max_walltime.setter
    def max_walltime(self, value):
        self._context.max_walltime = value

    @property
    def max_attempt(self):
        return self._context.max_attempt

    @max_attempt.setter
    def max_attempt(self, value):
        self._context.max_attempt = value

    @property
    def username(self):
        return self._context.username

    @username.setter
    def username(self, value):
        self._context.username = value

    @property
    def userdn(self):
        return self._context.userdn

    @userdn.setter
    def userdn(self, value):
        self._context.userdn = value

    @property
    def workflow_type(self):
        return self._context.workflow_type

    @workflow_type.setter
    def workflow_type(self, value):
        self._context.workflow_type = value

    @property
    def map_results(self):
        return self._context.map_results

    @map_results.setter
    def map_results(self, value):
        self._context.map_results = value

    @property
    def lifetime(self):
        return self._context.lifetime

    @lifetime.setter
    def lifetime(self, value):
        self._context.lifetime = value

    @property
    def workload_id(self):
        return self._context.workload_id

    @workload_id.setter
    def workload_id(self, value):
        self._context.workload_id = value

    def get_workload_id(self):
        return self.workload_id

    @property
    def parent_workload_id(self):
        return self._context.parent_workload_id

    @parent_workload_id.setter
    def parent_workload_id(self, value):
        self._context.parent_workload_id = value

    def get_parent_workload_id(self):
        return self.parent_workload_id

    @property
    def no_wait_parent(self):
        return self._context.no_wait_parent

    @no_wait_parent.setter
    def no_wait_parent(self, value):
        self._context.no_wait_parent = value

    @property
    def enable_separate_log(self):
        return self._context.enable_separate_log

    @enable_separate_log.setter
    def enable_separate_log(self, value):
        self._context.enable_separate_log = value

    @property
    def container_options(self):
        return self._context.container_options

    @container_options.setter
    def container_options(self, value):
        self._context.container_options = value

    @property
    def other_attributes(self):
        return self._context.other_attributes

    @other_attributes.setter
    def other_attributes(self, value):
        self._context.other_attributes = value

    @property
    def token(self):
        return self._context.token

    @token.setter
    def token(self, value):
        self._context.token = value

    @property
    def multi_jobs_kwargs_list(self):
        return self._multi_jobs_kwargs_list

    @multi_jobs_kwargs_list.setter
    def multi_jobs_kwargs_list(self, value):
        raise Exception("Not allwed to update multi_jobs_kwargs_list")

    def get_work_tag(self):
        return self._context.workflow_type.name

    def get_work_type(self):
        return self._context.workflow_type.name

    def get_work_name(self):
        return self._name

    def add_other_attributes(self, other_attributes):
        for k, v in other_attributes.items():
            self._other_attributes[k] = v
        self.other_attributes = self._other_attributes

    def get_parent_transform_id(self):
        if not self.other_attributes:
            return None
        return self.other_attributes.get("parent_transform_id", None)

    def set_other_attribute(self, name, value):
        if self.other_attributes is None:
            self.other_attributes = {}
        self.other_attributes[name] = value

    def get_other_attribute(self, name):
        if not self.other_attributes:
            return None
        return self.other_attributes.get(name, None)

    @property
    def job_key(self):
        return self.get_other_attribute('job_key')

    @job_key.setter
    def job_key(self, value):
        self.set_other_attribute('job_key', value)

    @property
    def parent_internal_id(self):
        return self.get_other_attribute('parent_internal_id')

    @parent_internal_id.setter
    def parent_internal_id(self, value):
        self.set_other_attribute('parent_internal_id', value)

    def get_parent_internal_id(self):
        return self.get_other_attribute('parent_internal_id')

    @property
    def input_datasets(self):
        return self.get_other_attribute('input_datasets')

    @input_datasets.setter
    def input_datasets(self, value):
        self.set_other_attribute('input_datasets', value)

    @property
    def output_file_name(self):
        return self.get_other_attribute('output_file_name')

    @output_file_name.setter
    def output_file_name(self, value):
        self.set_other_attribute('output_file_name', value)

    @property
    def output_dataset_name(self):
        return self.get_other_attribute('output_dataset_name')

    @output_dataset_name.setter
    def output_dataset_name(self, value):
        self.set_other_attribute('output_dataset_name', value)

    @property
    def log_dataset_name(self):
        return self.get_other_attribute('log_dataset_name')

    @log_dataset_name.setter
    def log_dataset_name(self, value):
        self.set_other_attribute('log_dataset_name', value)

    @property
    def num_events(self):
        return self.get_other_attribute('num_events')

    @num_events.setter
    def num_events(self, value):
        self.set_other_attribute('num_events', value)

    @property
    def num_events_per_job(self):
        return self.get_other_attribute('num_events_per_job')

    @num_events_per_job.setter
    def num_events_per_job(self, value):
        self.set_other_attribute('num_events_per_job', value)

    @property
    def parent_transform_id(self):
        return self.get_other_attribute('parent_transform_id')

    @parent_transform_id.setter
    def parent_transform_id(self, value):
        self.set_other_attribute('parent_transform_id', value)

    def to_dict(self):
        func = self._func
        self._func = None
        obj = super(Work, self).to_dict()
        self._func = func
        return obj

    def store(self):
        if self._context:
            content = {'type': 'work',
                       'name': self.name,
                       'context': self._context,
                       'original_args': self._func_name_and_args,
                       'multi_jobs_kwargs_list': self._multi_jobs_kwargs_list,
                       'current_job_kwargs': self._current_job_kwargs}
            content = json_dumps(content)
            source_dir = self._context.get_source_dir()
            self.save_context(source_dir, self._name, content)

    def load(self, source_dir=None):
        if not source_dir:
            source_dir = self._context.get_source_dir()
            if not source_dir:
                source_dir = os.getcwd()
        ret = self.load_context(source_dir, self._name)
        if ret:
            ret = json_loads(ret)
            self.logger.info(f"Loaded context: {ret}")
            if 'multi_jobs_kwargs_list' in ret:
                self._multi_jobs_kwargs_list = ret['multi_jobs_kwargs_list']

    def submit_to_idds_server(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: The workflow id.
        :raise Exception when failing to submit the workflow.
        """
        # iDDS ClientManager
        from idds.client.clientmanager import ClientManager
        client = ClientManager(host=self._context.get_idds_server(), timeout=60)
        request_id = self._context.request_id
        transform_id = client.submit_work(request_id, self, use_dataset_name=False)
        self.logger.info("Submitted into iDDS with transform id=%s", str(transform_id))
        return transform_id

    def submit_to_panda_server(self):
        """
        Submit the workflow to the iDDS server through PanDA service.

        :returns id: The workflow id.
        :raise Exception when failing to submit the workflow.
        """
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api
        idds_server = self._context.get_idds_server()
        request_id = self._context.request_id
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)
        ret = client.submit_work(request_id, self, use_dataset_name=False)
        if ret[0] == 0 and ret[1][0]:
            transform_id = ret[1][1]
        else:
            transform_id = None
            self.logger.error("Failed to submit work to PanDA-iDDS with error: %s" % str(ret))

        self.logger.info("Submitted work into PanDA-iDDS with transform id=%s", str(transform_id))
        return transform_id

    def submit(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: The workflow id.
        :raise Exception when failing to submit the workflow.
        """
        try:
            # self._func = None
            if self._context.get_service() == 'panda':
                tf_id = self.submit_to_panda_server()
            else:
                tf_id = self.submit_to_idds_server()
        except Exception as ex:
            self.logger.error("Failed to submit work: %s" % str(ex))

        try:
            self._context.transform_id = int(tf_id)
            return tf_id
        except Exception as ex:
            self.logger.error("Transform id (%s) is not integer, there should be some submission errors: %s" % (tf_id, str(ex)))

        return None

    def get_status_from_panda_server(self):
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self._context.get_idds_server()
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)

        request_id = self._context.request_id
        transform_id = self._context.transform_id
        if not transform_id:
            log_msg = f"No transform id defined (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id})"
            self.logger.error(log_msg)
            return exceptions.IDDSException(log_msg)

        ret = client.get_transform(request_id=request_id, transform_id=transform_id)
        if ret[0] == 0 and ret[1][0]:
            tf = ret[1][1]
            if type(tf) in [dict]:
                tf = json_loads(json.dumps(tf))
            elif type(tf) in [str]:
                try:
                    tf = json_loads(tf)
                except Exception as ex:
                    self.logger.warn(f"Failed to json loads transform({tf}): {ex}")
        else:
            tf = None
            self.logger.error(f"Failed to get transform (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id}) status from PanDA-iDDS: {ret}")
            return TransformStatus.Transforming

        if not tf:
            self.logger.info(f"Get transform (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id}) from PanDA-iDDS: {tf}")
            return None

        if self.num_checks % 60 == 0:
            if type(tf) in [dict] and "status" in tf:
                self.logger.info(f"Get transform status (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id}) from PanDA-iDDS: {tf['status']}")
            else:
                self.logger.info(f"Get transform status (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id}) from PanDA-iDDS: {tf}")
        self.num_checks += 1

        return tf['status']

    def get_status_from_idds_server(self):
        from idds.client.clientmanager import ClientManager
        client = ClientManager(host=self._context.get_idds_server(), timeout=60)

        request_id = self._context.request_id
        transform_id = self._context.transform_id
        if not transform_id:
            log_msg = f"No transform id defined (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id})"
            self.logger.error(log_msg)
            return exceptions.IDDSException(log_msg)

        tf = client.get_transform(request_id=request_id, transform_id=transform_id)
        if not tf:
            self.logger.info(f"Get transform (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id}) from iDDS: {tf}")
            return None

        self.logger.info(f"Get transform status (request_id: {request_id}, transform_id: {transform_id}, internal_id: {self.internal_id}) from iDDS: {tf['status']}")
        return tf['status']

    def get_status(self):
        if self._context.get_service() == 'panda':
            return self.get_status_from_panda_server()
        return self.get_status_from_idds_server()

        try:
            if self._context.get_service() == 'panda':
                return self.get_status_from_panda_server()
            return self.get_status_from_idds_server()
        except Exception as ex:
            self.logger.info("Failed to get transform status: %s" % str(ex))

    def cancel_from_panda_server(self):
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self._context.get_idds_server()
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)

        request_id = self._context.request_id
        # transform_id = self._context.transform_id
        workload_id = self._context.workload_id
        if not workload_id:
            log_msg = f"No workload id defined (request_id: {request_id}, workload_id: {workload_id}, internal_id: {self.internal_id})"
            self.logger.error(log_msg)
            return exceptions.IDDSException(log_msg)

        ret = client.abort_task(request_id=request_id, workload_id=workload_id)
        if ret[0] == 0 and ret[1][0]:
            ret_value = ret[1][1]
            if type(ret_value) in [dict]:
                ret_value = json_loads(json.dumps(ret_value))
            elif type(ret_value) in [str]:
                try:
                    ret_value = json_loads(ret_value)
                except Exception as ex:
                    self.logger.warn(f"Failed to json cancel transform({ret_value}): {ex}")
        else:
            ret_value = None
            self.logger.error(f"Failed to cancel transform (request_id: {request_id}, worklaod_id: {workload_id}, internal_id: {self.internal_id}) status from PanDA-iDDS: {ret}")
            return TransformStatus.Transforming

        if not ret_value:
            self.logger.info(f"Failed to cancel transform (request_id: {request_id}, workload_id: {workload_id}, internal_id: {self.internal_id}) from PanDA-iDDS: {ret_value}")
            return None

        self.logger.info(f"Cancel transform (request_id: {request_id}, workload_id: {workload_id}, internal_id: {self.internal_id}) from PanDA-iDDS: {ret_value}")

        return ret_value

    def cancel_from_idds_server(self):
        from idds.client.clientmanager import ClientManager
        client = ClientManager(host=self._context.get_idds_server(), timeout=60)

        request_id = self._context.request_id
        # transform_id = self._context.transform_id
        workload_id = self._context.workload_id
        if not workload_id:
            log_msg = f"No workload id defined (request_id: {request_id}, workload_id: {workload_id}, internal_id: {self.internal_id})"
            self.logger.error(log_msg)
            return exceptions.IDDSException(log_msg)

        ret = client.abort_task(request_id=request_id, workload_id=workload_id)
        if not ret:
            self.logger.info(f"Cancel transform (request_id: {request_id}, workload_id: {workload_id}, internal_id: {self.internal_id}) from iDDS: {ret}")
            return None

        self.logger.info(f"Cancel transform (request_id: {request_id}, workload_id: {workload_id}, internal_id: {self.internal_id}) from iDDS: {ret}")
        return ret

    def cancel(self):
        try:
            if self._context.get_service() == 'panda':
                return self.cancel_from_panda_server()
            return self.cancel_from_idds_server()
        except Exception as ex:
            self.logger.info("Failed to get transform status: %s" % str(ex))

    def get_finished_status(self):
        return [TransformStatus.Finished]

    def get_subfinished_status(self):
        return [TransformStatus.SubFinished]

    def get_failed_status(self):
        return [None, TransformStatus.Failed, TransformStatus.Cancelled,
                TransformStatus.Suspended, TransformStatus.Expired]

    def get_terminated_status(self):
        return [None, TransformStatus.Finished, TransformStatus.SubFinished,
                TransformStatus.Failed, TransformStatus.Cancelled,
                TransformStatus.Suspended, TransformStatus.Expired]

    def is_terminated(self, status=None):
        if status is None:
            status = self.get_status()
        if status in self.get_terminated_status():
            self.stop_async_result()
            return True
        if self._async_ret:
            self._async_ret.get_results(nologs=True)
            if self._async_ret.is_terminated:
                self.stop_async_result()
                return True
        if self._async_result_status in [AsyncResultStatus.Finished, AsyncResultStatus.SubFinished, AsyncResultStatus.Failed]:
            return True
        return False

    def is_finished(self, status=None):
        if status is None:
            status = self.get_status()
        if status in self.get_finished_status():
            self.stop_async_result()
            return True
        if self._async_ret:
            self._async_ret.get_results(nologs=True)
            if self._async_ret.is_finished:
                self.stop_async_result()
                return True
        if self._async_result_status in [AsyncResultStatus.Finished]:
            return True
        return False

    def is_subfinished(self, status=None):
        if status is None:
            status = self.get_status()
        if status in self.get_subfinished_status():
            self.stop_async_result()
            return True
        if self._async_ret:
            self._async_ret.get_results(nologs=True)
            if self._async_ret.is_subfinished:
                self.stop_async_result()
                return True
        if self._async_result_status in [AsyncResultStatus.SubFinished]:
            return True
        return False

    def is_failed(self, status=None):
        if status is None:
            status = self.get_status()
        if status in self.get_failed_status():
            self.stop_async_result()
            return True
        if self._async_ret:
            self._async_ret.get_results(nologs=True)
            if self._async_ret.is_failed:
                self.stop_async_result()
                return True
        if self._async_result_status in [AsyncResultStatus.Failed]:
            return True
        return False

    def get_func_name(self):
        func_name = self._func_name_and_args[0]
        return func_name

    def get_multi_jobs_kwargs_list(self):
        multi_jobs_kwargs_list = self.multi_jobs_kwargs_list
        multi_jobs_kwargs_list = [pickle.loads(zlib.decompress(base64.b64decode(k))) for k in multi_jobs_kwargs_list]
        return multi_jobs_kwargs_list

    def init_async_result(self):
        if not self._async_result_initialized:
            multi_jobs_kwargs_list = self.get_multi_jobs_kwargs_list()
            if multi_jobs_kwargs_list:
                self._async_ret = AsyncResult(self._context, name=self.get_func_name(), multi_jobs_kwargs_list=multi_jobs_kwargs_list,
                                              map_results=self.map_results, internal_id=self.internal_id)
            else:
                self._async_ret = AsyncResult(self._context, name=self.get_func_name(), wait_num=1, internal_id=self.internal_id,
                                              map_results=self.map_results)

            self._async_result_initialized = True
            self._async_result_status = AsyncResultStatus.Running
            self._async_ret.subscribe()

    def stop_async_result(self):
        if self._async_ret:
            self._async_ret.stop()
            self._results = self._async_ret.get_results()
            if self._async_ret.is_finished:
                self._async_result_status = AsyncResultStatus.Finished
            elif self._async_ret.is_subfinished:
                self._async_result_status = AsyncResultStatus.SubFinished
            elif self._async_ret.is_failed:
                self._async_result_status = AsyncResultStatus.Failed
            self._async_ret = None
            # self._async_result_initialized = False

    def wait_results(self):
        try:
            terminated_status = self.get_terminated_status()

            # multi_jobs_kwargs_list = self.get_multi_jobs_kwargs_list()
            # if multi_jobs_kwargs_list:
            #     async_ret = AsyncResult(self._context, name=self.get_func_name(), multi_jobs_kwargs_list=multi_jobs_kwargs_list,
            #                             map_results=self.map_results, internal_id=self.internal_id)
            # else:
            #     async_ret = AsyncResult(self._context, name=self.get_func_name(), wait_num=1, internal_id=self.internal_id)

            # async_ret.subscribe()
            self.init_async_result()

            status = self.get_status()
            time_last_check_status = time.time()
            self.logger.info("waiting for results")
            while status not in terminated_status:
                # time.sleep(10)
                ret = self._async_ret.wait_results(timeout=10)
                if ret:
                    self.logger.info("Recevied result: %s" % str(ret))
                    break
                if self._async_ret.waiting_result_terminated:
                    self.logger.info("waiting_result_terminated is set, Received result is: %s" % str(ret))
                if time.time() - time_last_check_status > 600:   # 10 minutes
                    status = self.get_status()
                    time_last_check_status = time.time()

            self._results = self._async_ret.wait_results(force_return_results=True)
            self.stop_async_result()
            return self._results
        except Exception as ex:
            self.logger.error("wait_results got some errors: %s" % str(ex))
            self.stop_async_result()
            return ex

    def get_results(self):
        if self._async_ret:
            self._results = self._async_ret.get_results()
        return self._results

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        return self._context.setup()

    def get_clean_env(self):
        """
        :returns command: `str` to clean the workflow.
        """
        return self._context.get_clean_env()

    def load_func(self, func_name):
        """
        Load the function from the source files.

        :raise Exception
        """
        with modified_environ(IDDS_IGNORE_WORK_DECORATOR='true'):
            func = super(Work, self).load_func(func_name)

        return func

    def pre_run(self):
        # test AsyncResult
        a_ret = None
        try:
            workflow_context = self._context
            if workflow_context.distributed:
                self.logger.info("Test AsyncResult")
                a_ret = AsyncResult(workflow_context, wait_num=1, timeout=30)
                ret = a_ret.is_ok()
                a_ret.stop()
                self.logger.info(f"pre_run asyncresult test is_ok: {ret}")
                return ret
            return True
        except Exception as ex:
            self.logger.error(f"pre_run failed with error: {ex}")
            self.logger.error(traceback.format_exc())
        if a_ret:
            a_ret.stop()
        return False

    def run(self):
        self.logger.info("Start work run().")
        ret = None
        try:
            ret = self.run_local()
        except Exception as ex:
            self.logger.error(f"Failed to run function: {ex}")
            self.logger.error(traceback.format_exc())
        except:
            self.logger.error("Unknow error")
            self.logger.error(traceback.format_exc())
        self.logger.info(f"finish work run() with ret: {ret}")
        return ret

    def run_local(self):
        """
        Run the work.
        """
        is_ok = self.pre_run()
        if not is_ok:
            self.logger.error(f"pre_run is_ok: {is_ok}, will exit.")
            raise Exception("work pre_run failed")

        func_name, pre_kwargs, args, kwargs = self._func_name_and_args
        multi_jobs_kwargs_list = self.multi_jobs_kwargs_list
        current_job_kwargs = self._current_job_kwargs

        if args:
            args = pickle.loads(zlib.decompress(base64.b64decode(args)))
        if pre_kwargs:
            pre_kwargs = pickle.loads(zlib.decompress(base64.b64decode(pre_kwargs)))
        if kwargs:
            kwargs = pickle.loads(zlib.decompress(base64.b64decode(kwargs)))
        if multi_jobs_kwargs_list:
            multi_jobs_kwargs_list = [pickle.loads(zlib.decompress(base64.b64decode(k))) for k in multi_jobs_kwargs_list]
        if self._current_job_kwargs:
            current_job_kwargs = pickle.loads(zlib.decompress(base64.b64decode(current_job_kwargs)))

        if self._func is None:
            func = self.load_func(func_name)
            self._func = func

        if self._context.distributed:
            args_copy = copy.deepcopy(args)
            pre_kwargs_copy = copy.deepcopy(pre_kwargs)
            kwargs_copy = copy.deepcopy(kwargs)
            if current_job_kwargs and type(current_job_kwargs) in [dict]:
                kwargs_copy.update(current_job_kwargs)
            elif current_job_kwargs and type(current_job_kwargs) in [tuple, list]:
                args_copy = copy.deepcopy(current_job_kwargs)

            if self.inputs and self.input_map:
                new_kwargs = {self.input_map: ",".join[self.inputs]}
                kwargs_copy.update(new_kwargs)
            if self.inputs_group:
                # new_kwargs = {
                #     k: ",".join(v) if isinstance(v, (list, tuple)) else str(v)
                #     for k, v in self.inputs_group.items()
                # }
                new_kwargs = {k: v for k, v in self.inputs_group.items()}
                kwargs_copy.update(new_kwargs)

            ret_status, ret_output, ret_err = self.run_func(self._func, pre_kwargs_copy, args_copy, kwargs_copy)

            request_id = self._context.request_id
            transform_id = self._context.transform_id
            ret_log = f"(status: {ret_status}, return: {ret_output}, error: {ret_err})"
            self.logger.info(f"publishing AsyncResult to (request_id: {request_id}, transform_id: {transform_id}): {ret_log}")
            async_ret = AsyncResult(self._context, name=self.get_func_name(), internal_id=self.internal_id, current_job_kwargs=current_job_kwargs)
            async_ret.publish(ret_output, ret_status=ret_status, ret_error=ret_err, key=self.job_key)

            if not self.map_results:
                self._results = ret_output
            else:
                self._results = MapResult()
                self._results.add_result(name=self.get_func_name(), args=current_job_kwargs, result=ret_output, key=self.job_key)
            return ret_status
        else:
            if not multi_jobs_kwargs_list:
                ret_status, rets, ret_err = self.run_func(self._func, pre_kwargs, args, kwargs)
                if not self.map_results:
                    self._results = rets
                else:
                    self._results = MapResult()
                    self._results.add_result(name=self.get_func_name(), args=kwargs, result=rets, key=self.job_key)
                return ret_status
            else:
                if not self.map_results:
                    self._results = []
                    for one_job_kwargs in multi_jobs_kwargs_list:
                        kwargs_copy = copy.deepcopy(kwargs)
                        args_copy = copy.deepcopy(args)
                        pre_kwargs_copy = copy.deepcopy(pre_kwargs)
                        if type(one_job_kwargs) in [dict]:
                            kwargs_copy.update(one_job_kwargs)
                        elif type(one_job_kwargs) in [tuple, list]:
                            args_copy = copy.deepcopy(one_job_kwargs)

                        ret_status, rets, ret_error = self.run_func(self._func, pre_kwargs_copy, args_copy, kwargs_copy)
                        self._results.append(rets)
                else:
                    self._results = MapResult()
                    for one_job_kwargs in multi_jobs_kwargs_list:
                        kwargs_copy = copy.deepcopy(kwargs)
                        args_copy = copy.deepcopy(args)
                        pre_kwargs_copy = copy.deepcopy(pre_kwargs)
                        if type(one_job_kwargs) in [dict]:
                            kwargs_copy.update(one_job_kwargs)
                        elif type(one_job_kwargs) in [tuple, list]:
                            args_copy = copy.deepcopy(one_job_kwargs)

                        ret_status, rets, ret_error = self.run_func(self._func, pre_kwargs_copy, args_copy, kwargs_copy)
                        self._results.add_result(name=self.get_func_name(), args=one_job_kwargs, result=rets, key=self.job_key)
                return ret_status

    def get_run_command(self):
        cmd = f"run_workflow --type work --name {self.name} --key {self.job_key} "
        cmd += "--context %s --original_args %s " % (encode_base64(json_dumps(self._context)),
                                                     encode_base64(json_dumps(self._func_name_and_args)))
        cmd += '--current_job_kwargs "${IN/T}"'
        return cmd

    def get_run_args_to_file_cmd(self):
        args = {'type': 'work',
                'name': self.name,
                'context': self._context,
                'original_args': self._func_name_and_args,
                'current_job_kwargs': '"${IN/T}"'}
        args_json = encode_base64(json_dumps(args))
        cmd = 'echo ' + args_json + ' > run_workflow_args; '
        return cmd

    def get_run_command_test(self):
        cmd = "run_workflow.sh"
        return cmd

    def get_runner(self):
        setup = self.setup()
        cmd = ""

        run_command = self.get_run_command()

        if setup:
            pre_setup, main_setup = self.split_setup(setup)
            pre_setup = encode_base64(json_dumps(pre_setup))
            main_setup = encode_base64(json_dumps(main_setup))
            if pre_setup:
                cmd = " --pre_setup " + pre_setup + " "
            cmd = cmd + " --setup " + main_setup + " "
        if cmd:
            cmd = cmd + " " + run_command
        else:
            cmd = run_command

        clean_env = self.get_clean_env()
        if clean_env:
            # cmd = cmd + "; " + clean_env
            cmd = cmd + "; ret=$?; " + clean_env + "; exit $ret"

        return cmd


def run_work_distributed(w):
    try:
        logger = logging.getLogger("run_work_distributed")
        tf_id = w.submit()
        if tf_id:
            logger.info("wait for results")
            rets = w.wait_results()
            logger.info("Got results: %s" % rets)
            return rets
        else:
            logger.error("Failed to distribute work: %s" % w.name)
        return None
    except Exception as ex:
        logger.error("Failed to run the work distributedly: %s" % ex)
        logger.error(traceback.format_exc())
        return None


# foo = work(arg)(foo)
def work(func=None, *, workflow=None, pre_kwargs={}, name=None, return_work=False, map_results=False, lazy=False, init_env=None, no_wraps=False,
         container_options=None, parent_workload_id=None, no_wait_parent=False, input_datasets=None, output_file_name=None,
         enable_separate_log=False, output_dataset_name=None, log_dataset_name=None, num_events=None, num_events_per_job=None,
         parent_transform_id=None, parent_internal_id=None, job_key=None):
    if func is None:
        return functools.partial(work, workflow=workflow, pre_kwargs=pre_kwargs, return_work=return_work, no_wraps=no_wraps,
                                 name=name, map_results=map_results, lazy=lazy, init_env=init_env, container_options=container_options,
                                 parent_workload_id=parent_workload_id, no_wait_parent=no_wait_parent, parent_transform_id=parent_transform_id,
                                 input_datasets=input_datasets, output_file_name=output_file_name, output_dataset_name=output_dataset_name,
                                 log_dataset_name=log_dataset_name, job_key=job_key,
                                 enable_separate_log=enable_separate_log, num_events=num_events, num_events_per_job=num_events_per_job,
                                 parent_internal_id=parent_internal_id)

    if 'IDDS_IGNORE_WORK_DECORATOR' in os.environ:
        return func

    # @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger = logging.getLogger("work_def")
            f = workflow or kwargs.pop('workflow', None) or WorkflowCanvas.get_current_workflow()
            workflow_context = f._context
            multi_jobs_kwargs_list = kwargs.pop('multi_jobs_kwargs_list', [])
            logger.debug("workflow context: %s" % workflow_context)

            logger.debug("work decorator: func: %s, map_results: %s" % (func, map_results))
            if workflow_context:
                logger.debug("setup work")
                w = Work(workflow_context=workflow_context, func=func, pre_kwargs=pre_kwargs, args=args, kwargs=kwargs,
                         name=name, multi_jobs_kwargs_list=multi_jobs_kwargs_list, map_results=map_results, init_env=init_env,
                         container_options=container_options, parent_workload_id=parent_workload_id, no_wait_parent=no_wait_parent,
                         parent_transform_id=parent_transform_id, input_datasets=input_datasets, output_file_name=output_file_name,
                         output_dataset_name=output_dataset_name, num_events=num_events, num_events_per_job=num_events_per_job,
                         parent_internal_id=parent_internal_id, enable_separate_log=enable_separate_log, log_dataset_name=log_dataset_name,
                         job_key=job_key)
                # if distributed:

                if return_work:
                    return w

                if workflow_context.distributed:
                    ret = run_work_distributed(w)
                    return ret

                return w.run()
            else:
                logger.info("workflow context is not defined, run function locally")
                if not multi_jobs_kwargs_list:
                    kwargs_copy = copy.deepcopy(pre_kwargs)
                    kwargs_copy.update(kwargs)
                    return func(*args, **kwargs_copy)

                if not kwargs:
                    kwargs = {}
                if not map_results:
                    rets = []
                    for one_job_kwargs in multi_jobs_kwargs_list:
                        kwargs_copy = copy.deepcopy(kwargs)
                        args_copy = copy.deepcopy(args)
                        pre_kwargs_copy = copy.deepcopy(pre_kwargs)
                        if type(one_job_kwargs) in [dict]:
                            kwargs_copy.update(one_job_kwargs)
                        elif type(one_job_kwargs) in [tuple, list]:
                            args_copy = copy.deepcopy(one_job_kwargs)

                        pre_kwargs_copy.update(kwargs_copy)

                        ret = func(*args_copy, **pre_kwargs_copy)
                        rets.append(ret)
                    return rets
                else:
                    rets = MapResult()
                    for one_job_kwargs in multi_jobs_kwargs_list:
                        kwargs_copy = copy.deepcopy(kwargs)
                        args_copy = copy.deepcopy(args)
                        pre_kwargs_copy = copy.deepcopy(pre_kwargs)
                        if type(one_job_kwargs) in [dict]:
                            kwargs_copy.update(one_job_kwargs)
                        elif type(one_job_kwargs) in [tuple, list]:
                            args_copy = copy.deepcopy(one_job_kwargs)

                        pre_kwargs_copy.update(kwargs_copy)

                        ret = func(*args_copy, **pre_kwargs_copy)
                        rets.add_result(name=get_func_name(func), args=one_job_kwargs, result=ret, key=job_key)
                    return rets
        except Exception as ex:
            logger.error("Failed to run workflow %s: %s" % (func, ex))
            raise ex
        except:
            raise
    if no_wraps:
        return wrapper
    else:
        return functools.wraps(func)(wrapper)
