#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2023

import copy
import datetime
import logging
import os
import stat
import uuid
import traceback

from idds.common import exceptions
from idds.common.constants import (WorkStatus, ProcessingStatus,
                                   CollectionStatus, CollectionType)
from idds.common.constants import get_work_status_from_transform_processing_status
from idds.common.utils import setup_logging
from idds.common.utils import str_to_date
# from idds.common.utils import json_dumps

from .base import Base


setup_logging(__name__)


class Parameter(object):
    def __init__(self, params):
        assert(type(params) in [dict])
        self.params = params

    def add(self, name, value):
        self.params[name] = value

    def get_param_names(self):
        return self.params.keys()

    def get_param_value(self, name):
        value = self.params.get(name, None)
        if value and callable(value):
            value = value()
        return value


class Collection(Base):

    def __init__(self, scope=None, name=None, coll_type=CollectionType.Dataset, coll_metadata={}):
        super(Collection, self).__init__()
        self.scope = scope
        self.name = name
        self.coll_metadata = coll_metadata

        self.collection = None

        self.internal_id = str(uuid.uuid4())[:8]
        self.coll_id = None
        self.coll_type = coll_type
        self.status = CollectionStatus.New
        self.substatus = CollectionStatus.New

        self.total_files = 0
        self.processed_files = 0
        self.processing_files = 0
        self.bytes = 0
        self.new_files = 0
        self.failed_files = 0
        self.missing_files = 0

        self.ext_files = 0
        self.processed_ext_files = 0
        self.failed_ext_files = 0
        self.missing_ext_files = 0

    @property
    def internal_id(self):
        return self.get_metadata_item('internal_id')

    @internal_id.setter
    def internal_id(self, value):
        self.add_metadata_item('internal_id', value)

    @property
    def coll_id(self):
        return self.get_metadata_item('coll_id', None)

    @coll_id.setter
    def coll_id(self, value):
        self.add_metadata_item('coll_id', value)

    @property
    def status(self):
        st = self.get_metadata_item('status', CollectionStatus.New)
        if type(st) in [int]:
            st = CollectionStatus(st)
        return st

    @status.setter
    def status(self, value):
        self.add_metadata_item('status', value.value if value else value)
        if self.collection:
            self.collection['status'] = value

    @property
    def coll_type(self):
        st = self.get_metadata_item('coll_type', CollectionType.Dataset)
        if type(st) in [int]:
            st = CollectionType(st)
        return st

    @coll_type.setter
    def coll_type(self, value):
        self.add_metadata_item('coll_type', value.value if value else value)
        if self.collection:
            self.collection['coll_type'] = value

    @property
    def substatus(self):
        st = self.get_metadata_item('substatus', CollectionStatus.New)
        if type(st) in [int]:
            st = CollectionStatus(st)
        return st

    @substatus.setter
    def substatus(self, value):
        self.add_metadata_item('substatus', value.value if value else value)
        if self.collection:
            self.collection['substatus'] = value

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value
        if self._collection:
            self.scope = self._collection['scope']
            self.name = self._collection['name']
            self.coll_metadata = self._collection['coll_metadata']
            self.coll_id = self._collection['coll_id']
            self.coll_type = self._collection['coll_type']
            self.status = self._collection['status']
            self.substatus = self._collection['substatus']

            self.total_files = self._collection['total_files']
            self.processed_files = self._collection['processed_files']
            self.processing_files = self._collection['processing_files']
            self.bytes = self._collection['bytes']

    def to_origin_dict(self):
        return {'scope': self.scope, 'name': self.name}


class Processing(Base):

    def __init__(self, processing_metadata={}):
        super(Processing, self).__init__()

        self.processing_metadata = processing_metadata
        if self.processing_metadata and 'work' in self.processing_metadata:
            self.work = self.processing_metadata['work']
        else:
            self.work = None

        self.processing = None

        self.internal_id = str(uuid.uuid4())[:8]
        self.task_name = None
        self.processing_id = None
        self.workload_id = None
        self.status = ProcessingStatus.New
        self.substatus = ProcessingStatus.New
        self.last_updated_at = datetime.datetime.utcnow()
        self.polling_retries = 0
        self.tocancel = False
        self.tosuspend = False
        self.toresume = False
        self.toexpire = False
        self.tofinish = False
        self.toforcefinish = False
        self.operation_time = datetime.datetime.utcnow()
        self.submitted_at = None

        self.username = None

        self.external_id = None
        self.errors = None

        self.output_data = None

        self.retries = 0

    @property
    def internal_id(self):
        return self.get_metadata_item('internal_id')

    @internal_id.setter
    def internal_id(self, value):
        self.add_metadata_item('internal_id', value)

    @property
    def processing_id(self):
        return self.get_metadata_item('processing_id', None)

    @processing_id.setter
    def processing_id(self, value):
        self.add_metadata_item('processing_id', value)

    @property
    def workload_id(self):
        return self.get_metadata_item('workload_id', None)

    @workload_id.setter
    def workload_id(self, value):
        self.add_metadata_item('workload_id', value)

    def get_workload_id(self):
        return self.workload_id

    @property
    def status(self):
        st = self.get_metadata_item('status', ProcessingStatus.New)
        if type(st) in [int]:
            st = ProcessingStatus(st)
        return st

    @status.setter
    def status(self, value):
        self.add_metadata_item('status', value.value if value else value)
        if self.processing:
            self.processing['status'] = value

    @property
    def substatus(self):
        st = self.get_metadata_item('substatus', ProcessingStatus.New)
        if type(st) in [int]:
            st = ProcessingStatus(st)
        return st

    @substatus.setter
    def substatus(self, value):
        self.add_metadata_item('substatus', value.value if value else value)
        if self.processing:
            self.processing['substatus'] = value

    @property
    def retries(self):
        return self.get_metadata_item('retries', 0)

    @retries.setter
    def retries(self, value):
        self.add_metadata_item('retries', value)

    @property
    def last_updated_at(self):
        last_updated_at = self.get_metadata_item('last_updated_at', None)
        if last_updated_at and type(last_updated_at) in [str]:
            last_updated_at = str_to_date(last_updated_at)
        return last_updated_at

    @last_updated_at.setter
    def last_updated_at(self, value):
        self.add_metadata_item('last_updated_at', value)

    @property
    def polling_retries(self):
        return self.get_metadata_item('polling_retries', 0)

    @polling_retries.setter
    def polling_retries(self, value):
        self.add_metadata_item('polling_retries', value)

    @property
    def tocancel(self):
        return self.get_metadata_item('tocancel', False)

    @tocancel.setter
    def tocancel(self, value):
        old_value = self.get_metadata_item('tocancel', False)
        if old_value != value:
            self.operation_time = datetime.datetime.utcnow()
        self.add_metadata_item('tocancel', value)

    @property
    def tosuspend(self):
        return self.get_metadata_item('tosuspend', False)

    @tosuspend.setter
    def tosuspend(self, value):
        old_value = self.get_metadata_item('tosuspend', False)
        if old_value != value:
            self.operation_time = datetime.datetime.utcnow()
        self.add_metadata_item('tosuspend', value)

    @property
    def toresume(self):
        return self.get_metadata_item('toresume', False)

    @toresume.setter
    def toresume(self, value):
        old_value = self.get_metadata_item('toresume', False)
        if old_value != value:
            self.operation_time = datetime.datetime.utcnow()
        self.add_metadata_item('toresume', value)

    @property
    def toexpire(self):
        return self.get_metadata_item('toexpire', False)

    @toexpire.setter
    def toexpire(self, value):
        old_value = self.get_metadata_item('toexpire', False)
        if old_value != value:
            self.operation_time = datetime.datetime.utcnow()
        self.add_metadata_item('toexpire', value)

    @property
    def tofinish(self):
        return self.get_metadata_item('tofinish', False)

    @tofinish.setter
    def tofinish(self, value):
        old_value = self.get_metadata_item('tofinish', False)
        if old_value != value:
            self.operation_time = datetime.datetime.utcnow()
        self.add_metadata_item('tofinish', value)

    @property
    def toforcefinish(self):
        return self.get_metadata_item('toforcefinish', False)

    @toforcefinish.setter
    def toforcefinish(self, value):
        old_value = self.get_metadata_item('toforcefinish', False)
        if old_value != value:
            self.operation_time = datetime.datetime.utcnow()
        self.add_metadata_item('toforcefinish', value)

    @property
    def operation_time(self):
        opt_time = self.get_metadata_item('operation_time', None)
        if opt_time and type(opt_time) in [str]:
            opt_time = str_to_date(opt_time)
        return opt_time

    @operation_time.setter
    def operation_time(self, value):
        self.add_metadata_item('operation_time', value)

    def in_operation_time(self):
        if self.operation_time:
            time_diff = datetime.datetime.utcnow() - self.operation_time
            time_diff = time_diff.total_seconds()
            if time_diff < 120:
                return True
        return False

    @property
    def submitted_at(self):
        opt_time = self.get_metadata_item('submitted_at', None)
        if opt_time and type(opt_time) in [str]:
            opt_time = str_to_date(opt_time)
        return opt_time

    @submitted_at.setter
    def submitted_at(self, value):
        self.add_metadata_item('submitted_at', value)

    @property
    def errors(self):
        return self.get_metadata_item('errors', None)

    @errors.setter
    def errors(self, value):
        self.add_metadata_item('errors', value)

    @property
    def external_id(self):
        return self.get_metadata_item('external_id', None)

    @external_id.setter
    def external_id(self, value):
        self.add_metadata_item('external_id', value)

    @property
    def old_external_id(self):
        return self.get_metadata_item('old_external_id', [])

    @old_external_id.setter
    def old_external_id(self, value):
        self.add_metadata_item('old_external_id', value)

    @property
    def task_name(self):
        return self.get_metadata_item('task_name', None)

    @task_name.setter
    def task_name(self, value):
        self.add_metadata_item('task_name', value)

    @property
    def processing(self):
        return self._processing

    @processing.setter
    def processing(self, value):
        self._processing = value
        if self._processing:
            self.processing_id = self._processing.get('processing_id', None)
            self.workload_id = self._processing.get('workload_id', None)
            self.status = self._processing.get('status', None)
            self.substatus = self._processing.get('substatus', None)
            self.processing_metadata = self._processing.get('processing_metadata', None)
            self.submitted_at = self._processing.get('submitted_at', None)
            if self.processing_metadata and 'processing' in self.processing_metadata:
                proc = self.processing_metadata['processing']
                self.work = proc.work
                self.external_id = proc.external_id
                self.errors = proc.errors
                if not self.submitted_at:
                    self.submitted_at = proc.submitted_at

            self.output_data = self._processing.get('output_metadata', None)

    def has_new_updates(self):
        self.last_updated_at = datetime.datetime.utcnow()


class Work(Base):

    def __init__(self, executable=None, arguments=None, parameters=None, setup=None, work_type=None,
                 work_tag=None, exec_type='local', sandbox=None, request_id=None, work_id=None, work_name=None,
                 primary_input_collection=None, other_input_collections=None, input_collections=None,
                 primary_output_collection=None, other_output_collections=None, output_collections=None,
                 log_collections=None, release_inputs_after_submitting=False, username=None,
                 agent_attributes=None, is_template=False,
                 logger=None):
        """
        Init a work/task/transformation.

        :param setup: A string to setup the executable enviroment, it can be None.
        :param executable: The executable.
        :param arguments: The arguments.
        :param parameters: A dict with arguments needed to be replaced.
        :param work_type: The work type like data carousel, hyperparameteroptimization and so on.
        :param exec_type: The exec type like 'local', 'remote'(with remote_package set), 'docker' and so on.
        :param sandbox: The sandbox.
        :param work_id: The work/task id.
        :param primary_input_collection: The primary input collection.
        :param other_input_collections: List of the input collections.
        :param output_collections: List of the output collections.
        # :param workflow: The workflow the current work belongs to.
        """
        self._collections = {}
        self._primary_input_collection = None
        self._primary_output_collection = None
        self._other_input_collections = []
        self._other_output_collections = []

        self._processings = {}

        super(Work, self).__init__()

        self.internal_id = str(uuid.uuid4())[:8]
        self.template_work_id = self.internal_id
        self.is_template = is_template
        self.class_name = self.__class__.__name__.lower()
        self.initialized = False
        self.sequence_id = 0

        self.logger = logger
        if self.logger is None:
            self.setup_logger()

        self.setup = setup
        self.executable = executable
        self.arguments = arguments
        self.parameters = parameters

        self.username = username
        self.work_type = work_type
        self.work_tag = work_tag
        self.exec_type = exec_type
        self.sandbox = sandbox
        self.request_id = request_id
        self.work_id = work_id
        self.work_name = work_name
        if not self.work_name:
            self.work_name = self.template_work_id
        # self.workflow = workflow
        self.transforming = False
        self.workdir = None

        self.log_collections = []
        if input_collections and (primary_input_collection or other_input_collections):
            raise Exception("input_collections and (primary_input_collection, other_input_collections) cannot be used at the same time.")
        if output_collections and (primary_output_collection or other_output_collections):
            raise Exception("output_collections and (primary_output_collection, other_output_collections) cannot be used at the same time.")

        if input_collections and type(input_collections) not in [list, tuple]:
            input_collections = [input_collections]
        if output_collections and type(output_collections) not in [list, tuple]:
            output_collections = [output_collections]

        if input_collections:
            primary_input_collection = input_collections[0]
            if len(input_collections) > 1:
                other_input_collections = input_collections[1:]
        if output_collections:
            primary_output_collection = output_collections[0]
            if len(output_collections) > 1:
                other_output_collections = output_collections[1:]

        # self.primary_input_collection = primary_input_collection
        self.set_primary_input_collection(primary_input_collection)
        self.set_primary_output_collection(primary_output_collection)

        # self.other_input_collections = other_input_collections
        if other_input_collections and type(other_input_collections) not in [list, tuple]:
            other_input_collections = [other_input_collections]
        self.add_other_input_collections(other_input_collections)
        if other_output_collections and type(other_output_collections) not in [list, tuple]:
            other_output_collections = [other_output_collections]
        self.add_other_output_collections(other_output_collections)

        # if input_collections and type(input_collections) not in [list, tuple]:
        #     input_collections = [input_collections]
        # self.add_input_collections(input_collections)
        # if output_collections and type(output_collections) not in [list, tuple]:
        #     output_collections = [output_collections]
        # self.add_output_collections(output_collections)

        if log_collections and type(log_collections) not in [list, tuple]:
            log_collections = [log_collections]
        self.add_log_collections(log_collections)

        self.release_inputs_after_submitting = release_inputs_after_submitting
        self.has_new_inputs = True

        self.started = False
        self.status = WorkStatus.New
        self.substatus = WorkStatus.New
        self.polling_retries = 0
        self.errors = []
        self.next_works = []

        self.work_name_to_coll_map = []

        self.processings = {}
        self.active_processings = []
        self.cancelled_processings = []
        self.suspended_processings = []
        self.old_processings = []
        self.terminated_msg = ""
        self.output_data = {}
        self.parameters_for_next_task = None

        self.status_statistics = {}

        self.agent_attributes = agent_attributes

        self.proxy = None
        self.original_proxy = None

        self.tocancel = False
        self.tosuspend = False
        self.toresume = False
        self.toexpire = False
        self.tofinish = False
        self.toforcefinish = False

        self.last_updated_at = datetime.datetime.utcnow()

        self.to_update_processings = {}

        self.backup_to_release_inputs = {'0': [], '1': [], '2': []}

        self.num_run = 0

        self.or_custom_conditions = {}
        self.and_custom_conditions = {}

        self.sliced_global_parameters = None

        self.func_site_to_cloud = None

        """
        self._running_data_names = []
        for name in ['internal_id', 'template_work_id', 'initialized', 'sequence_id', 'parameters', 'work_id', 'transforming', 'workdir',
                     # 'collections', 'primary_input_collection', 'other_input_collections', 'output_collections', 'log_collections',
                     'collections',
                     # 'output_data',
                     '_has_new_inputs', 'status', 'substatus', 'polling_retries', 'errors', 'next_works',
                     'processings', 'active_processings', 'cancelled_processings', 'suspended_processings', 'old_processings',
                     # 'terminated_msg', 'output_data', 'parameters_for_next_task', 'status_statistics',
                     'tocancel', 'tosuspend', 'toresume']:
            self._running_data_names.append(name)
        """

    def get_logger(self):
        if self.logger is None:
            self.logger = self.setup_logger()
        return self.logger

    def get_class_name(self):
        return self.__class__.__name__

    def get_internal_id(self):
        return self.internal_id

    def get_template_work_id(self):
        return self.template_work_id

    def get_sequence_id(self):
        return self.sequence_id

    def get_site(self):
        return None

    @property
    def internal_id(self):
        return self.get_metadata_item('internal_id')

    @internal_id.setter
    def internal_id(self, value):
        self.add_metadata_item('internal_id', value)

    @property
    def template_work_id(self):
        return self.get_metadata_item('template_work_id')

    @template_work_id.setter
    def template_work_id(self, value):
        self.add_metadata_item('template_work_id', value)

    @property
    def workload_id(self):
        return self.get_metadata_item('workload_id', None)

    @workload_id.setter
    def workload_id(self, value):
        self.add_metadata_item('workload_id', value)

    @property
    def external_id(self):
        return self.get_metadata_item('external_id', None)

    @external_id.setter
    def external_id(self, value):
        self.add_metadata_item('external_id', value)

    def get_workload_id(self):
        return self.workload_id

    @property
    def initialized(self):
        return self.get_metadata_item('initialized', False)

    @initialized.setter
    def initialized(self, value):
        self.add_metadata_item('initialized', value)

    @property
    def sequence_id(self):
        return self.get_metadata_item('sequence_id', 0)

    @sequence_id.setter
    def sequence_id(self, value):
        self.add_metadata_item('sequence_id', value)

    @property
    def parameters(self):
        return self.get_metadata_item('parameters', None)

    @parameters.setter
    def parameters(self, value):
        self.add_metadata_item('parameters', value)

    @property
    def output_data(self):
        return self.get_metadata_item('output_data', {})

    @output_data.setter
    def output_data(self, value):
        self.add_metadata_item('output_data', value)
        if value and type(value) in [dict]:
            for key in value:
                new_key = "user_" + str(key)
                setattr(self, new_key, value[key])

    @property
    def work_id(self):
        return self.get_metadata_item('work_id', None)

    @work_id.setter
    def work_id(self, value):
        self.add_metadata_item('work_id', value)

    @property
    def parent_workload_id(self):
        return self.get_metadata_item('parent_workload_id', None)

    @parent_workload_id.setter
    def parent_workload_id(self, value):
        self.add_metadata_item('parent_workload_id', value)

    @property
    def transforming(self):
        return self.get_metadata_item('transforming', False)

    @transforming.setter
    def transforming(self, value):
        self.add_metadata_item('transforming', value)

    @property
    def submitted(self):
        return self.get_metadata_item('submitted', False)

    @submitted.setter
    def submitted(self, value):
        self.add_metadata_item('submitted', value)

    @property
    def workdir(self):
        return self.get_metadata_item('workdir', None)

    @workdir.setter
    def workdir(self, value):
        self.add_metadata_item('workdir', value)

    @property
    def work_name(self):
        return self.get_metadata_item('work_name', 0)

    @work_name.setter
    def work_name(self, value):
        self.add_metadata_item('work_name', value)

    @property
    def has_new_inputs(self):
        return self.get_metadata_item('has_new_inputs', 0)

    @has_new_inputs.setter
    def has_new_inputs(self, value):
        self.add_metadata_item('has_new_inputs', value)

    @property
    def started(self):
        return self.get_metadata_item('started', False)

    @started.setter
    def started(self, value):
        self.add_metadata_item('started', value)

    @property
    def status(self):
        st = self.get_metadata_item('status', WorkStatus.New)
        if type(st) in [int]:
            st = WorkStatus(st)
        return st

    @status.setter
    def status(self, value):
        if not self.transforming:
            if value and value in [WorkStatus.Transforming,
                                   WorkStatus.Finished,
                                   WorkStatus.SubFinished,
                                   WorkStatus.Failed,
                                   WorkStatus.Running]:
                self.transforming = True
        self.add_metadata_item('status', value.value if value else value)

    @property
    def substatus(self):
        st = self.get_metadata_item('substatus', WorkStatus.New)
        if type(st) in [int]:
            st = WorkStatus(st)
        return st

    @substatus.setter
    def substatus(self, value):
        self.add_metadata_item('substatus', value.value if value else value)

    @property
    def polling_retries(self):
        return self.get_metadata_item('polling_retries', 0)

    @polling_retries.setter
    def polling_retries(self, value):
        self.add_metadata_item('polling_retries', value)

    @property
    def errors(self):
        return self.get_metadata_item('errors', None)

    @errors.setter
    def errors(self, value):
        self.add_metadata_item('errors', value)

    @property
    def next_works(self):
        return self.get_metadata_item('next_works', [])

    @next_works.setter
    def next_works(self, value):
        self.add_metadata_item('next_works', value)

    @property
    def collections(self):
        return self._collections

    @collections.setter
    def collections(self, value):
        self._collections = value
        coll_metadata = {}
        if self._collections:
            for k in self._collections:
                coll = self._collections[k]
                if type(coll) in [Collection]:
                    coll_metadata[k] = {'coll_id': coll.coll_id}
        self.add_metadata_item('collections', coll_metadata)

    def with_sub_map_id(self):
        return False

    @property
    def processings(self):
        return self._processings

    @processings.setter
    def processings(self, value):
        self._processings = value
        proc_metadata = {}
        if self._processings:
            for k in self._processings:
                proc = self._processings[k]
                if type(proc) in [Processing]:
                    proc_metadata[k] = {'processing_id': proc.processing_id,
                                        'workload_id': proc.workload_id,
                                        'external_id': proc.external_id}
        self.add_metadata_item('processings', proc_metadata)

    def generating_new_inputs(self):
        return False

    def refresh_work(self):
        coll_metadata = {}
        if self._collections:
            for k in self._collections:
                coll = self._collections[k]
                if type(coll) in [Collection]:
                    coll_metadata[k] = {'coll_id': coll.coll_id}
        self.add_metadata_item('collections', coll_metadata)

        proc_metadata = {}
        if self._processings:
            for k in self._processings:
                proc = self._processings[k]
                if type(proc) in [Processing]:
                    proc_metadata[k] = {'processing_id': proc.processing_id,
                                        'workload_id': proc.workload_id,
                                        'external_id': proc.external_id}
        self.add_metadata_item('processings', proc_metadata)

    def load_work(self):
        coll_metadata = self.get_metadata_item('collections', {})
        for k in self._collections:
            if k in coll_metadata:
                coll_id = coll_metadata[k]['coll_id']
                self._collections[k].coll_id = coll_id

        proc_metadata = self.get_metadata_item('processings', {})
        for k in self._processings:
            if k in proc_metadata:
                proc_id = proc_metadata[k]['processing_id']
                self._processings[k].processing_id = proc_id
                if 'workload_id' in proc_metadata[k] and proc_metadata[k]['workload_id']:
                    self._processings[k].workload_id = proc_metadata[k]['workload_id']
                    self.workload_id = proc_metadata[k]['workload_id']
                if 'external_id' in proc_metadata[k] and proc_metadata[k]['external_id']:
                    self._processings[k].external_id = proc_metadata[k]['external_id']
                    self.external_id = proc_metadata[k]['external_id']
        for k in proc_metadata:
            if k not in self._processings:
                self._processings[k] = Processing(processing_metadata={})
                proc_id = proc_metadata[k]['processing_id']
                self._processings[k].processing_id = proc_id
                self._processings[k].internal_id = k
                if 'workload_id' in proc_metadata[k] and proc_metadata[k]['workload_id']:
                    self._processings[k].workload_id = proc_metadata[k]['workload_id']
                    self.workload_id = proc_metadata[k]['workload_id']
                if 'external_id' in proc_metadata[k] and proc_metadata[k]['external_id']:
                    self._processings[k].external_id = proc_metadata[k]['external_id']
                    self.external_id = proc_metadata[k]['external_id']

    def load_metadata(self):
        self.load_work()

    @property
    def active_processings(self):
        return self.get_metadata_item('active_processings', [])

    @active_processings.setter
    def active_processings(self, value):
        self.add_metadata_item('active_processings', value)

    @property
    def cancelled_processings(self):
        return self.get_metadata_item('cancelled_processings', [])

    @cancelled_processings.setter
    def cancelled_processings(self, value):
        self.add_metadata_item('cancelled_processings', value)

    @property
    def suspended_processings(self):
        return self.get_metadata_item('suspended_processings', [])

    @suspended_processings.setter
    def suspended_processings(self, value):
        self.add_metadata_item('suspended_processings', value)

    @property
    def old_processings(self):
        return self.get_metadata_item('old_processings', [])

    @old_processings.setter
    def old_processings(self, value):
        self.add_metadata_item('old_processings', value)

    @property
    def tocancel(self):
        return self.get_metadata_item('tocancel', False)

    @tocancel.setter
    def tocancel(self, value):
        self.add_metadata_item('tocancel', value)

    @property
    def tosuspend(self):
        return self.get_metadata_item('tosuspend', False)

    @tosuspend.setter
    def tosuspend(self, value):
        self.add_metadata_item('tosuspend', value)

    @property
    def toresume(self):
        return self.get_metadata_item('toresume', False)

    @toresume.setter
    def toresume(self, value):
        self.add_metadata_item('toresume', value)

    @property
    def toexpire(self):
        return self.get_metadata_item('toexpire', False)

    @toexpire.setter
    def toexpire(self, value):
        self.add_metadata_item('toexpire', value)

    @property
    def tofinish(self):
        return self.get_metadata_item('tofinish', False)

    @tofinish.setter
    def tofinish(self, value):
        self.add_metadata_item('tofinish', value)

    @property
    def toforcefinish(self):
        return self.get_metadata_item('toforcefinish', False)

    @toforcefinish.setter
    def toforcefinish(self, value):
        self.add_metadata_item('toforcefinish', value)

    @property
    def last_updated_at(self):
        last_updated_at = self.get_metadata_item('last_updated_at', None)
        if last_updated_at and type(last_updated_at) in [str]:
            last_updated_at = str_to_date(last_updated_at)
        return last_updated_at

    @last_updated_at.setter
    def last_updated_at(self, value):
        self.add_metadata_item('last_updated_at', value)

    def has_new_updates(self):
        self.last_updated_at = datetime.datetime.utcnow()

    @property
    def to_update_processings(self):
        return self.get_metadata_item('to_update_processings', {})

    @to_update_processings.setter
    def to_update_processings(self, value):
        self.add_metadata_item('to_update_processings', value)

    @property
    def num_run(self):
        return self.get_metadata_item('num_run', 0)

    @num_run.setter
    def num_run(self, value):
        self.add_metadata_item('num_run', value)
        if value is not None and value > 1:
            # for k in self._collections:
            for coll in self.output_collections:
                if type(coll) in [Collection]:
                    if "___idds___" not in coll.name:
                        coll.name = coll.name + "." + str(value)

    @property
    def primary_input_collection(self):
        if self._primary_input_collection:
            return self.collections[self._primary_input_collection]
        return None

    @primary_input_collection.setter
    def primary_input_collection(self, value):
        if type(value) in [str] and len(value) == 8:
            # local value from old idds version
            self._primary_input_collection = value
        else:
            self.set_primary_input_collection(value)

    @property
    def primary_output_collection(self):
        if self._primary_output_collection:
            return self.collections[self._primary_output_collection]
        return None

    @primary_output_collection.setter
    def primary_output_collection(self, value):
        if type(value) in [str] and len(value) == 8:
            # local value from old idds version
            self._primary_output_collection = value
        else:
            self.set_primary_output_collection(value)

    @property
    def input_collections(self):
        if self._primary_input_collection:
            keys = [self._primary_input_collection] + self._other_input_collections
        else:
            keys = self._other_input_collections
        return [self.collections[k] for k in keys]

    @input_collections.setter
    def input_collections(self, value):
        if value and type(value) not in [list, tuple]:
            value = [value]

        if value:
            primary_collection = value[0]
            other_collections = []
            if len(value) > 1:
                other_collections = value[1:]

            self.set_primary_input_collection(primary_collection)

            if other_collections and type(other_collections) not in [list, tuple]:
                other_collections = [other_collections]
            self.add_other_input_collections(other_collections)

    @property
    def output_collections(self):
        if self._primary_output_collection:
            keys = [self._primary_output_collection] + self._other_output_collections
        else:
            keys = self._other_output_collections
        return [self.collections[k] for k in keys]

    @output_collections.setter
    def output_collections(self, value):
        if value and type(value) not in [list, tuple]:
            value = [value]

        if value:
            primary_collection = value[0]
            other_collections = []
            if len(value) > 1:
                other_collections = value[1:]

            self.set_primary_output_collection(primary_collection)

            if other_collections and type(other_collections) not in [list, tuple]:
                other_collections = [other_collections]
            self.add_other_output_collections(other_collections)

    def set_work_name(self, work_name):
        self.work_name = work_name

    def get_work_name(self):
        return self.work_name

    def set_func_site_to_cloud(self, func):
        self.func_site_to_cloud = func

    def get_func_site_to_cloud(self):
        return self.func_site_to_cloud

    def get_is_template(self):
        self.is_template

    def sync_global_parameters(self, global_parameters, sliced_global_parameters=None):
        if sliced_global_parameters:
            self.sliced_global_parameters = sliced_global_parameters

        if global_parameters:
            for key in global_parameters:
                sliced_index = None
                sliced_name = None
                if self.sliced_global_parameters and key in self.sliced_global_parameters:
                    sliced_index = self.sliced_global_parameters[key]['index']
                    sliced_name = self.sliced_global_parameters[key]['name']
                    if type(global_parameters[key]) in [list, tuple] and sliced_index < len(global_parameters[key]):
                        pass
                    else:
                        sliced_index = None
                if not sliced_name:
                    sliced_name = key

                if sliced_index is None:
                    setattr(self, sliced_name, global_parameters[key])
                else:
                    setattr(self, sliced_name, global_parameters[key][sliced_index])

    def get_global_parameter_from_output_data(self, key):
        self.logger.debug("get_global_parameter_from_output_data, key: %s, output_data: %s" % (key, str(self.output_data)))
        gp_output_data = {}
        if self.output_data and type(self.output_data) in [dict]:
            for key in self.output_data:
                new_key = "user_" + str(key)
                gp_output_data[new_key] = self.output_data[key]
        if key in gp_output_data:
            return True, gp_output_data[key]
        else:
            return False, None

    def renew_parameters_from_attributes(self):
        pass

    def add_custom_condition(self, key, value, op='and'):
        # op in ['and', 'or']
        if op and op == 'or':
            op = 'or'
        else:
            op = 'and'
        if op == 'and':
            self.and_custom_conditions[key] = value
        else:
            self.or_custom_conditions[key] = value

    def get_custom_condition_status_value_bool(self, key):
        user_key = "user_" + key
        if hasattr(self, user_key):
            key = user_key

        if hasattr(self, key) and getattr(self, key):
            value = getattr(self, key)
            if type(value) in [str]:
                value = value.lower()
                if value == 'true':
                    return True
                else:
                    return False
            elif type(value) in [bool]:
                return value
            elif type(value) in [int]:
                if value > 0:
                    return True
                else:
                    return False
            else:
                return value
        else:
            return False

    def get_custom_condition_status_value(self, key):
        if self.output_data and key in self.output_data:
            return self.output_data[key]

        user_key = "user_" + key
        if hasattr(self, user_key):
            key = user_key

        if hasattr(self, key) and getattr(self, key):
            return getattr(self, key)
        else:
            return None

    def get_custom_condition_status_real(self):
        if self.or_custom_conditions:
            for key in self.or_custom_conditions:
                value = self.get_custom_condition_status_value(key)
                if value == self.or_custom_conditions[key]:
                    return True

        if self.and_custom_conditions:
            for key in self.and_custom_conditions:
                value = self.get_custom_condition_status_value(key)
                if not (value == self.and_custom_conditions[key]):
                    return False
            return True

        return False

    def get_custom_condition_status(self):
        # self.logger.debug("get_custom_condition_status, or_custom_conditions: %s" % str(self.or_custom_conditions))
        # self.logger.debug("get_custom_condition_status, and_custom_conditions: %s" % str(self.and_custom_conditions))
        # self.logger.debug("get_custom_condition_status, work: %s" % (json_dumps(self, sort_keys=True, indent=4)))

        status = self.get_custom_condition_status_real()
        self.logger.debug("get_custom_condition_status, status: %s" % (status))
        return status

    def get_not_custom_condition_status(self):
        return not self.get_custom_condition_status()

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())
        return self.logger

    def add_errors(self, error):
        self.errors.append(error)

    def get_errors(self):
        return self.errors

    def set_work_id(self, work_id, transforming=True):
        """
        *** Function called by Marshaller and clerk agent.
        *** It's the transform_id set by core_workprogresses
        """
        self.work_id = work_id
        self.transforming = transforming

    def get_work_id(self):
        """
        *** Function called by Marshaller and clerk agent.
        """
        return self.work_id

    def set_request_id(self, request_id):
        """
        *** Function called by Marshaller and clerk agent.
        *** It's the transform_id set by core_workprogresses
        """
        self.request_id = request_id

    def get_request_id(self):
        """
        *** Function called by Marshaller and clerk agent.
        """
        return self.request_id

    # def set_workflow(self, workflow):
    #     self.workflow = workflow

    def clean_work(self):
        self.processings = {}
        self.active_processings = []
        self.cancelled_processings = []
        self.suspended_processings = []
        self.old_processings = []
        self.terminated_msg = ""
        self.output_data = {}
        self.parameters_for_next_task = None

    def set_agent_attributes(self, attrs, req_attributes=None):
        if attrs and self.class_name in attrs:
            if self.agent_attributes is None:
                self.agent_attributes = {}
            for key, value in attrs[self.class_name].items():
                self.agent_attributes[key] = value
        self.logger.info("agent_attributes: %s" % self.agent_attributes)

    def get_agent_attributes(self):
        return self.agent_attributes

    def set_workdir(self, workdir):
        self.workdir = workdir

    def get_workdir(self):
        return self.workdir

    def set_status(self, status):
        """
        *** Function called by Marshaller agent.
        """
        assert(isinstance(status, WorkStatus))
        self.status = status
        # if self.workflow:
        #     self.workflow.work_status_update_trigger(self, status)

    def get_status(self):
        return self.status

    def set_terminated_msg(self, msg):
        """
        *** Function called by Marshaller agent.
        """
        self.terminated_msg = msg
        self.add_errors(msg)

    def get_terminated_msg(self):
        return self.terminated_msg

    def set_output_data(self, data):
        self.output_data = data

    def get_output_data(self):
        return self.output_data

    def set_parameters_for_next_task(self, params):
        self.parameters_for_next_task = params

    def get_parameters_for_next_task(self):
        return self.parameters_for_next_task

    def __eq__(self, obj):
        if self.work_id == obj.work_id:
            return True
        return False

    def __hash__(self):
        return self.work_id

    def __str__(self):
        return str(self.to_dict())

    def get_work_type(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.work_type

    def get_work_tag(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.work_tag

    def set_parameters(self, parameters):
        self.parameters = parameters
        for p in self.parameters:
            if self.parameters[p] is not None and hasattr(self, p):
                # fp = getattr(self, p)
                # fp = self.parameters[p]  # noqa F841
                setattr(self, p, self.parameters[p])

    def get_parameters(self):
        return self.parameters

    def set_arguments(self, arguments):
        self.arguments = arguments

    def get_arguments(self):
        return self.arguments

    def get_ancestry_works(self):
        return []

    def has_to_release_inputs(self):
        if self.backup_to_release_inputs['0'] or self.backup_to_release_inputs['1'] or self.backup_to_release_inputs['2']:
            return True
        return False

    def add_backup_to_release_inputs(self, to_release_inputs):
        if to_release_inputs:
            self.backup_to_release_inputs['0'] = self.backup_to_release_inputs['0'] + to_release_inputs

    def get_backup_to_release_inputs(self):
        to_release_inputs = self.backup_to_release_inputs['0'] + self.backup_to_release_inputs['1'] + self.backup_to_release_inputs['2']
        self.backup_to_release_inputs['2'] = self.backup_to_release_inputs['1']
        self.backup_to_release_inputs['1'] = self.backup_to_release_inputs['0']
        self.backup_to_release_inputs['0'] = []
        return to_release_inputs

    def is_starting(self):
        return self.transforming

    def is_started(self):
        return self.started or self.submitted

    def is_running(self):
        if self.status in [WorkStatus.Running, WorkStatus.Transforming]:
            return True
        return False

    def is_terminated(self):
        """
        *** Function called by Transformer agent.
        """
        if (self.status in [WorkStatus.Finished, WorkStatus.SubFinished, WorkStatus.Failed, WorkStatus.Cancelled, WorkStatus.Suspended, WorkStatus.Expired]
            and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]):   # noqa W503
            return True
        return False

    def is_finished(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Finished] and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]:
            return True
        return False

    def is_subfinished(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.SubFinished] and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]:
            return True
        return False

    def is_processed(self, synchronize=True):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Finished, WorkStatus.SubFinished] and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]:
            return True
        return False

    def is_failed(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Failed] and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]:
            return True
        return False

    def is_expired(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Expired] and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]:
            return True
        return False

    def is_cancelled(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Cancelled] and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]:
            return True
        return False

    def is_suspended(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Suspended] and self.substatus not in [WorkStatus.ToCancel, WorkStatus.ToSuspend, WorkStatus.ToResume]:
            return True
        return False

    def add_next_work(self, work):
        next_works = self.next_works
        next_works.append(work)
        self.next_works = next_works

    def parse_arguments(self):
        try:
            arguments = self.get_arguments()
            parameters = self.get_parameters()
            arguments = arguments.format(**parameters)
            return arguments
        except Exception as ex:
            self.add_errors(str(ex))

    def set_initialized(self):
        self.initialized = True

    def unset_initialized(self):
        self.initialized = False

    def is_initialized(self):
        return self.initialized

    def initialize_work(self):
        if self.parameters and self.arguments:
            # for key in self.parameters.get_param_names():
            #    self.arguments = re.sub(key, str(self.parameters.get_param_value(key)), self.arguments)
            # self.arguments = self.arguments.format(**self.parameters)
            pass
        if not self.is_initialized():
            self.set_initialized()

    def copy(self):
        new_work = copy.deepcopy(self)
        return new_work

    def __deepcopy__(self, memo):
        logger = self.logger
        self.logger = None

        cls = self.__class__
        result = cls.__new__(cls)

        memo[id(self)] = result

        # Deep copy all other attributes
        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memo))

        self.logger = logger
        result.logger = logger
        return result

    def depend_on(self, work):
        return False

    def generate_work_from_template(self):
        logger = self.logger
        self.logger = None
        new_work = copy.deepcopy(self)
        self.logger = logger
        new_work.logger = logger
        # new_work.template_work_id = self.get_internal_id()
        if self.is_template:
            new_work.internal_id = str(uuid.uuid4())[:8]
        return new_work

    def get_template_id(self):
        return self.template_work_id

    def resume_work(self):
        if self.status in [WorkStatus.New, WorkStatus.Ready]:
            pass
        else:
            self.status = WorkStatus.Transforming
        self.polling_retries = 0

    def add_collection_to_collections(self, coll):
        assert(isinstance(coll, dict))
        assert('scope' in coll)
        assert('name' in coll)

        coll_metadata = copy.copy(coll)
        del coll_metadata['scope']
        del coll_metadata['name']
        if 'type' in coll_metadata:
            coll_type = coll_metadata['type']
            del coll_metadata['type']
        else:
            coll_type = CollectionType.Dataset

        collection = Collection(scope=coll['scope'], name=coll['name'], coll_type=coll_type, coll_metadata=coll_metadata)
        self.collections[collection.internal_id] = collection
        return collection

    def set_primary_input_collection(self, coll):
        if coll:
            if type(coll) in [str] and len(coll) == 8:
                # local value from old idds version
                # load value submitted from old idds version
                self._primary_input_collection = coll
            else:
                collection = self.add_collection_to_collections(coll)
                self._primary_input_collection = collection.internal_id

    def get_primary_input_collection(self):
        """
        *** Function called by Marshaller agent.
        """
        if self._primary_input_collection:
            return self.collections[self._primary_input_collection]
        return None

    def set_primary_output_collection(self, coll):
        if coll:
            if type(coll) in [str] and len(coll) == 8:
                # local value from old idds version
                # load value submitted from old idds version
                self._primary_output_collection = coll
            else:
                collection = self.add_collection_to_collections(coll)
                self._primary_output_collection = collection.internal_id

    def get_primary_output_collection(self):
        """
        *** Function called by Marshaller agent.
        """
        if self._primary_output_collection:
            return self.collections[self._primary_output_collection]
        return None

    def add_other_input_collections(self, colls):
        if not colls:
            return
        if type(colls) not in [list, tuple]:
            colls = [colls]

        for coll in colls:
            collection = self.add_collection_to_collections(coll)
            self._other_input_collections.append(collection.internal_id)

    def get_other_input_collections(self):
        return [self.collections[k] for k in self._other_input_collections]

    def add_other_output_collections(self, colls):
        if not colls:
            return
        if type(colls) not in [list, tuple]:
            colls = [colls]

        for coll in colls:
            collection = self.add_collection_to_collections(coll)
            self._other_output_collections.append(collection.internal_id)

    def get_other_output_collections(self):
        return [self.collections[k] for k in self._other_output_collections]

    def get_input_collections(self, poll_externel=False):
        """
        *** Function called by Transformer agent.
        """
        if self._primary_input_collection:
            keys = [self._primary_input_collection] + self._other_input_collections
        else:
            keys = self._other_input_collections
        return [self.collections[k] for k in keys]

    def get_output_collections(self):
        """
        *** Function called by Transformer agent.
        """
        if self._primary_output_collection:
            keys = [self._primary_output_collection] + self._other_output_collections
        else:
            keys = self._other_output_collections
        return [self.collections[k] for k in keys]

    def get_collections(self):
        return [self.collections[k] for k in self.collections.keys()]

    def is_input_collections_closed(self):
        colls = self.get_input_collections()
        for coll in colls:
            if coll.status not in [CollectionStatus.Closed]:
                return False
        return True

    def is_internal_collection(self, coll):
        if (coll.coll_metadata and 'source' in coll.coll_metadata and coll.coll_metadata['source']  # noqa W503
            and type(coll.coll_metadata['source']) == str and coll.coll_metadata['source'].lower() == 'idds'):  # noqa W503
            return True
        return False

    def get_internal_collections(self, coll):
        if coll.coll_metadata and 'request_id' in coll.coll_metadata:
            # relation_type = coll['coll_metadata']['relation_type'] if 'relation_type' in coll['coll_metadata'] else CollectionRelationType.Output
            # colls = core_catalog.get_collections(scope=coll['scope'],
            #                                 name=coll['name'],
            #                                 request_id=coll['coll_metadata']['request_id'],
            #                                 relation_type=relation_type)
            return []
        return []

    def poll_external_collection(self, coll):
        return coll

    def poll_internal_collection(self, coll):
        try:
            if coll.status in [CollectionStatus.Closed]:
                return coll
            else:
                if coll.coll_metadata is None:
                    coll.coll_metadata = {}
                coll.coll_metadata['bytes'] = 0
                coll.coll_metadata['availability'] = 0
                coll.coll_metadata['events'] = 0
                coll.coll_metadata['is_open'] = True
                coll.coll_metadata['run_number'] = 1
                coll.coll_metadata['did_type'] = 'DATASET'
                coll.coll_metadata['list_all_files'] = False
                coll.coll_metadata['interal_colls'] = []

                is_open = False
                internal_colls = self.get_internal_collections(coll)
                for i_coll in internal_colls:
                    if i_coll.status not in [CollectionStatus.Closed]:
                        is_open = True
                    coll.coll_metadata['bytes'] += i_coll['bytes']

                if not is_open:
                    coll_status = CollectionStatus.Closed
                else:
                    coll_status = CollectionStatus.Open
                coll.status = coll_status
                if len(internal_colls) > 1:
                    coll.coll_metadata['coll_type'] = CollectionType.Container
                else:
                    coll.coll_metadata['coll_type'] = CollectionType.Dataset

                return coll
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_internal_input_contents(self, coll):
        """
        Get all input contents from iDDS collections.
        """
        coll = self.collections[self._primary_input_collection]
        internal_colls = self.get_internal_collection(coll)
        internal_coll_ids = [coll.coll_id for coll in internal_colls]
        if internal_coll_ids:
            # contents = catalog.get_contents_by_coll_id_status(coll_id=coll_ids)
            contents = []
        else:
            contents = []
        return contents

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        pass

    def add_output_collections(self, colls):
        """
        """
        if not colls:
            return
        if type(colls) not in [list, tuple]:
            colls = [colls]

        value = colls
        if value:
            primary_collection = value[0]
            other_collections = []
            if len(value) > 1:
                other_collections = value[1:]

            self.set_primary_output_collection(primary_collection)

            if other_collections and type(other_collections) not in [list, tuple]:
                other_collections = [other_collections]
            self.add_other_output_collections(other_collections)

    def get_output_contents(self):
        pass

    def add_log_collections(self, colls):
        if not colls:
            return
        if type(colls) not in [list, tuple]:
            colls = [colls]

        for coll in colls:
            collection = self.add_collection_to_collections(coll)
            self.log_collections.append(collection.internal_id)

    def get_log_collections(self):
        return [self.collections[k] for k in self.log_collections]

    def set_has_new_inputs(self, yes=True):
        self.has_new_inputs = yes

    def has_dependency(self):
        return False

    def get_parent_work_names(self):
        return []

    def get_parent_workload_ids(self):
        return []

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
        *** Function called by Transformer agent.
        New inputs which are not yet mapped to outputs.

        :param mapped_input_output_maps: Inputs that are already mapped.
        """
        inputs = self.get_input_contents()
        # mapped_inputs = mapped_input_output_maps.keys()

        mapped_inputs = []
        for map_id in mapped_input_output_maps:
            map_id_inputs = mapped_input_output_maps[map_id]
            for ip in map_id_inputs:
                if ip['coll_id'] == self.primary_input_collection['coll_id']:
                    mapped_inputs.append(ip['scope'] + ':' + ip['name'])

        new_inputs = []
        for ip in inputs:
            if ip in mapped_inputs:
                pass
            else:
                new_inputs.append(ip)

        new_input_output_maps = {}
        mapped_keys = mapped_input_output_maps.keys()
        if mapped_keys:
            next_key = max(mapped_keys) + 1
        else:
            next_key = 1
        for ip in new_inputs:
            self.num_mapped_inputs += 1
            out_ip = copy.deepcopy(ip)
            out_ip['coll_id'] = self.collections[self._primary_output_collection]['coll_id']
            new_input_output_maps[next_key] = {'inputs': [ip],
                                               'outputs': [out_ip],
                                               'inputs_dependency': [],
                                               'logs': []}
            next_key += 1

        return new_input_output_maps

    def set_collection_id(self, collection, coll_id):
        # print(collection)
        # print(coll_id)
        self.collections[collection.internal_id]['coll_id'] = coll_id

    def should_release_inputs(self, processing=None, poll_operation_time_period=120):
        if self.release_inputs_after_submitting:
            if not poll_operation_time_period:
                poll_operation_time_period = 120

            processing_model = processing.processing
            if (processing_model and processing_model['submitted_at']                                                 # noqa: W503
               and processing_model['submitted_at'] + datetime.timedelta(seconds=int(poll_operation_time_period))    # noqa: W503
               < datetime.datetime.utcnow()):                                                                        # noqa: W503

                if (processing and processing.status
                   and processing.status not in [ProcessingStatus.New, ProcessingStatus.New.value]):                   # noqa: W503
                    # and processing.status not in [ProcessingStatus.New, ProcessingStatus.New.value,                   # noqa: W503
                    #                               ProcessingStatus.Submitting, ProcessingStatus.Submitting.value]):   # noqa: W503
                    return True
            return False
        return True

    def use_dependency_to_release_jobs(self):
        """
        *** Function called by Transformer agent.
        """
        return False

    def require_ext_contents(self):
        return False

    def has_external_content_id(self):
        return False

    def set_work_name_to_coll_map(self, work_name_to_coll_map):
        self.work_name_to_coll_map = work_name_to_coll_map

    def get_work_name_to_coll_map(self):
        return self.work_name_to_coll_map

    def add_processing_to_processings(self, processing):
        # assert(isinstance(processing, dict))
        # assert('processing_metadata' in processing)
        # if 'processing_metadata' not in processing:
        #     processing['processing_metadata'] = {}

        # if 'internal_id' not in processing['processing_metadata']:
        #     processing['processing_metadata']['internal_id'] = str(uuid.uuid1())
        self.processings[processing.internal_id] = processing

    def get_processing_ids(self):
        ids = []
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if p.processing_id:
                ids.append(p.processing_id)
        return ids

    # def set_processing(self, processing):
    #     self.processing = processing

    def set_processing_id(self, processing, processing_id):
        """
        *** Function called by Transformer agent.
        """
        self.processings[processing.internal_id].processing_id = processing_id

    def set_processing_status(self, processing, status, substatus):
        """
        *** Function called by Transformer agent.
        """
        self.processings[processing.internal_id].status = status
        self.processings[processing.internal_id].substatus = substatus
        # if status not in [ProcessingStatus.New, ProcessingStatus.Submitting,
        #                   ProcessingStatus.Submitted, ProcessingStatus.Running]:
        #     if processing['processing_metadata']['internal_id'] in self.active_processings:
        #         del self.active_processings[processing['processing_metadata']['internal_id']]

    def set_processing_output_metadata(self, processing, output_metadata):
        """
        *** Function called by Transformer agent.
        """
        processing = self.processings[processing.internal_id]
        # processing['output_metadata'] = output_metadata
        self.set_output_data(output_metadata)

    def is_processing_substatus_new_operationing(self, processing):
        if processing.substatus in [ProcessingStatus.ToCancel,
                                    ProcessingStatus.ToSuspend,
                                    ProcessingStatus.ToResume,
                                    ProcessingStatus.ToFinish,
                                    ProcessingStatus.ToForceFinish]:
            return True
        return False

    def is_processing_terminated(self, processing):
        self.logger.debug("is_processing_terminated: status: %s, substatus: %s" % (processing.status, processing.substatus))
        if self.is_processing_substatus_new_operationing(processing):
            return False

        if processing.status not in [ProcessingStatus.New,
                                     ProcessingStatus.Submitting,
                                     ProcessingStatus.Submitted,
                                     ProcessingStatus.Running,
                                     ProcessingStatus.ToCancel,
                                     ProcessingStatus.Cancelling,
                                     ProcessingStatus.ToSuspend,
                                     ProcessingStatus.Suspending,
                                     ProcessingStatus.ToResume,
                                     ProcessingStatus.Resuming,
                                     ProcessingStatus.ToFinish,
                                     ProcessingStatus.ToForceFinish]:
            return True
        return False

    def reap_processing(self, processing):
        if self.is_processing_terminated(processing):
            self.active_processings.remove(processing.internal_id)
        else:
            self.logger.error("Cannot reap an unterminated processing: %s" % processing)

    def is_processings_started(self):
        """
        *** Function called by Transformer agent.
        """
        # for p_id in self.active_processings:
        for p_id in self.processings:
            p = self.processings[p_id]
            if p.submitted_at:
                return True
        return False

    def is_processings_running(self):
        """
        *** Function called by Transformer agent.
        """
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if p.status in [ProcessingStatus.Running]:
                return True
        return False

    def is_processings_terminated(self):
        """
        *** Function called by Transformer agent.
        """
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if self.is_processing_terminated(p):
                pass
            else:
                return False
        return True

    def is_processings_finished(self):
        """
        *** Function called by Transformer agent.
        """
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if not self.is_processing_terminated(p) or p.status not in [ProcessingStatus.Finished]:
                return False
        return True

    def is_processings_subfinished(self):
        """
        *** Function called by Transformer agent.
        """
        has_finished = False
        has_failed = False
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if not self.is_processing_terminated(p):
                return False
            else:
                if p.status in [ProcessingStatus.Finished]:
                    has_finished = True
                if p.status in [ProcessingStatus.Failed]:
                    has_failed = True
        if has_finished and has_failed:
            return True
        return False

    def is_processings_failed(self):
        """
        *** Function called by Transformer agent.
        """
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if not self.is_processing_terminated(p) or p.status not in [ProcessingStatus.Failed]:
                return False
        return True

    def is_processings_expired(self):
        """
        *** Function called by Transformer agent.
        """
        has_expired = False
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if not self.is_processing_terminated(p):
                return False
            elif p.status in [ProcessingStatus.Expired]:
                has_expired = True
        if has_expired:
            return True
        return False

    def is_processings_cancelled(self):
        """
        *** Function called by Transformer agent.
        """
        has_cancelled = False
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if not self.is_processing_terminated(p):
                return False
            elif p.status in [ProcessingStatus.Cancelled]:
                has_cancelled = True
        if has_cancelled:
            return True
        return False

    def is_processings_suspended(self):
        """
        *** Function called by Transformer agent.
        """
        has_suspended = False
        for p_id in self.active_processings:
            p = self.processings[p_id]
            if not self.is_processing_terminated(p):
                return False
            elif p.status in [ProcessingStatus.Suspended]:
                has_suspended = True
        if has_suspended:
            return True
        return False

    def create_processing(self, input_output_maps=[]):
        """
        *** Function called by Transformer agent.
        """
        # proc = {'processing_metadata': {'internal_id': str(uuid.uuid1())}}
        proc = Processing()
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc.internal_id)
        return proc

    def get_processing(self, input_output_maps, without_creating=False):
        """
        *** Function called by Transformer agent.
        """
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            if not without_creating:
                # return None
                return self.create_processing(input_output_maps)
                # self.process = process
                # return process
        return None

    def sync_processing(self, processing, processing_model):
        processing.processing = processing_model
        self.set_processing_status(processing, processing_model['status'], processing_model['substatus'])

    def submit_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        raise exceptions.NotImplementedException

    def abort_processing_old(self, processing):
        """
        *** Function called by Carrier agent.
        """
        # raise exceptions.NotImplementedException
        self.tocancel = True
        if (processing and 'processing_metadata' in processing and processing['processing_metadata']                     # noqa W503
            and 'processing' in processing['processing_metadata'] and processing['processing_metadata']['processing']):  # noqa W503
            proc = processing['processing_metadata']['processing']
            proc.tocancel = True

    def suspend_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        # raise exceptions.NotImplementedException
        self.tosuspend = True
        if (processing and 'processing_metadata' in processing and processing['processing_metadata']                     # noqa W503
            and 'processing' in processing['processing_metadata'] and processing['processing_metadata']['processing']):  # noqa W503
            proc = processing['processing_metadata']['processing']
            proc.tosuspend = True

    def resume_processing_old(self, processing):
        """
        *** Function called by Carrier agent.
        """
        # raise exceptions.NotImplementedException
        self.toresume = True
        if (processing and 'processing_metadata' in processing and processing['processing_metadata']                     # noqa W503
            and 'processing' in processing['processing_metadata'] and processing['processing_metadata']['processing']):  # noqa W503
            proc = processing['processing_metadata']['processing']
            proc.toresume = True

    def expire_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        # raise exceptions.NotImplementedException
        self.toexpire = True
        if (processing and 'processing_metadata' in processing and processing['processing_metadata']                     # noqa W503
            and 'processing' in processing['processing_metadata'] and processing['processing_metadata']['processing']):  # noqa W503
            proc = processing['processing_metadata']['processing']
            proc.toexpire = True

    def finish_processing(self, processing, forcing=False):
        """
        *** Function called by Carrier agent.
        """
        # raise exceptions.NotImplementedException
        if forcing:
            self.toforcefinish = True
        else:
            self.tofinish = True
        if (processing and 'processing_metadata' in processing and processing['processing_metadata']                     # noqa W503
            and 'processing' in processing['processing_metadata'] and processing['processing_metadata']['processing']):  # noqa W503
            proc = processing['processing_metadata']['processing']
            proc.tofinish = True
            if forcing:
                proc.toforcefinish = True

    def poll_processing_updates(self, processing, input_output_maps, contents_ext=None, log_prefix=''):
        """
        *** Function called by Carrier agent.
        """
        processing['processing'].has_new_updates()
        raise exceptions.NotImplementedException

    def is_all_outputs_flushed(self, input_output_maps):
        for map_id in input_output_maps:
            outputs = input_output_maps[map_id]['outputs']

            for content in outputs:
                if content['status'] != content['substatus']:
                    return False
        return True

    def syn_work_status(self, input_output_maps, all_updates_flushed=True, output_statistics={}, to_release_input_contents=[]):
        """
        *** Function called by Transformer agent.
        """
        # raise exceptions.NotImplementedException
        self.logger.debug("syn_work_status(%s): is_processings_terminated: %s" % (str(self.get_processing_ids()), str(self.is_processings_terminated())))
        self.logger.debug("syn_work_status(%s): is_input_collections_closed: %s" % (str(self.get_processing_ids()), str(self.is_input_collections_closed())))
        self.logger.debug("syn_work_status(%s): has_new_inputs: %s" % (str(self.get_processing_ids()), str(self.has_new_inputs)))
        self.logger.debug("syn_work_status(%s): has_to_release_inputs: %s" % (str(self.get_processing_ids()), str(self.has_to_release_inputs())))
        self.logger.debug("syn_work_status(%s): to_release_input_contents: %s" % (str(self.get_processing_ids()), str(to_release_input_contents)))
        if self.is_processings_terminated() and self.is_input_collections_closed() and not self.has_new_inputs and not self.has_to_release_inputs() and not to_release_input_contents:
            if not self.is_all_outputs_flushed(input_output_maps):
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

        if self.is_processings_terminated() or self.is_processings_running() or self.is_processings_started():
            self.started = True
        self.logger.debug("syn_work_status(%s): work.status: %s" % (str(self.get_processing_ids()), str(self.status)))

    def sync_work_data(self, status, substatus, work, workload_id=None, output_data=None, processing=None):
        # self.status = work.status
        work.work_id = self.work_id
        work.transforming = self.transforming

        # clerk will update next_works while transformer doesn't.
        # synchronizing work metadata from transformer to clerk needs to keep it at first.
        next_works = self.next_works
        # self.metadata = work.metadata
        self.next_works = next_works

        self.status_statistics = work.status_statistics
        # self.processings = work.processings
        if output_data:
            self.output_data = output_data
        else:
            self.output_data = work.output_data

        self.status = get_work_status_from_transform_processing_status(status)
        self.substatus = get_work_status_from_transform_processing_status(substatus)
        if workload_id:
            self.workload_id = workload_id
        if processing is not None:
            # called by transformer to sync from processing
            if processing.submitted_at:
                self.submitted = True
        else:
            # called by clerk to syn from transform
            self.submitted = work.submitted

        if self.submitted:
            self.started = True

        """
        self.status = WorkStatus(status.value)
        self.substatus = WorkStatus(substatus.value)
        self.workdir = work.workdir
        self.has_new_inputs = work.has_new_inputs
        self.errors = work.errors
        self.next_works = work.next_works

        self.terminated_msg = work.terminated_msg
        self.output_data = work.output_data
        self.parameters_for_next_task = work.parameters_for_next_task

        self.status_statistics = work.status_statistics

        self.processings = work.processings
        self.active_processings = work.active_processings
        self.cancelled_processings = work.cancelled_processings
        self.suspended_processings = work.suspended_processings
        """

    def abort_processing(self, processing, log_prefix=''):
        msg = "abort processing is not implemented"
        self.logger.error(log_prefix + msg)

    def resume_processing(self, processing, log_prefix=''):
        msg = "resume processing is not implemented"
        self.logger.error(log_prefix + msg)

    def add_proxy(self, proxy):
        self.proxy = proxy

    def get_proxy(self):
        return self.proxy

    def set_user_proxy(self):
        if 'X509_USER_PROXY' in os.environ:
            self.original_proxy = os.environ['X509_USER_PROXY']
        if self.get_proxy():
            user_proxy = '/tmp/idds_user_proxy'
            with open(user_proxy, 'w') as fp:
                fp.write(self.get_proxy())
            os.chmod(user_proxy, stat.S_IRUSR | stat.S_IWUSR)
            os.environ['X509_USER_PROXY'] = user_proxy

    def unset_user_proxy(self):
        if self.original_proxy:
            os.environ['X509_USER_PROXY'] = self.original_proxy
        else:
            del os.environ['X509_USER_PROXY']

    def get_external_content_ids(self, processing, log_prefix=''):
        return []
