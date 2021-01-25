#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import copy
import logging
import os
import stat
import uuid

from idds.common import exceptions
from idds.common.constants import WorkStatus, ProcessingStatus
from idds.common.utils import setup_logging

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


class Work(Base):

    def __init__(self, executable=None, arguments=None, parameters=None, setup=None, work_type=None,
                 work_tag=None, exec_type='local', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 output_collections=None, log_collections=None, release_inputs_after_submitting=False,
                 agent_attributes=None,
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
        self.internal_id = str(uuid.uuid1())
        self.template_work_id = self.internal_id
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

        self.work_type = work_type
        self.work_tag = work_tag
        self.exec_type = exec_type
        self.sandbox = sandbox
        self.work_id = work_id
        # self.workflow = workflow
        self.transforming = False
        self.workdir = None

        self.collections = {}
        self.primary_input_collection = None
        self.other_input_collections = []
        self.output_collections = []
        self.log_collections = []
        # self.primary_input_collection = primary_input_collection
        self.set_primary_input_collection(primary_input_collection)
        # self.other_input_collections = other_input_collections
        if other_input_collections and type(other_input_collections) not in [list, tuple]:
            other_input_collections = [other_input_collections]
        self.add_other_input_collections(other_input_collections)
        if output_collections and type(output_collections) not in [list, tuple]:
            output_collections = [output_collections]
        self.add_output_collections(output_collections)
        if log_collections and type(log_collections) not in [list, tuple]:
            log_collections = [log_collections]
        self.add_log_collections(log_collections)

        self.release_inputs_after_submitting = release_inputs_after_submitting
        self._has_new_inputs = True

        self.status = WorkStatus.New
        self.errors = []
        self.next_works = []

        self.processings = {}
        self.active_processings = []
        self.terminated_msg = ""
        self.output_data = None
        self.parameters_for_next_task = None

        self.status_statistics = {}

        self.agent_attributes = agent_attributes

        self.proxy = None
        self.original_proxy = None

    def get_class_name(self):
        return self.__class__.__name__

    def get_internal_id(self):
        return self.internal_id

    def set_sequence_id(self, seq_id):
        self.sequence_id = seq_id

    def get_sequence_id(self):
        return self.sequence_id

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())

    def add_errors(self, error):
        self.errors.append(error)

    def get_errors(self):
        return self.errors

    def set_work_id(self, work_id, transforming=True):
        """
        *** Function called by Marshaller agent.
        *** It's the transform_id set by core_workprogresses
        """
        self.work_id = work_id
        self.transforming = transforming

    def get_work_id(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.work_id

    # def set_workflow(self, workflow):
    #     self.workflow = workflow

    def set_agent_attributes(self, attrs, req_attributes=None):
        if attrs and self.class_name in attrs:
            self.agent_attributes = attrs[self.class_name]
        self.logger.info("agent_attributes: %s" % self.agent_attributes)

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

    """
    def to_dict(self):
        return {key: value for key, value
                in self.__dict__.items() if not key.startswith('_')}
    """

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

    def get_parameters(self):
        return self.parameters

    def set_arguments(self, arguments):
        self.arguments = arguments

    def get_arguments(self):
        return self.arguments

    def is_terminated(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Finished, WorkStatus.SubFinished, WorkStatus.Failed, WorkStatus.Cancelled]:
            return True
        return False

    def is_finished(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Finished]:
            return True
        return False

    def is_subfinished(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.SubFinished]:
            return True
        return False

    def is_failed(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Failed]:
            return True
        return False

    def is_cancelled(self):
        """
        *** Function called by Transformer agent.
        """
        if self.status in [WorkStatus.Cancelled]:
            return True
        return False

    def add_next_work(self, work):
        self.next_works.append(work)

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

    def generate_work_from_template(self):
        logger = self.logger
        self.logger = None
        new_work = copy.deepcopy(self)
        self.logger = logger
        new_work.logger = logger
        # new_work.template_work_id = self.get_internal_id()
        new_work.internal_id = str(uuid.uuid1())
        return new_work

    def get_template_id(self):
        return self.template_work_id

    def add_collection_to_collections(self, coll):
        assert(isinstance(coll, dict))
        assert('scope' in coll)
        assert('name' in coll)
        if 'coll_metadata' not in coll:
            coll['coll_metadata'] = {}
        coll['coll_metadata']['internal_id'] = str(uuid.uuid1())
        self.collections[coll['coll_metadata']['internal_id']] = coll

    def set_primary_input_collection(self, coll):
        if coll:
            self.add_collection_to_collections(coll)
            self.primary_input_collection = coll['coll_metadata']['internal_id']

    def get_primary_input_collection(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.collections[self.primary_input_collection]

    def add_other_input_collections(self, colls):
        if not colls:
            return

        for coll in colls:
            self.add_collection_to_collections(coll)
            self.other_input_collections.append(coll['coll_metadata']['internal_id'])

    def get_other_input_collections(self):
        return [self.collections[k] for k in self.other_input_collections]

    def get_input_collections(self):
        """
        *** Function called by Transformer agent.
        """
        keys = [self.primary_input_collection] + self.other_input_collections
        return [self.collections[k] for k in keys]

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

        for coll in colls:
            self.add_collection_to_collections(coll)
            self.output_collections.append(coll['coll_metadata']['internal_id'])

    def get_output_collections(self):
        return [self.collections[k] for k in self.output_collections]

    def get_output_contents(self):
        pass

    def add_log_collections(self, colls):
        if not colls:
            return

        for coll in colls:
            self.add_collection_to_collections(coll)
            self.log_collections.append(coll['coll_metadata']['internal_id'])

    def get_log_collections(self):
        return [self.collections[k] for k in self.log_collections]

    def set_has_new_inputs(self, yes=True):
        self._has_new_inputs = yes

    def has_new_inputs(self):
        """
        *** Function called by Transformer agent.
        """
        return self._has_new_inputs

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
        *** Function called by Transformer agent.
        New inputs which are not yet mapped to outputs.

        :param mapped_input_output_maps: Inputs that are already mapped.
        """
        inputs = self.get_input_contents()
        # mapped_inputs = mapped_input_output_maps.keys()
        next_map_id = max(mapped_input_output_maps.keys()) + 1

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
        new_input_maps = {}
        for new_input in new_inputs:
            new_input_maps[next_map_id] = [new_input]
        return new_input_maps

    def set_collection_id(self, collection, coll_id):
        # print(collection)
        # print(coll_id)
        self.collections[collection['coll_metadata']['internal_id']]['coll_id'] = coll_id

    def should_release_inputs(self, processing=None):
        if self.release_inputs_after_submitting:
            if (processing and 'status' in processing
                and processing['status'] in [ProcessingStatus.Submitted, ProcessingStatus.Submitted.value]):  # noqa: W503
                return True
            return False
        return True

    def add_processing_to_processings(self, processing):
        assert(isinstance(processing, dict))
        # assert('processing_metadata' in processing)
        if 'processing_metadata' not in processing:
            processing['processing_metadata'] = {}

        if 'internal_id' not in processing['processing_metadata']:
            processing['processing_metadata']['internal_id'] = str(uuid.uuid1())
        self.processings[processing['processing_metadata']['internal_id']] = processing

    # def set_processing(self, processing):
    #     self.processing = processing

    def set_processing_id(self, processing, processing_id):
        """
        *** Function called by Transformer agent.
        """
        self.processings[processing['processing_metadata']['internal_id']]['processing_id'] = processing_id

    def set_processing_status(self, processing, status):
        """
        *** Function called by Transformer agent.
        """
        self.processings[processing['processing_metadata']['internal_id']]['status'] = status
        # if status not in [ProcessingStatus.New, ProcessingStatus.Submitting,
        #                   ProcessingStatus.Submitted, ProcessingStatus.Running]:
        #     if processing['processing_metadata']['internal_id'] in self.active_processings:
        #         del self.active_processings[processing['processing_metadata']['internal_id']]

    def set_processing_output_metadata(self, processing, output_metadata):
        """
        *** Function called by Transformer agent.
        """
        processing = self.processings[processing['processing_metadata']['internal_id']]
        processing['output_metadata'] = output_metadata
        self.set_output_data(output_metadata)

    def is_processing_terminated(self, processing):
        if 'status' in processing and processing['status'] not in [ProcessingStatus.New,
                                                                   ProcessingStatus.Submitting,
                                                                   ProcessingStatus.Submitted,
                                                                   ProcessingStatus.Running]:
            return True
        return False

    def reap_processing(self, processing):
        if self.is_processing_terminated(processing):
            self.active_processings.remove(processing['processing_metadata']['internal_id'])
        else:
            self.logger.error("Cannot reap an unterminated processing: %s" % processing)

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
            if not self.is_processing_terminated(p) or p['status'] not in [ProcessingStatus.Finished]:
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
                if p['status'] in [ProcessingStatus.Finished]:
                    has_finished = True
                if p['status'] in [ProcessingStatus.Failed]:
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
            if not self.is_processing_terminated(p) or p['status'] not in [ProcessingStatus.Failed]:
                return False
        return True

    def create_processing(self, input_output_maps):
        """
        *** Function called by Transformer agent.
        """
        proc = {'processing_metadata': {'internal_id': str(uuid.uuid1())}}
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc['processing_metadata']['internal_id'])
        return proc

    def get_processing(self, input_output_maps):
        """
        *** Function called by Transformer agent.
        """
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            return None
            # self.process = process
            # return process

    def submit_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        raise exceptions.NotImplementedException

    def abort_processing(self, processing):
        """
        *** Function called by Carrier agent.
        """
        raise exceptions.NotImplementedException

    def poll_processing_updates(self, processing, input_output_maps):
        """
        *** Function called by Carrier agent.
        """
        raise exceptions.NotImplementedException

    def syn_work_status(self, input_output_maps):
        """
        *** Function called by Transformer agent.
        """
        raise exceptions.NotImplementedException

    def sync_work_data(self, work):
        self.status = work.status
        self.workdir = work.workdir
        self._has_new_inputs = work._has_new_inputs
        self.errors = work.errors
        self.next_works = work.next_works

        self.terminated_msg = work.terminated_msg
        self.output_data = work.output_data
        self.parameters_for_next_task = work.parameters_for_next_task

        self.status_statistics = work.status_statistics

        self.processings = work.processings
        self.active_processings = work.active_processings

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
