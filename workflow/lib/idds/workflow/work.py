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
import re
import uuid

from idds.common.constants import WorkStatus
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
                 output_collections=None, log_collections=None,
                 workflow=None, logger=None):
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
        :param workflow: The workflow the current work belongs to.
        """
        self.internal_id = str(uuid.uuid1())

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
        self.workflow = workflow
        self.transforming = False

        self.collections = {}
        self.primary_input_collection = None
        self.other_input_collections = []
        self.output_collections = []
        self.log_collections = []
        # self.primary_input_collection = primary_input_collection
        self.set_primary_input_collection(primary_input_collection)
        # self.other_input_collections = other_input_collections
        self.add_other_input_collections(other_input_collections)
        self.add_output_collections(output_collections)
        self.add_log_collections(log_collections)

        self._has_new_inputs = True

        self.status = WorkStatus.New
        self.next_works = []

        self.processings = {}
        self.active_processings = []
        self.terminated_msg = ""

        self.status_statistics = {}

    def get_class_name(self):
        return self.__class__.__name__

    def get_internal_id(self):
        return self.internal_id

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())

    def set_work_id(self, work_id, transforming=True):
        self.work_id = work_id
        self.transforming = transforming

    def get_work_id(self):
        return self.work_id

    # def set_workflow(self, workflow):
    #     self.workflow = workflow

    def set_status(self, status):
        assert(isinstance(status, WorkStatus))
        self.status = status
        # if self.workflow:
        #     self.workflow.work_status_update_trigger(self, status)

    def set_terminated_msg(self, msg):
        self.terminated_msg = msg

    def get_terminated_msg(self):
        return self.terminated_msg

    def __eq__(self, obj):
        if self.work_id == obj.work_id:
            return True
        return False

    def __hash__(self):
        return self.work_id

    def copy(self):
        return copy.deepcopy(self)

    """
    def to_dict(self):
        return {key: value for key, value
                in self.__dict__.items() if not key.startswith('_')}
    """

    def __str__(self):
        return str(self.to_dict())

    def get_work_type(self):
        return self.work_type

    def get_work_tag(self):
        return self.work_tag

    def set_parameters(self, parameters):
        self.parameters = parameters

    def syn_work_status(self):
        pass

    def is_terminated(self):
        if self.status in [WorkStatus.Finished, WorkStatus.SubFinished, WorkStatus.Failed, WorkStatus.Cancelled]:
            return True
        return False

    def is_finished(self):
        if self.status in [WorkStatus.Finished]:
            return True
        return False

    def is_subfinished(self):
        if self.status in [WorkStatus.SubFinished]:
            return True
        return False

    def is_failed(self):
        if self.status in [WorkStatus.Failed, WorkStatus.Cancelled]:
            return True
        return False

    def add_next_work(self, work):
        self.next_works.append(work)

    def initialize_work(self):
        if self.parameters:
            for key in self.parameters.get_param_names():
                self.arguments = re.sub(key, str(self.parameters.get_param_value(key)), self.arguments)

    def add_collection_to_collections(self, coll):
        assert(isinstance(coll, dict))
        assert('scope' in coll)
        assert('name' in coll)
        if 'coll_metadata' not in coll:
            coll['coll_metadata'] = {}
        coll['coll_metadata']['internal_id'] = str(uuid.uuid1())
        self.collections[coll['coll_metadata']['internal_id']] = coll

    def set_primary_input_collection(self, coll):
        self.add_collection_to_collections(coll)
        self.primary_input_collection = coll['coll_metadata']['internal_id']

    def get_primary_input_collection(self):
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
        keys = [self.primary_input_collection] + self.other_input_collections
        return [self.collections[k] for k in keys]

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        pass

    def add_output_collections(self, colls):
        """
        if scope is None:
            self.output_collection_scope = self.input_collection_scope
        else:
            self.output_collection_scope = scope

        if name is None:
            self.output_collection_name = self.input_collection_name + "." + self.work_type + "." + str(self.work_id)
        else:
            self.output_collection_name = name
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
        return self._has_new_inputs

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
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
        self.collections[collection['coll_metadata']['internal_id']]['coll_id'] = coll_id

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
        self.processings[processing['processing_metadata']['internal_id']]['processing_id'] = processing_id

    def create_processing(self, input_out_maps):
        proc = {'processing_metadata': {'internal_id': str(uuid.uuid1())}}
        self.add_processing_to_processings(proc)
        self.active_processings.append(proc['processing_metadata']['internal_id'])

    def get_processing(self, input_output_maps):
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            return None
            # self.process = process
            # return process

    def submit_processing(self):
        pass

    def poll_processing(self):
        pass
