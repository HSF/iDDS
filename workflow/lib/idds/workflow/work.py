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
import re

from .base import Base


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

    def __init__(self, setup, executable, arguments, parameters, work_type=None, exec_type='local', sandbox=None, work_id=None):
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
        """
        self.setup = setup
        self.executable = executable
        self.arguments = arguments
        self.parameters = parameters

        self.work_type = work_type
        self.exec_type = exec_type
        self.sandbox = sandbox
        self.work_id = work_id

        self.input_collection_scope, self.input_collection_name = None, None
        self.output_collection_scope, self.output_collection_name = None, None

        self.status = 'new'
        self.next_works = []

    def copy(self):
        return copy.deepcopy(self)

    def is_terminated(self):
        if self.status in []:
            return True
        return False

    def add_next_work(self, work):
        self.next_works.append(work)

    def initialize_work(self):
        for key in self.parameters.get_param_names():
            self.arguments = re.sub(key, str(self.parameters.get_param_value(key)), self.arguments)

    def set_input_collection(self, scope, name):
        self.input_collection_scope = scope
        self.input_collection_name = name

    def get_input_collection(self):
        return self.input_collection_scope, self.input_collection_name

    def get_input_contents(self):
        pass

    def set_output_collection(self, scope=None, name=None):
        if scope is None:
            self.output_collection_scope = self.input_collection_scope + ""
        self.input_collection_name = name

    def get_output_collection(self):
        return self.output_collection_scope, self.output_collection_name

    def get_output_contents(self):
        pass
