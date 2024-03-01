#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

import logging
import inspect
import os
import traceback
import uuid

from typing import Any, Dict, List, Optional, Tuple, Union

from idds.common.dict_class import DictMetadata, DictBase
from idds.common.imports import import_func, get_func_name


class IDDSDict(dict):
    def __setitem__(self, key, value):
        if key == 'test':
            pass
        else:
            super().__setitem__(key, value)


class IDDSMetadata(DictMetadata):
    def __init__(self):
        super(IDDSMetadata, self).__init__()


class Base(DictBase):
    def __init__(self):
        super(Base, self).__init__()
        self._internal_id = str(uuid.uuid4())[:8]
        self._template_id = self._internal_id
        self._sequence_id = 0

    @property
    def internal_id(self):
        return self._internal_id

    @internal_id.setter
    def internal_id(self, value):
        self._internal_id = value

    def get_func_name_and_args(self,
                               func,
                               args: Union[List[Any], Optional[Tuple]] = None,
                               kwargs: Optional[Dict[str, Any]] = None,
                               group_kwargs: Optional[list[Dict[str, Any]]] = None):

        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        if group_kwargs is None:
            group_kwargs = []
        if not isinstance(args, (tuple, list)):
            raise TypeError('{0!r} is not a valid args list'.format(args))
        if not isinstance(kwargs, dict):
            raise TypeError('{0!r} is not a valid kwargs dict'.format(kwargs))
        if not isinstance(group_kwargs, list):
            raise TypeError('{0!r} is not a valid group_kwargs list'.format(group_kwargs))

        func_call, func_name = None, None
        if isinstance(func, str):
            func_name = func
        elif inspect.ismethod(func) or inspect.isfunction(func) or inspect.isbuiltin(func):
            # func_name = '{0}.{1}'.format(func.__module__, func.__qualname__)
            func_name = get_func_name(func)
            func_call = func
        else:
            # raise TypeError('Expected a callable or a string, but got: {0}'.format(func))
            func_name = func
        return func_call, (func_name, args, kwargs, group_kwargs)

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

    @logger.setter
    def logger(self, value):
        pass

    def get_internal_id(self):
        return self._internal_id

    def get_template_work_id(self):
        return self._template_id

    def get_sequence_id(self):
        return self._sequence_id

    def get_input_collections(self):
        return []

    def get_output_collections(self):
        return []

    def get_log_collections(self):
        return []

    def prepare(self):
        """
        Prepare the workflow: upload the source files to server.

        :returns id: The workflow id.
        :raise Exception when failing to prepare the workflow.
        """

    def submit(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: The workflow id.
        :raise Exception when failing to submit the workflow.
        """
        self.prepare()
        return None

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        return None

    def load(self, func_name):
        """
        Load the function from the source files.

        :raise Exception
        """
        os.environ['IDDS_IWORKFLOW_LOAD'] = 'true'
        func = import_func(func_name)
        del os.environ['IDDS_IWORKFLOW_LOAD']

        return func

    def run_func(self, func, args, kwargs):
        """
        Run the function.

        :raise Exception.
        """
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            logging.error("Failed to run the function: %s" % str(ex))
            logging.debug(traceback.format_exc())


class Context(DictBase):
    def __init__(self):
        super(Context, self).__init__()
        self._internal_id = str(uuid.uuid4())[:8]

    @property
    def internal_id(self):
        return self._internal_id

    @internal_id.setter
    def internal_id(self, value):
        self._internal_id = value

    def prepare(self):
        """
        Prepare the workflow.
        """
        return None

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        return None
