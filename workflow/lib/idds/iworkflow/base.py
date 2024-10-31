#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

import copy
import base64
import logging
import inspect
import json
import os
import pickle
import traceback
import uuid
import zlib

from typing import Any, Dict, List, Optional, Tuple, Union    # noqa F401

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
                               pre_kwargs=None,
                               args=None,
                               kwargs=None,
                               multi_jobs_kwargs_list=None):

        if args is None:
            args = ()
        if pre_kwargs is None:
            pre_kwargs = {}
        if kwargs is None:
            kwargs = {}
        if multi_jobs_kwargs_list is None:
            multi_jobs_kwargs_list = []
        if not isinstance(args, (tuple, list)):
            raise TypeError('{0!r} is not a valid args list'.format(args))
        if not isinstance(pre_kwargs, dict):
            raise TypeError('{0!r} is not a valid pre_kwargs dict'.format(pre_kwargs))
        if not isinstance(kwargs, dict):
            raise TypeError('{0!r} is not a valid kwargs dict'.format(kwargs))
        if not isinstance(multi_jobs_kwargs_list, list):
            raise TypeError('{0!r} is not a valid multi_jobs_kwargs_list list'.format(multi_jobs_kwargs_list))

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

        if args:
            args = base64.b64encode(zlib.compress(pickle.dumps(args))).decode("utf-8")
        if pre_kwargs:
            pre_kwargs = base64.b64encode(zlib.compress(pickle.dumps(pre_kwargs))).decode("utf-8")
        if kwargs:
            kwargs = base64.b64encode(zlib.compress(pickle.dumps(kwargs))).decode("utf-8")
        if multi_jobs_kwargs_list:
            multi_jobs_kwargs_list = [base64.b64encode(zlib.compress(pickle.dumps(k))).decode("utf-8") for k in multi_jobs_kwargs_list]

        return func_call, (func_name, pre_kwargs, args, kwargs), multi_jobs_kwargs_list

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

    def save_context(self, source_dir, name, context):
        if source_dir and name and context:
            try:
                file_name = name + ".json"
                file_name = os.path.join(source_dir, file_name)
                with open(file_name, 'w') as f:
                    json.dump(context, f)
                self.logger.info(f"Saved context to file {file_name}")
            except Exception as ex:
                self.logger.error(f"Failed to save context to file {file_name}: {ex}")

    def load_context(self, source_dir, name):
        if source_dir and name:
            try:
                context = None
                file_name = name + ".json"
                file_name = os.path.join(source_dir, file_name)
                if os.path.exists(file_name):
                    with open(file_name, 'r') as f:
                        context = json.load(f)
                self.logger.info(f"Loading context from file {file_name}")
                return context
            except Exception as ex:
                self.logger.error(f"Failed to load context from file: {ex}")
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

    def split_setup(self, setup):
        """
        Split setup string
        """
        if ";" not in setup:
            return "", setup

        setup_list = setup.split(";")
        main_setup = setup_list[-1]
        pre_setup = "; ".join(setup_list[:-1])
        pre_setup = pre_setup + "; "
        return pre_setup, main_setup

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        return None

    def load_func(self, func_name):
        """
        Load the function from the source files.

        :raise Exception
        """
        os.environ['IDDS_IWORKFLOW_LOAD'] = 'true'
        func = import_func(func_name)
        del os.environ['IDDS_IWORKFLOW_LOAD']

        return func

    def run_func(self, func, pre_kwargs, args, kwargs):
        """
        Run the function.

        :returns: status, output, error

        :raise Exception.
        """
        try:
            logging.info(f"func type: {type(func)}: {str(func)}")
            logging.info(f"pre_kwargs type: {type(pre_kwargs)}: {str(pre_kwargs)}")
            logging.info(f"args type: {type(args)}: {str(args)}")
            logging.info(f"kwargs type: {type(kwargs)}: {str(kwargs)}")
            kwargs_copy = copy.deepcopy(pre_kwargs)
            kwargs_copy.update(kwargs)
            logging.info(f"start to run function: {str(func)}")
            if kwargs_copy:
                ret = func(*args, **kwargs_copy)
            else:
                ret = func(*args)
            logging.info(f"Successfully run function, ret: {ret}")
            return True, ret, None
        except Exception as ex:
            logging.error(f"Failed to run the function: {str(ex)}")
            logging.error(traceback.format_exc())
            return False, None, str(ex)


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


class CollectionBase(DictBase):
    def __init__(self):
        super(CollectionBase, self).__init__()
        pass
