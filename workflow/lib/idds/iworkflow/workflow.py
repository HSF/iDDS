#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2024

import collections
import datetime
import functools
import logging
import inspect
import os
import tarfile
import uuid

# from types import ModuleType

# from idds.common import exceptions
from idds.common.constants import WorkflowType
from idds.common.utils import setup_logging, create_archive_file, json_dumps, encode_base64
from .asyncresult import AsyncResult
from .base import Base, Context


setup_logging(__name__)


class WorkflowCanvas(object):

    _managed_workflows: collections.deque[Base] = collections.deque()

    @classmethod
    def push_managed_workflow(cls, workflow: Base):
        cls._managed_workflows.appendleft(workflow)

    @classmethod
    def pop_managed_workflow(cls):
        workflow = cls._managed_workflows.popleft()
        return workflow

    @classmethod
    def get_current_workflow(cls):
        try:
            return cls._managed_workflows[0]
        except IndexError:
            return None


class WorkflowContext(Context):
    def __init__(self, name=None, service='panda', source_dir=None, distributed=True, init_env=None):
        super(WorkflowContext, self).__init__()
        self._service = service     # panda, idds, sharefs
        self._request_id = None
        self._type = WorkflowType.iWorkflow

        # self.idds_host = None
        # self.idds_async_host = None
        self._idds_env = {}
        self._panda_env = {}

        self._name = name
        self._source_dir = source_dir
        self.remote_source_file = None

        self._vo = None

        self._queue = None
        self._site = None
        self._cloud = None

        self._working_group = None

        self._priority = 500
        self._core_count = 1
        self._total_memory = 1000          # MB
        self._max_walltime = 7 * 24 * 3600
        self._max_attempt = 5

        self._username = None
        self._userdn = None
        self._type = WorkflowType.iWorkflow
        self._lifetime = 7 * 24 * 3600
        self._workload_id = None
        self._request_id = None

        self.distributed = distributed

        self._broker_initialized = False
        self._brokers = None
        self._broker_timeout = 180
        self._broker_username = None
        self._broker_password = None
        self._broker_destination = None

        self.init_brokers()

        self._token = str(uuid.uuid4())

        self._panda_initialized = False
        self.init_panda()
        self._idds_initialized = False
        self.init_idds()

        self._init_env = init_env

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

    @property
    def distributed(self):
        return self._distributed

    @distributed.setter
    def distributed(self, value):
        self._distributed = value

    @property
    def service(self):
        return self._service

    @service.setter
    def service(self, value):
        self._service = value

    @property
    def init_env(self):
        return self._init_env

    @init_env.setter
    def init_env(self, value):
        self._init_env = value

    @property
    def vo(self):
        return self._vo

    @vo.setter
    def vo(self, value):
        self._vo = value

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, value):
        self._queue = value

    @property
    def site(self):
        return self._site

    @site.setter
    def site(self, value):
        self._site = value

    @property
    def cloud(self):
        return self._cloud

    @cloud.setter
    def cloud(self, value):
        self._cloud = value

    @property
    def working_group(self):
        return self._working_group

    @working_group.setter
    def working_group(self, value):
        self._working_group = value

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, value):
        self._priority = value

    @property
    def core_count(self):
        return self._core_count

    @core_count.setter
    def core_count(self, value):
        self._core_count = value

    @property
    def total_memory(self):
        return self._total_memory

    @total_memory.setter
    def total_memory(self, value):
        self._total_memory = value

    @property
    def max_walltime(self):
        return self._max_walltime

    @max_walltime.setter
    def max_walltime(self, value):
        self._max_walltime = value

    @property
    def max_attempt(self):
        return self._max_attempt

    @max_attempt.setter
    def max_attempt(self, value):
        self._max_attempt = value

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        self._username = value

    @property
    def userdn(self):
        return self._userdn

    @userdn.setter
    def userdn(self, value):
        self._userdn = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def lifetime(self):
        return self._lifetime

    @lifetime.setter
    def lifetime(self, value):
        self._lifetime = value

    @property
    def request_id(self):
        return self._request_id

    @request_id.setter
    def request_id(self, value):
        self._request_id = int(value)

    @property
    def workload_id(self):
        return self._workload_id

    @workload_id.setter
    def workload_id(self, value):
        self._workload_id = value

    @property
    def brokers(self):
        return self._brokers

    @brokers.setter
    def brokers(self, value):
        self._brokers = value

    @property
    def broker_timeout(self):
        return self._broker_timeout

    @broker_timeout.setter
    def broker_timeout(self, value):
        self._broker_timeout = value

    @property
    def broker_username(self):
        return self._broker_username

    @broker_username.setter
    def broker_username(self, value):
        self._broker_username = value

    @property
    def broker_password(self):
        if self._broker_password:
            return self._broker_password
        return None

    @broker_password.setter
    def broker_password(self, value):
        self._broker_password = value

    @property
    def broker_destination(self):
        return self._broker_destination

    @broker_destination.setter
    def broker_destination(self, value):
        self._broker_destination = value

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

    def init_brokers(self):
        if not self._broker_initialized:
            brokers = os.environ.get("IDDS_BROKERS", None)
            broker_destination = os.environ.get("IDDS_BROKER_DESTINATION", None)
            broker_timeout = os.environ.get("IDDS_BROKER_TIMEOUT", 180)
            broker_username = os.environ.get("IDDS_BROKER_USERNAME", None)
            broker_password = os.environ.get("IDDS_BROKER_PASSWORD", None)
            if brokers and broker_destination and broker_username and broker_password:
                self._brokers = brokers
                self._broker_timeout = int(broker_timeout)
                self._broker_username = broker_username
                self._broker_password = broker_password
                self._broker_destination = broker_destination

                self._broker_initialized = True

    def init_idds(self):
        if not self._idds_initialized:
            self._idds_initialized = True
            self._idds_env = self.get_idds_env()

    def init_panda(self):
        if not self._panda_initialized:
            self._panda_initialized = True
            if not self.site:
                self.site = os.environ.get("PANDA_SITE", None)
            if not self.queue:
                self.queue = os.environ.get("PANDA_QUEUE", None)
            if not self.cloud:
                self.cloud = os.environ.get("PANDA_CLOUD", None)
            if not self.vo:
                self.vo = os.environ.get("PANDA_VO", None)
            if not self.working_group:
                self.working_group = os.environ.get("PANDA_WORKING_GROUP", None)

    def initialize(self):
        # env_list = ['IDDS_HOST', 'IDDS_AUTH_TYPE', 'IDDS_VO', 'IDDS_AUTH_NO_VERIFY',
        #             'OIDC_AUTH_ID_TOKEN', 'OIDC_AUTH_VO']
        env_list = ['IDDS_HOST', 'IDDS_AUTH_NO_VERIFY']
        for env in env_list:
            if env not in os.environ and env in self._idds_env:
                os.environ[env] = self._idds_env[env]

        # env_list = ['PANDA_CONFIG_ROOT', 'PANDA_URL_SSL', 'PANDA_URL', 'PANDACACHE_URL', 'PANDAMON_URL',
        #             'PANDA_AUTH', 'PANDA_VERIFY_HOST', 'PANDA_AUTH_VO', 'PANDA_BEHIND_REAL_LB']
        env_list = ['PANDA_URL_SSL', 'PANDA_URL', 'PANDACACHE_URL', 'PANDAMON_URL',
                    'PANDA_VERIFY_HOST', 'PANDA_BEHIND_REAL_LB']
        for env in env_list:
            if env not in os.environ and env in self._panda_env:
                os.environ[env] = self._panda_env[env]
        if 'PANDA_CONFIG_ROOT' not in os.environ:
            os.environ['PANDA_CONFIG_ROOT'] = os.getcwd()

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        if self.service == 'panda':
            set_up = self.setup_panda()
        elif self.service == 'idds':
            set_up = self.setup_idds()
        elif self.service == 'sharefs':
            set_up = self.setup_sharefs()
        else:
            set_up = self.setup_sharefs()

        init_env = self.init_env
        ret = None
        if set_up:
            ret = set_up
        if init_env:
            if ret:
                ret = ret + "; " + init_env
            else:
                ret = init_env
        return ret

    def setup_source_files(self):
        """
        Setup source files.
        """
        if self.service == 'panda':
            return self.setup_panda_source_files()
        elif self.service == 'idds':
            return self.setup_idds_source_files()
        elif self.service == 'sharefs':
            return self.setup_sharefs_source_files()
        return self.setup_sharefs_source_files()

    def download_source_files_from_panda(self, filename):
        """Download and extract the tarball from pandacache"""
        archive_basename = os.path.basename(filename)
        target_dir = os.getcwd()
        full_output_filename = os.path.join(target_dir, archive_basename)
        logging.info("Downloading %s to %s" % (filename, full_output_filename))

        if filename.startswith("https:"):
            panda_cache_url = os.path.dirname(os.path.dirname(filename))
            os.environ["PANDACACHE_URL"] = panda_cache_url
        elif "PANDACACHE_URL" not in os.environ and "PANDA_URL_SSL" in os.environ:
            os.environ["PANDACACHE_URL"] = os.environ["PANDA_URL_SSL"]
        logging.info("PANDACACHE_URL: %s" % os.environ.get("PANDACACHE_URL", None))

        from pandaclient import Client

        attempt = 0
        max_attempts = 3
        done = False
        while attempt < max_attempts and not done:
            attempt += 1
            status, output = Client.getFile(archive_basename, output_path=full_output_filename)
            if status == 0:
                done = True
        logging.info(f"Download archive file from pandacache status: {status}, output: {output}")
        if status != 0:
            raise RuntimeError("Failed to download archive file from pandacache")
        with tarfile.open(full_output_filename, "r:gz") as f:
            f.extractall(target_dir)
        logging.info(f"Extract {full_output_filename} to {target_dir}")
        os.remove(full_output_filename)
        logging.info("Remove %s" % full_output_filename)

    def setup_panda(self):
        """
        Download source files from the panda cache and return the setup env.

        :returns command: `str` to setup the workflow.
        """
        # setup = 'source setup.sh'
        # return setup
        return None

    def setup_idds(self):
        """
        Download source files from the idds cache and return the setup env.

        :returns command: `str` to setup the workflow.
        """
        return None

    def setup_sharefs(self):
        """
        Download source files from the share file system or use the codes from the share file system.
        Return the setup env.

        :returns command: `str` to setup the workflow.
        """
        return None

    def setup_panda_source_files(self):
        """
        Download source files from the panda cache and return the setup env.

        :returns command: `str` to setup the workflow.
        """
        if self.remote_source_file:
            self.download_source_files_from_panda(self.remote_source_file)
        return None

    def setup_idds_source_files(self):
        """
        Download source files from the idds cache and return the setup env.

        :returns command: `str` to setup the workflow.
        """
        if self.remote_source_file:
            self.download_source_files_from_panda(self.remote_source_file)

        return None

    def setup_sharefs_source_files(self):
        """
        Download source files from the share file system or use the codes from the share file system.
        Return the setup env.

        :returns command: `str` to setup the workflow.
        """
        return None

    def get_panda_env(self):
        env_list = ['PANDA_CONFIG_ROOT', 'PANDA_URL_SSL', 'PANDA_URL', 'PANDACACHE_URL', 'PANDAMON_URL',
                    'PANDA_AUTH', 'PANDA_VERIFY_HOST', 'PANDA_AUTH_VO', 'PANDA_BEHIND_REAL_LB']
        ret_envs = {}
        for env in env_list:
            if env in os.environ:
                ret_envs[env] = os.environ[env]
        return ret_envs

    def get_archive_name(self):
        name = self._name.split(":")[-1]
        # name = name + "_" + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        archive_name = "%s.tar.gz" % name
        return archive_name

    def upload_source_files_to_panda(self):
        if not self._source_dir:
            return None

        archive_name = self.get_archive_name()
        archive_file = create_archive_file('/tmp', archive_name, [self._source_dir])
        logging.info("created archive file: %s" % archive_file)
        from pandaclient import Client

        attempt = 0
        max_attempts = 3
        done = False
        while attempt < max_attempts and not done:
            attempt += 1
            status, out = Client.putFile(archive_file, True)
            if status == 0:
                done = True
        logging.info(f"copy_files_to_pandacache: status: {status}, out: {out}")
        if out.startswith("NewFileName:"):
            # found the same input sandbox to reuse
            archive_file = out.split(":")[-1]
        elif out != "True":
            logging.error(out)
            return None

        filename = os.path.basename(archive_file)
        cache_path = os.path.join(os.environ["PANDACACHE_URL"], "cache")
        filename = os.path.join(cache_path, filename)
        return filename

    def prepare_with_panda(self):
        """
        Upload the source files to the panda server.

        :raise Exception when failed.
        """
        logging.info("preparing workflow with PanDA")
        self._panda_env = self.get_panda_env()
        remote_file_name = self.upload_source_files_to_panda()
        self.remote_source_file = remote_file_name
        logging.info("remote source file: %s" % self.remote_source_file)
        logging.info("prepared workflow with PanDA")

    def get_idds_env(self):
        env_list = ['IDDS_HOST', 'IDDS_AUTH_TYPE', 'IDDS_VO', 'IDDS_AUTH_NO_VERIFY',
                    'OIDC_AUTH_ID_TOKEN', 'OIDC_AUTH_VO', 'IDDS_CONFIG']
        ret_envs = {}
        for env in env_list:
            if env in os.environ:
                ret_envs[env] = os.environ[env]
        return ret_envs

    def get_idds_server(self):
        if 'IDDS_HOST' in self._idds_env:
            return self._idds_env['IDDS_HOST']
        if os.environ.get('IDDS_HOST', None):
            return os.environ.get('IDDS_HOST', None)
        return None

    def prepare_with_idds(self):
        """
        Upload the source files to the idds server.

        :raise Exception when failed.
        """
        # idds_env = self.get_idds_env()
        pass

    def prepare_with_sharefs(self):
        """
        Upload the source files to the share file system
        Or directly use the source files on the share file system..

        :raise Exception when failed.
        """
        pass

    def prepare(self):
        """
        Prepare the workflow.
        """
        if self.service == 'panda':
            return self.prepare_with_panda()
        elif self.service == 'idds':
            # return self.prepare_with_idds()
            return self.prepare_with_panda()
        elif self.service == 'sharefs':
            return self.prepare_with_sharefs()
        return self.prepare_with_sharefs()


class Workflow(Base):

    def __init__(self, func=None, service='panda', context=None, source_dir=None, distributed=True,
                 args=None, kwargs={}, group_kwargs=[], update_kwargs=None, init_env=None, is_unique_func_name=False):
        """
        Init a workflow.
        """
        super(Workflow, self).__init__()
        self.prepared = False

        # self._func = func
        self._func, self._func_name_and_args = self.get_func_name_and_args(func, args, kwargs, group_kwargs)
        self._update_kwargs = update_kwargs

        self._name = self._func_name_and_args[0]
        if self._name:
            self._name = self._name.replace('__main__:', '').replace('.py', '').replace(':', '.')
        if not is_unique_func_name:
            if self._name:
                self._name = self._name + "_" + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        source_dir = self.get_source_dir(self._func, source_dir)
        if context is not None:
            self._context = context
        else:
            self._context = WorkflowContext(name=self._name, service=service, source_dir=source_dir, distributed=distributed, init_env=init_env)

    @property
    def service(self):
        return self._context.service

    @property
    def internal_id(self):
        return self._context.internal_id

    @internal_id.setter
    def internal_id(self, value):
        self._context.internal_id = value

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

    def set_request_id(self, request_id):
        self.request_id = request_id

    @property
    def vo(self):
        return self._context.vo

    @vo.setter
    def vo(self, value):
        self._context.vo = value

    @property
    def site(self):
        return self._context.site

    @site.setter
    def site(self, value):
        self._context.site = value

    @property
    def queue(self):
        return self._context.queue

    @queue.setter
    def queue(self, value):
        self._context.queue = value

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
    def type(self):
        return self._context.type

    @type.setter
    def type(self, value):
        self._context.type = value

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
    def token(self):
        return self._context.token

    @token.setter
    def token(self, value):
        self._context.token = value

    def get_work_tag(self):
        return 'iWorkflow'

    def get_work_type(self):
        return WorkflowType.iWorkflow

    def get_work_name(self):
        return self._name

    @property
    def group_parameters(self):
        return self._func_name_and_args[3]

    @group_parameters.setter
    def group_parameters(self, value):
        raise Exception("Not allwed to update group parameters")

    def to_dict(self):
        func = self._func
        self._func = None
        obj = super(Workflow, self).to_dict()
        self._func = func
        return obj

    def get_source_dir(self, func, source_dir):
        if source_dir:
            return source_dir
        if func:
            if inspect.isbuiltin(func):
                return None
            source_file = inspect.getsourcefile(func)
            if not source_file:
                return None
            file_path = os.path.abspath(source_file)
            return os.path.dirname(file_path)
        return None

    def prepare(self):
        """
        Prepare the workflow: for example uploading the source codes to cache server.
        :returns command: `str` to setup the workflow.
        """
        if not self.prepared:
            self._context.prepare()
            self.prepared = True

    def submit_to_idds_server(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: the workflow id.
        :raise Exception when failing to submit the workflow.
        """
        # iDDS ClientManager
        from idds.client.clientmanager import ClientManager

        client = ClientManager(host=self._context.get_idds_server())
        request_id = client.submit(self, use_dataset_name=False)

        logging.info("Submitted into iDDS with request id=%s", str(request_id))
        return request_id

    def submit_to_panda_server(self):
        """
        Submit the workflow to the iDDS server through PanDA service.

        :returns id: the workflow id.
        :raise Exception when failing to submit the workflow.
        """
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self._context.get_idds_server()
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  manager=True)
        request_id = client.submit(self, username=None, use_dataset_name=False)

        logging.info("Submitted into PanDA-iDDS with request id=%s", str(request_id))
        return request_id

    def submit(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: the workflow id.
        :raise Exception when failing to submit the workflow.
        """
        self.prepare()
        if self.service == 'panda':
            request_id = self.submit_to_panda_server()
        else:
            request_id = self.submit_to_idds_server()

        try:
            self._context.request_id = int(request_id)
            return request_id
        except Exception as ex:
            logging.info("Request id (%s) is not integer, there should be some submission errors: %s" % (request_id, str(ex)))

        return None

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        return self._context.setup()

    def setup_source_files(self):
        """
        Setup location of source files
        """
        return self._context.setup_source_files()

    def load(self, func_name):
        """
        Load the function from the source files.

        :raise Exception
        """
        os.environ['IDDS_IWORKFLOW_LOAD_WORKFLOW'] = 'true'
        func = super(Workflow, self).load(func_name)
        del os.environ['IDDS_IWORKFLOW_LOAD_WORKFLOW']

        return func

    def pre_run(self):
        # test AsyncResult
        workflow_context = self._context
        if workflow_context.distributed:
            logging.info("Test AsyncResult")
            a_ret = AsyncResult(workflow_context, wait_num=1, timeout=30)
            a_ret.subscribe()

            async_ret = AsyncResult(workflow_context, internal_id=a_ret.internal_id)
            test_result = "AsyncResult test (request_id: %s)" % workflow_context.request_id
            logging.info("AsyncResult publish: %s" % test_result)
            async_ret.publish(test_result)

            ret_q = a_ret.wait_result(force_return_results=True)
            logging.info("AsyncResult results: %s" % str(ret_q))
            if ret_q:
                if ret_q == test_result:
                    logging.info("AsyncResult test succeeded")
                    return True
                else:
                    logging.info("AsyncResult test failed (published: %s, received: %s)" % (test_result, ret_q))
                    return False
            else:
                logging.info("Not received results")
                return False
        return True

    def run(self):
        """
        Run the workflow.
        """
        # with self:
        if True:
            self.pre_run()

            func_name, args, kwargs, group_kwargs = self._func_name_and_args
            if self._func is None:
                func = self.load(func_name)
                self._func = func
            ret = self.run_func(self._func, args, kwargs)

            return ret

    # Context Manager -----------------------------------------------
    def __enter__(self):
        WorkflowCanvas.push_managed_workflow(self)
        return self

    def __exit__(self, _type, _value, _tb):
        WorkflowCanvas.pop_managed_workflow()

    # /Context Manager ----------------------------------------------

    def get_run_command(self):
        cmd = "run_workflow --type workflow "
        cmd += "--context %s --original_args %s " % (encode_base64(json_dumps(self._context)),
                                                     encode_base64(json_dumps(self._func_name_and_args)))
        cmd += "--update_args ${IN/L}"
        return cmd

    def get_runner(self):
        setup = self.setup()
        cmd = ""
        run_command = self.get_run_command()

        if setup:
            cmd = ' --setup "' + setup + '" '
        if cmd:
            cmd = cmd + " " + run_command
        else:
            cmd = run_command
        return cmd

    def get_func_name(self):
        func_name = self._func_name_and_args[0]
        return func_name


# foo = workflow(arg)(foo)
def workflow(func=None, *, lazy=False, service='panda', source_dir=None, primary=False, distributed=True):
    if func is None:
        return functools.partial(workflow, lazy=lazy)

    if 'IDDS_IWORKFLOW_LOAD_WORKFLOW' in os.environ:
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            f = Workflow(func, service=service, source_dir=source_dir, distributed=distributed)

            if lazy:
                return f

            return f.run()
        except Exception as ex:
            logging.error("Failed to run workflow %s: %s" % (func, ex))
            raise ex
        except:
            raise
    return wrapper


def workflow_old(func=None, *, lazy=False, service='panda', source_dir=None, primary=False, distributed=True):

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                f = Workflow(func, service=service, source_dir=source_dir, distributed=distributed)

                if lazy:
                    return f

                return f.run()
            except Exception as ex:
                logging.error("Failed to run workflow %s: %s" % (func, ex))
                raise ex
            except:
                raise
        return wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)
