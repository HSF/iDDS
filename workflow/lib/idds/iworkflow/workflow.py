#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2024
# - Lino Oscar Gerlach, <lino.oscar.gerlach@cern.ch>, 2024

import base64
import collections
import datetime
import functools
import logging
import inspect
import os
import pickle
import tarfile
import traceback
import uuid
import zlib

# from types import ModuleType

from idds.common import exceptions
from idds.common.constants import WorkflowType
from idds.common.utils import setup_logging, create_archive_file, json_dumps, json_loads, encode_base64, modified_environ, is_panda_client_verbose
from .asyncresult import AsyncResult
from .base import Base, Context


setup_logging(__name__)


class WorkflowCanvas(object):

    _managed_workflows = collections.deque()

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
    def __init__(self, name=None, service='panda', source_dir=None, workflow_type=WorkflowType.iWorkflow, distributed=True,
                 max_walltime=24 * 3600, init_env=None, exclude_source_files=[], clean_env=None, enable_separate_log=False,
                 cloud=None, site=None, queue=None, vo=None, container_options=None):
        super(WorkflowContext, self).__init__()
        self._service = service     # panda, idds, sharefs
        self._request_id = None
        self._workflow_type = workflow_type

        # self.idds_host = None
        # self.idds_async_host = None
        self._idds_env = {}
        self._panda_env = {}

        self._name = name
        self._source_dir = source_dir
        self.remote_source_file = None

        self._vo = vo

        self._queue = queue
        self._site = site
        self._cloud = queue

        self._working_group = None

        self._priority = 500
        self._core_count = 1
        self._total_memory = 1000          # MB
        self._max_walltime = max_walltime
        self._max_attempt = 5

        self._username = None
        self._userdn = None
        self._workflow_type = workflow_type
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

        # self.init_brokers()

        self._token = str(uuid.uuid4())

        self._panda_initialized = False
        self.init_panda()
        self._idds_initialized = False
        self.init_idds()

        self.init_env = init_env
        self._clean_env = clean_env

        self._exclude_source_files = []
        if exclude_source_files:
            if type(exclude_source_files) in [list, tuple]:
                self._exclude_source_files = exclude_source_files
            else:
                self._exclude_source_files = [exclude_source_files]

        self._enable_separate_log = enable_separate_log

        self._container_options = container_options

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
        if self._init_env:
            self._init_env = self._init_env + " "

    @property
    def clean_env(self):
        return self._clean_env

    @clean_env.setter
    def clean_env(self, value):
        self._clean_env = value
        if self._clean_env:
            self._clean_env = self._clean_env + " "

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
    def workflow_type(self):
        return self._workflow_type

    @workflow_type.setter
    def workflow_type(self, value):
        self._workflow_type = value

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
    def enable_separate_log(self):
        return self._enable_separate_log

    @enable_separate_log.setter
    def enable_separate_log(self, value):
        self._enable_separate_log = value

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

    def get_source_dir(self):
        return self._source_dir

    @property
    def container_options(self):
        return self._container_options

    @container_options.setter
    def container_options(self, value):
        self._container_options = value

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, value):
        self._token = value

    def init_brokers(self):
        if not self._broker_initialized:
            logging.info("To initialize broker")
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
                logging.info("Initialized brokers from environment")
            else:
                logging.info("Getting brokers information from central service")
                broker_info = self.get_broker_info()
                if broker_info:
                    brokers = broker_info.get("brokers", None)
                    broker_destination = broker_info.get("broker_destination", None)
                    broker_timeout = broker_info.get("broker_timeout", 180)
                    broker_username = broker_info.get("broker_username", None)
                    broker_password = broker_info.get("broker_password", None)
                    if brokers and broker_destination and broker_username and broker_password:
                        self._brokers = brokers
                        self._broker_timeout = int(broker_timeout)
                        self._broker_username = broker_username
                        self._broker_password = broker_password
                        self._broker_destination = broker_destination

                        self._broker_initialized = True
                        logging.info("Initialized brokers from central service")
                    else:
                        logging.warn("Broker information from the central service is missing, will not initialize it")
        if not self._broker_initialized:
            logging.warn("Broker is not initialized")
        return self._broker_initialized

    def get_broker_info_from_idds_server(self):
        """
        Get broker infomation from the iDDS server.

        :raise Exception when failing to get broker information.
        """
        logging.info("Getting broker information through idds server.")
        # iDDS ClientManager
        from idds.client.clientmanager import ClientManager

        client = ClientManager(host=self.get_idds_server(), timeout=60)
        ret = client.get_metainfo(name='asyncresult_config')
        if type(ret) in (list, tuple) and ret[0] is True:
            return ret[1]
        else:
            logging.warn(f"Failed to get broker info: {ret}")
            return None

        return ret

    def get_broker_info_from_panda_server(self):
        """
        Get broker infomation from the iDDS server through PanDA service.

        :raise Exception when failing to get broker information.
        """
        logging.info("Get broker information through panda server.")

        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self.get_idds_server()
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)
        ret = client.get_metainfo(name='asyncresult_config')
        if ret[0] == 0 and ret[1][0]:
            idds_ret = ret[1][1]
            if type(idds_ret) in (list, tuple) and idds_ret[0] is True:
                meta_info = idds_ret[1]
                if type(meta_info) in [dict]:
                    pass
                elif type(meta_info) in [str]:
                    try:
                        meta_info = json_loads(meta_info)
                    except Exception as ex:
                        logging.warn("Failed to json loads meta info(%s): %s" % (meta_info, ex))
            else:
                meta_info = None
                logging.warn("Failed to get meta info: %s" % str(ret))
        else:
            meta_info = None
            logging.warn("Failed to get meta info: %s" % str(ret))

        return meta_info

    def get_broker_info(self):
        try:
            if self.service == 'panda':
                return self.get_broker_info_from_panda_server()
            return self.get_broker_info_from_idds_server()
        except Exception as ex:
            logging.error("Failed to get broker info: %s" % str(ex))

    def init_idds(self):
        if not self._idds_initialized:
            self._idds_initialized = True
            self._idds_env = self.get_idds_env()

    def init_panda(self):
        if not self._panda_initialized:
            self._panda_initialized = True
            self._panda_env = self.get_panda_env()
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

    def global_setup(self):
        if self.service == 'panda':
            set_up = self.setup_panda()
        elif self.service == 'idds':
            set_up = self.setup_idds()
        elif self.service == 'sharefs':
            set_up = self.setup_sharefs()
        else:
            set_up = self.setup_sharefs()
        return set_up

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        set_up = self.global_setup()

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

    def get_clean_env(self):
        return self.clean_env

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
        return target_dir

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
            target_dir = self.download_source_files_from_panda(self.remote_source_file)
            self._source_dir = target_dir
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
        archive_file = create_archive_file('/tmp', archive_name, [self._source_dir], exclude_files=self._exclude_source_files)
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
        if self.remote_source_file is None:
            raise exceptions.IDDSException("Failed to upload source files to PanDA cache. Please check log file ${IDDS_LOG_FILE} or direct output for details.")
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
        return os.environ.get('IDDS_HOST', None)

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

    def __init__(self, func=None, service='panda', context=None, source_dir=None, local=False, distributed=True,
                 pre_kwargs={}, args=None, kwargs={}, multi_jobs_kwargs_list=[], current_job_kwargs=None, name=None,
                 init_env=None, is_unique_func_name=False, max_walltime=24 * 3600, source_dir_parent_level=None,
                 enable_separate_log=False, exclude_source_files=[], clean_env=None, container_options=None):
        """
        Init a workflow.
        """
        super(Workflow, self).__init__()
        self.prepared = False

        # self._func = func
        self._func, self._func_name_and_args, self._multi_jobs_kwargs_list = self.get_func_name_and_args(func, pre_kwargs, args, kwargs, multi_jobs_kwargs_list)
        self._current_job_kwargs = current_job_kwargs
        if self._current_job_kwargs:
            self._current_job_kwargs = base64.b64encode(zlib.compress(pickle.dumps(self._current_job_kwargs))).decode("utf-8")

        if name:
            self._name = name
        else:
            self._name = self._func_name_and_args[0]
            if self._name:
                self._name = self._name.replace('__main__:', '').replace('.py', '').replace(':', '.')
                self._name = self._name.replace("/", "_").replace(".", "_").replace(":", "_")
            if not is_unique_func_name:
                if self._name:
                    self._name = self._name + "_" + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        source_dir = self.get_source_dir(self._func, source_dir, source_dir_parent_level=source_dir_parent_level)

        workflow_type = WorkflowType.iWorkflow
        if local:
            self._func = None
            workflow_type = WorkflowType.iWorkflowLocal

        if context is not None:
            self._context = context
        else:
            self._context = WorkflowContext(name=self._name, service=service, workflow_type=workflow_type, source_dir=source_dir,
                                            distributed=distributed, init_env=init_env, max_walltime=max_walltime,
                                            exclude_source_files=exclude_source_files, clean_env=clean_env,
                                            enable_separate_log=enable_separate_log, container_options=container_options)

    @property
    def service(self):
        return self._context.service

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

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
    def workflow_type(self):
        return self._context.workflow_type

    @workflow_type.setter
    def workflow_type(self, value):
        self._context.workflow_type = value

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

    def get_work_tag(self):
        return self._context.workflow_type.name

    def get_work_type(self):
        return self._context.workflow_type

    def get_work_name(self):
        return self._name

    @property
    def multi_jobs_kwargs_list(self):
        return self._multi_jobs_kwargs_list

    @multi_jobs_kwargs_list.setter
    def multi_jobs_kwargs_list(self, value):
        raise Exception("Not allwed to update multi_jobs_kwargs_list")

    def to_dict(self):
        func = self._func
        self._func = None
        obj = super(Workflow, self).to_dict()
        self._func = func
        return obj

    def get_source_dir(self, func, source_dir, source_dir_parent_level=None):
        if source_dir:
            return source_dir
        if func:
            if inspect.isbuiltin(func):
                return None
            source_file = inspect.getsourcefile(func)
            if not source_file:
                return None
            file_path = os.path.abspath(source_file)
            source_dir = os.path.dirname(file_path)
            if source_dir_parent_level and source_dir_parent_level > 0:
                for _ in range(0, source_dir_parent_level):
                    source_dir = os.path.dirname(source_dir)
            return source_dir
        return None

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
            logging.info(f"Loaded context: {ret}")
            if 'multi_jobs_kwargs_list' in ret:
                self._multi_jobs_kwargs_list = ret['multi_jobs_kwargs_list']

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

        client = ClientManager(host=self._context.get_idds_server(), timeout=60)
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
        client = idds_api.get_api(dumper=idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)
        ret = client.submit(self, username=None, use_dataset_name=False)
        if ret[0] == 0 and ret[1][0]:
            request_id = ret[1][1]
        else:
            request_id = None
            logging.error("Failed to submit workflow to PanDA-iDDS with error: %s" % str(ret))

        logging.info("Submitted into PanDA-iDDS with request id=%s", str(request_id))
        return request_id

    def submit(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: the workflow id.
        :raise Exception when failing to submit the workflow.
        """
        try:
            request_id = None
            self.prepare()
            if self.service == 'panda':
                request_id = self.submit_to_panda_server()
            else:
                request_id = self.submit_to_idds_server()
        except Exception as ex:
            logging.error("Failed to submit workflow: %s" % str(ex))

        try:
            self._context.request_id = int(request_id)
            return request_id
        except Exception as ex:
            logging.error("Request id (%s) is not integer, there should be some submission errors: %s" % (request_id, str(ex)))

        return None

    def close_to_idds_server(self, request_id):
        """
        close the workflow to the iDDS server.

        :param request_id: the workflow id.
        :raise Exception when failing to close the workflow.
        """
        # iDDS ClientManager
        from idds.client.clientmanager import ClientManager

        client = ClientManager(host=self._context.get_idds_server(), timeout=60)
        ret = client.close(request_id)

        logging.info("Close request id=%s to iDDS server: %s", str(request_id), str(ret))
        return request_id

    def close_to_panda_server(self, request_id):
        """
        close the workflow to the iDDS server through PanDA service.

        :param request_id: the workflow id.
        :raise Exception when failing to closet the workflow.
        """
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self._context.get_idds_server()
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)
        ret = client.close(request_id)

        logging.info("Close request id=%s through PanDA-iDDS: %s", str(request_id), str(ret))
        return request_id

    def close(self):
        """
        close the workflow to the iDDS server.

        :raise Exception when failing to close the workflow.
        """
        if self._context.request_id is not None:
            try:
                if self.service == 'panda':
                    self.close_to_panda_server(self._context.request_id)
                else:
                    self.close_to_idds_server(self._context.request_id)
            except Exception as ex:
                logging.error("Failed to close request(%s): %s" % (self._context.request_id, str(ex)))

    def __del__(self):
        # self.close()
        pass

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

    def setup_source_files(self):
        """
        Setup location of source files
        """
        return self._context.setup_source_files()

    def load_func(self, func_name):
        """
        Load the function from the source files.

        :raise Exception
        """
        with modified_environ(IDDS_IGNORE_WORKFLOW_DECORATOR='true'):
            func = super(Workflow, self).load_func(func_name)

        return func

    def pre_run(self):
        # test AsyncResult
        a_ret = None
        try:
            workflow_context = self._context
            if workflow_context.distributed:
                logging.info("Test AsyncResult")
                a_ret = AsyncResult(workflow_context, wait_num=1, timeout=30)
                ret = a_ret.is_ok()
                a_ret.stop()
                logging.info(f"pre_run asyncresult test is_ok: {ret}")
                return ret
            return True
        except Exception as ex:
            logging.error(f"pre_run failed with error: {ex}")
            logging.error(traceback.format_exc())
        if a_ret:
            a_ret.stop()
        return False

    def run(self):
        logging.info("Start workflow run().")
        ret = None
        try:
            ret = self.run_local()
        except Exception as ex:
            logging.error(f"Failed to run function: {ex}")
            logging.error(traceback.format_exc())
        except:
            logging.error("Unknow error")
            logging.error(traceback.format_exc())
        logging.info(f"finish workflow run() with ret: {ret}.")
        return ret

    def run_local(self):
        """
        Run the workflow.
        """
        # with self:
        if True:
            is_ok = self.pre_run()
            if not is_ok:
                logging.error(f"pre_run is_ok: {is_ok}, will exit.")
                raise Exception("workflow pre_run failed")

            func_name, pre_kwargs, args, kwargs = self._func_name_and_args
            multi_jobs_kwargs_list = self.multi_jobs_kwargs_list
            if args:
                args = pickle.loads(zlib.decompress(base64.b64decode(args)))
            if pre_kwargs:
                pre_kwargs = pickle.loads(zlib.decompress(base64.b64decode(pre_kwargs)))
            if kwargs:
                kwargs = pickle.loads(zlib.decompress(base64.b64decode(kwargs)))
            if multi_jobs_kwargs_list:
                multi_jobs_kwargs_list = [pickle.loads(zlib.decompress(base64.b64decode(k))) for k in multi_jobs_kwargs_list]

            if self._func is None:
                func = self.load_func(func_name)
                self._func = func
            status, output, error = self.run_func(self._func, pre_kwargs, args, kwargs)
            if status:
                logging.info(f"run workflow successfully. output: {output}, error: {error}")
            else:
                logging.error(f"run workflow failed. output: {output}, error: {error}")
            return status

    # Context Manager -----------------------------------------------
    def __enter__(self):
        WorkflowCanvas.push_managed_workflow(self)
        return self

    def __exit__(self, _type, _value, _tb):
        w = WorkflowCanvas.pop_managed_workflow()
        if w is not None:
            w.close()

    # /Context Manager ----------------------------------------------

    def get_run_command(self):
        cmd = "run_workflow --type workflow --name %s " % self.name
        cmd += "--context %s --original_args %s " % (encode_base64(json_dumps(self._context)),
                                                     encode_base64(json_dumps(self._func_name_and_args)))
        cmd += "--current_job_kwargs ${IN/L}"
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
            cmd = cmd + "; ret=$?; " + clean_env + "; exit $ret"

        return cmd

    def get_func_name(self):
        func_name = self._func_name_and_args[0]
        return func_name


# foo = workflow(arg)(foo)
def workflow(func=None, *, local=False, service='idds', source_dir=None, primary=False, queue=None, site=None, cloud=None,
             max_walltime=24 * 3600, distributed=True, init_env=None, pre_kwargs={}, return_workflow=False, no_wraps=False,
             source_dir_parent_level=None, exclude_source_files=[], enable_separate_log=False, clean_env=None,
             core_count=1, total_memory=4000,    # MB
             container_options=None):
    if func is None:
        return functools.partial(workflow, local=local, service=service, source_dir=source_dir, primary=primary, queue=queue, site=site, cloud=cloud,
                                 max_walltime=max_walltime, distributed=distributed, init_env=init_env, pre_kwargs=pre_kwargs, no_wraps=no_wraps,
                                 return_workflow=return_workflow, source_dir_parent_level=source_dir_parent_level,
                                 exclude_source_files=exclude_source_files, clean_env=clean_env, enable_separate_log=enable_separate_log,
                                 core_count=core_count, total_memory=total_memory,
                                 container_options=container_options)

    if 'IDDS_IGNORE_WORKFLOW_DECORATOR' in os.environ:
        return func

    # @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            f = Workflow(func, service=service, source_dir=source_dir, local=local, max_walltime=max_walltime, distributed=distributed,
                         pre_kwargs=pre_kwargs, args=args, kwargs=kwargs, init_env=init_env, source_dir_parent_level=source_dir_parent_level,
                         exclude_source_files=exclude_source_files, clean_env=clean_env, enable_separate_log=enable_separate_log,
                         container_options=container_options)

            f.queue = queue
            f.site = site
            f.cloud = cloud
            f.core_count = core_count
            f.total_memory = total_memory

            logging.info("return_workflow %s" % return_workflow)
            if return_workflow:
                return f

            logging.info("Prepare workflow")
            f.prepare()
            logging.info("Prepared workflow")

            logging.info("Registering workflow")
            f.submit()

            if not local:
                logging.info("Run workflow at remote sites")
                return f
            else:
                logging.info("Run workflow locally")
                with f:
                    ret = f.run()
                return ret
        except Exception as ex:
            logging.error("Failed to run workflow %s: %s" % (func, ex))
            raise ex
        except:
            raise
    if no_wraps:
        return wrapper
    else:
        return functools.wraps(func)(wrapper)


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
