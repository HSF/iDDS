#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2024

import datetime
import functools
import logging
import os
import time
import traceback

from idds.common import exceptions
from idds.common.constants import WorkflowType, TransformStatus
from idds.common.imports import get_func_name
from idds.common.utils import setup_logging, json_dumps, encode_base64
from .asyncresult import AsyncResult, MapResult
from .base import Base, Context
from .workflow import WorkflowCanvas

setup_logging(__name__)


class WorkContext(Context):

    def __init__(self, name=None, workflow_context=None, source_dir=None):
        super(WorkContext, self).__init__()
        self._workflow_context = workflow_context
        self._transform_id = None
        self._processing_id = None
        self._type = WorkflowType.iWork

        self._name = name
        self._site = None
        self._queue = None

        self._priority = 500
        self._core_count = 1
        self._total_memory = 1000        # MB
        self._max_walltime = 7 * 24 * 3600
        self._max_attempt = 5

        self._map_results = False

    def get_service(self):
        return self._workflow_context.service

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

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
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

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

    def get_idds_server(self):
        return self._workflow_context.get_idds_server()

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
        return self._workflow_context.setup()


class Work(Base):

    def __init__(self, func=None, workflow_context=None, context=None, args=None, kwargs=None, group_kwargs=None,
                 update_kwargs=None, map_results=False, source_dir=None, is_unique_func_name=False):
        """
        Init a workflow.
        """
        super(Work, self).__init__()
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

        if context:
            self._context = context
        else:
            self._context = WorkContext(name=self._name, workflow_context=workflow_context)

        self.map_results = map_results
        self._results = None

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

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
    def type(self):
        return self._context.type

    @type.setter
    def type(self, value):
        self._context.type = value

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
    def token(self):
        return self._context.token

    @token.setter
    def token(self, value):
        self._context.token = value

    @property
    def group_parameters(self):
        return self._func_name_and_args[3]

    @group_parameters.setter
    def group_parameters(self, value):
        raise Exception("Not allwed to update group parameters")

    def get_work_tag(self):
        return 'iWork'

    def get_work_type(self):
        return WorkflowType.iWork

    def get_work_name(self):
        return self._name

    def to_dict(self):
        func = self._func
        self._func = None
        obj = super(Work, self).to_dict()
        self._func = func
        return obj

    def submit_to_idds_server(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: The workflow id.
        :raise Exception when failing to submit the workflow.
        """
        # iDDS ClientManager
        from idds.client.clientmanager import ClientManager
        client = ClientManager(host=self._context.get_idds_server())
        request_id = self._context.request_id
        transform_id = client.submit_work(request_id, self, use_dataset_name=False)
        logging.info("Submitted into iDDS with transform id=%s", str(transform_id))
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
                                  manager=True)
        transform_id = client.submit_work(request_id, self, use_dataset_name=False)
        logging.info("Submitted work into PanDA-iDDS with transform id=%s", str(transform_id))
        return transform_id

    def submit(self):
        """
        Submit the workflow to the iDDS server.

        :returns id: The workflow id.
        :raise Exception when failing to submit the workflow.
        """
        if self._context.get_service() == 'panda':
            tf_id = self.submit_to_panda_server()
        else:
            tf_id = self.submit_to_idds_server()

        try:
            self._context.transform_id = int(tf_id)
            return tf_id
        except Exception as ex:
            logging.info("Transform id (%s) is not integer, there should be some submission errors: %s" % (tf_id, str(ex)))

        return None

    def get_status_from_panda_server(self):
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self._context.get_idds_server()
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  manager=True)

        request_id = self._context.request_id
        transform_id = self._context.transform_id
        if not transform_id:
            log_msg = "No transform id defined (request_id: %s, transform_id: %s)", (request_id, transform_id)
            logging.error(log_msg)
            return exceptions.IDDSException(log_msg)

        tf = client.get_transform(request_id=request_id, transform_id=transform_id)
        if not tf:
            logging.info("Get transform (request_id: %s, transform_id: %s) from iDDS: %s" % (request_id, transform_id, tf))
            return None

        logging.info("Get transform status (request_id: %s, transform_id: %s) from iDDS: %s" % (request_id, transform_id, tf['status']))

        return tf['status']

    def get_status_from_idds_server(self):
        from idds.client.clientmanager import ClientManager
        client = ClientManager(host=self._context.get_idds_server())

        request_id = self._context.request_id
        transform_id = self._context.transform_id
        if not transform_id:
            log_msg = "No transform id defined (request_id: %s, transform_id: %s)" % (request_id, transform_id)
            logging.error(log_msg)
            return exceptions.IDDSException(log_msg)

        tf = client.get_transform(request_id=request_id, transform_id=transform_id)
        if not tf:
            logging.info("Get transform (request_id: %s, transform_id: %s) from iDDS: %s" % (request_id, transform_id, tf))
            return None

        logging.info("Get transform status (request_id: %s, transform_id: %s) from iDDS: %s" % (request_id, transform_id, tf['status']))

        return tf['status']

    def get_status(self):
        try:
            if self._context.get_service() == 'panda':
                return self.get_status_from_panda_server()
            return self.get_status_from_idds_server()
        except Exception as ex:
            logging.info("Failed to get transform status: %s" % str(ex))

    def get_terminated_status(self):
        return [None, TransformStatus.Finished, TransformStatus.SubFinished,
                TransformStatus.Failed, TransformStatus.Cancelled,
                TransformStatus.Suspended, TransformStatus.Expired]

    def get_func_name(self):
        func_name = self._func_name_and_args[0]
        return func_name

    def get_group_kwargs(self):
        group_kwargs = self._func_name_and_args[3]
        return group_kwargs

    def wait_results(self):
        try:
            terminated_status = self.get_terminated_status()

            group_kwargs = self.get_group_kwargs()
            if group_kwargs:
                async_ret = AsyncResult(self._context, name=self.get_func_name(), group_kwargs=group_kwargs,
                                        map_results=self.map_results, internal_id=self.internal_id)
            else:
                async_ret = AsyncResult(self._context, name=self.get_func_name(), wait_num=1, internal_id=self.internal_id)

            async_ret.subscribe()

            status = self.get_status()
            time_last_check_status = time.time()
            logging.info("waiting for results")
            while status not in terminated_status:
                # time.sleep(10)
                ret = async_ret.wait_results(timeout=10)
                if ret:
                    logging.info("Recevied result: %s" % ret)
                    break
                if time.time() - time_last_check_status > 600:   # 10 minutes
                    status = self.get_status()
                    time_last_check_status = time.time()

            async_ret.stop()
            self._results = async_ret.wait_results(force_return_results=True)
            return self._results
        except Exception as ex:
            logging.error("wait_results got some errors: %s" % str(ex))
            async_ret.stop()
            return ex

    def get_results(self):
        return self._results

    def setup(self):
        """
        :returns command: `str` to setup the workflow.
        """
        return self._context.setup()

    def load(self, func_name):
        """
        Load the function from the source files.

        :raise Exception
        """
        os.environ['IDDS_IWORKFLOW_LOAD_WORK'] = 'true'
        func = super(Work, self).load(func_name)
        del os.environ['IDDS_IWORKFLOW_LOAD_WORK']

        return func

    def pre_run(self):
        # test AsyncResult
        workflow_context = self._context
        if workflow_context.distributed:
            logging.info("Test AsyncResult")
            a_ret = AsyncResult(workflow_context, wait_num=1, timeout=30)
            a_ret.subscribe()

            async_ret = AsyncResult(workflow_context, internal_id=a_ret.internal_id)
            test_result = "AsyncResult test (request_id: %s, transform_id: %s)" % (workflow_context.request_id, workflow_context.transform_id)
            logging.info("AsyncResult publish: %s" % test_result)
            async_ret.publish(test_result)

            ret_q = a_ret.wait_result(force_return_results=True)
            logging.info("AsyncResult results: %s" % str(ret_q))
            if ret_q and ret_q == test_result:
                logging.info("AsyncResult test succeeded")
                return True
            else:
                logging.info("AsyncResult test failed (published: %s, received: %s)" % (test_result, ret_q))
                return False
        return True

    def run(self):
        """
        Run the work.
        """
        self.pre_run()

        func_name, args, kwargs, group_kwargs = self._func_name_and_args
        if self._func is None:
            func = self.load(func_name)
            self._func = func

        if self._context.distributed:
            rets = None
            kwargs_copy = kwargs.copy()
            if self._update_kwargs and type(self._update_kwargs) in [dict]:
                kwargs_copy.update(self._update_kwargs)

            rets = self.run_func(self._func, args, kwargs_copy)

            request_id = self._context.request_id
            transform_id = self._context.transform_id
            logging.info("publishing AsyncResult to (request_id: %s, transform_id: %s): %s" % (request_id, transform_id, rets))
            async_ret = AsyncResult(self._context, name=self.get_func_name(), internal_id=self.internal_id, run_group_kwarg=self._update_kwargs)
            async_ret.publish(rets)

            if not self.map_results:
                self._results = rets
            else:
                self._results = MapResult()
                self._results.add_result(name=self.get_func_name(), args=self._update_kwargs, result=rets)
            return self._results
        else:
            if not group_kwargs:
                rets = self.run_func(self._func, args, kwargs)
                if not self.map_results:
                    self._results = rets
                else:
                    self._results = MapResult()
                    self._results.add_result(name=self.get_func_name(), args=self._update_kwargs, result=rets)
                return self._results
            else:
                if not self.map_results:
                    self._results = []
                    for group_kwarg in group_kwargs:
                        kwargs_copy = kwargs.copy()
                        kwargs_copy.update(group_kwarg)
                        rets = self.run_func(self._func, args, kwargs_copy)
                        self._results.append(rets)
                else:
                    self._results = MapResult()
                    for group_kwarg in group_kwargs:
                        kwargs_copy = kwargs.copy()
                        kwargs_copy.update(group_kwarg)
                        rets = self.run_func(self._func, args, kwargs_copy)
                        self._results.add_result(name=self.get_func_name(), args=group_kwarg, result=rets)
                return self._results

    def get_run_command(self):
        cmd = "run_workflow --type work "
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


def run_work_distributed(w):
    try:
        tf_id = w.submit()
        if tf_id:
            logging.info("wait for results")
            rets = w.wait_results()
            logging.info("Got results: %s" % rets)
            return rets
        else:
            logging.error("Failed to distribute work: %s" % w.name)
        return None
    except Exception as ex:
        logging.error("Failed to run the work distributedly: %s" % ex)
        logging.error(traceback.format_exc())
        return None


# foo = work(arg)(foo)
def work(func=None, *, map_results=False, lazy=False):
    if func is None:
        return functools.partial(work, map_results=map_results, lazy=lazy)

    if 'IDDS_IWORKFLOW_LOAD_WORK' in os.environ:
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            f = kwargs.pop('workflow', None) or WorkflowCanvas.get_current_workflow()
            workflow_context = f._context
            group_kwargs = kwargs.pop('group_kwargs', [])
            logging.debug("workflow context: %s" % workflow_context)

            logging.debug("work decorator: func: %s, map_results: %s" % (func, map_results))
            if workflow_context:
                logging.debug("setup work")
                w = Work(workflow_context=workflow_context, func=func, args=args, kwargs=kwargs, group_kwargs=group_kwargs, map_results=map_results)
                # if distributed:
                if workflow_context.distributed:
                    ret = run_work_distributed(w)
                    return ret

                return w.run()
            else:
                logging.info("workflow context is not defined, run function locally")
                if not group_kwargs:
                    return func(*args, **kwargs)

                if not kwargs:
                    kwargs = {}
                if not map_results:
                    rets = []
                    for group_kwarg in group_kwargs:
                        kwargs_copy = kwargs.copy()
                        kwargs_copy.update(group_kwarg)
                        ret = func(*args, **kwargs_copy)
                        rets.append(ret)
                    return rets
                else:
                    rets = MapResult()
                    for group_kwarg in group_kwargs:
                        kwargs_copy = kwargs.copy()
                        kwargs_copy.update(group_kwarg)
                        ret = func(*args, **kwargs_copy)
                        rets.add_result(name=get_func_name(func), args=group_kwarg, result=ret)
                    return rets
        except Exception as ex:
            logging.error("Failed to run workflow %s: %s" % (func, ex))
            raise ex
        except:
            raise
    return wrapper
