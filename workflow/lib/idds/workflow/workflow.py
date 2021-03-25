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
import datetime
import logging
import inspect
import random
import time
import uuid

from idds.common.utils import json_dumps, setup_logging, get_proxy
from .base import Base


setup_logging(__name__)


class Condition(Base):
    def __init__(self, cond=None, current_work=None, true_work=None, false_work=None, logger=None):
        """
        Condition.
        if cond() is true, return true_work, else return false_work.

        :param cond: executable return true or false.
        :param work: The current Work instance.
        :param true_work: Work instance.
        :param false_work: Work instance.
        """
        if cond and callable(cond):
            assert(inspect.ismethod(cond))
            assert(cond.__self__ == current_work)
        self.cond = cond
        self.current_work = None
        if current_work:
            self.current_work = current_work.get_template_id()
        self.true_work = None
        if true_work:
            self.true_work = true_work.get_template_id()
        self.false_work = None
        if false_work:
            self.false_work = false_work.get_template_id()

    def all_works(self):
        works = []
        works.append(self.current_work)
        if self.true_work:
            works.append(self.true_work)
        if self.false_work:
            works.append(self.false_work)
        return works

    def all_next_works(self):
        works = []
        if self.true_work:
            works.append(self.true_work)
        if self.false_work:
            works.append(self.false_work)
        return works

    def get_cond_status(self):
        if callable(self.cond):
            if self.cond():
                return True
            else:
                return False
        else:
            if self.cond:
                return True
            else:
                return False

    def get_next_work(self):
        if self.get_cond_status():
            return self.true_work
        else:
            return self.false_work


class Workflow(Base):

    def __init__(self, name=None, workload_id=None, logger=None):
        """
        Init a workflow.
        """
        self.internal_id = str(uuid.uuid1())
        self.template_work_id = self.internal_id

        if name:
            self.name = name + "." + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))
        else:
            self.name = 'idds.workflow.' + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))

        self.workload_id = workload_id
        if self.workload_id is None:
            self.workload_id = int(time.time())

        self.logger = logger
        if self.logger is None:
            self.setup_logger()

        self.works_template = {}
        self.works = {}
        self.work_sequence = {}  # order list

        self.terminated_works = []
        self.initial_works = []
        # if the primary initial_work is not set, it's the first initial work.
        self.primary_initial_work = None
        self.independent_works = []

        self.first_initial = False
        self.new_to_run_works = []
        self.current_running_works = []
        self.work_conds = {}

        self.num_subfinished_works = 0
        self.num_finished_works = 0
        self.num_failed_works = 0
        self.num_cancelled_works = 0
        self.num_suspended_works = 0
        self.num_expired_works = 0
        self.num_total_works = 0

        self.last_work = None

        # user defined Condition class
        self.user_defined_conditions = {}

        self.proxy = None

    def get_name(self):
        return self.name

    def get_class_name(self):
        return self.__class__.__name__

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())

    def log_info(self, info):
        if self.logger is None:
            self.setup_logger()
        self.logger.info(info)

    def log_debug(self, info):
        if self.logger is None:
            self.setup_logger()
        self.logger.debug(info)

    def get_internal_id(self):
        return self.internal_id

    def copy(self):
        new_wf = copy.deepcopy(self)
        return new_wf

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

    def get_works_template(self):
        return self.works_template

    def add_work_template(self, work):
        self.works_template[work.get_template_id()] = work

    def get_new_work_from_template(self, work_id, new_parameters=None):
        # template_id = work.get_template_id()
        template_id = work_id
        work = self.works_template[template_id]
        new_work = work.generate_work_from_template()
        if new_parameters:
            new_work.set_parameters(new_parameters)
        new_work.set_sequence_id(self.num_total_works)
        new_work.initialize_work()
        self.works[new_work.get_internal_id()] = new_work
        # self.work_sequence.append(new_work.get_internal_id())
        self.work_sequence[str(self.num_total_works)] = new_work.get_internal_id()
        self.num_total_works += 1
        self.new_to_run_works.append(new_work.get_internal_id())
        self.last_work = new_work.get_internal_id()
        return new_work

    def register_user_defined_condition(self, condition):
        cond_src = inspect.getsource(condition)
        self.user_defined_conditions[condition.__name__] = cond_src

    def load_user_defined_condition(self):
        # try:
        #     Condition()
        # except NameError:
        #     global Condition
        #     import Condition

        for cond_src_name in self.user_defined_conditions:
            # global cond_src_name
            exec(self.user_defined_conditions[cond_src_name])

    def set_workload_id(self, workload_id):
        self.workload_id = workload_id

    def get_workload_id(self):
        return self.workload_id

    def add_work(self, work, initial=False, primary=False):
        self.first_initial = False
        self.add_work_template(work)
        if initial:
            if primary:
                self.primary_initial_work = work.get_template_id()
            self.add_initial_works(work.get_template_id())

        self.independent_works.append(work.get_template_id())

    def add_condition(self, cond):
        self.first_initial = False
        cond_works = cond.all_works()
        for cond_work in cond_works:
            assert(cond_work in self.get_works_template())

        if cond.current_work not in self.work_conds:
            self.work_conds[cond.current_work] = []
        self.work_conds[cond.current_work].append(cond)

        # if a work is a true_work or false_work of a condition,
        # should remove it from independent_works
        cond_next_works = cond.all_next_works()
        for next_work in cond_next_works:
            if next_work in self.independent_works:
                self.independent_works.remove(next_work)

    def add_initial_works(self, work):
        assert(work.get_template_id() in self.get_works_template())
        self.initial_works.append(work.get_template_id())
        if self.primary_initial_work is None:
            self.primary_initial_work = work.get_template_id()

    def enable_next_work(self, work, cond):
        self.log_debug("Checking Work %s condition: %s" % (work.get_internal_id(),
                                                           json_dumps(cond, sort_keys=True, indent=4)))
        if cond and self.is_class_method(cond.cond):
            # cond_work_id = self.works[cond.cond['idds_method_class_id']]
            cond.cond = getattr(work, cond.cond['idds_method'])
        self.log_info("Work %s condition: %s" % (work.get_internal_id(), cond.cond))
        next_work = cond.get_next_work()
        self.log_info("Work %s condition status %s" % (work.get_internal_id(), cond.get_cond_status()))
        self.log_info("Work %s next work %s" % (work.get_internal_id(), next_work))
        if next_work is not None:
            new_parameters = work.get_parameters_for_next_task()
            next_work = self.get_new_work_from_template(next_work, new_parameters)
            work.add_next_work(next_work.get_internal_id())
            return next_work

    def __str__(self):
        return str(json_dumps(self))

    def get_new_works(self):
        """
        *** Function called by Marshaller agent.

        new works to be ready to start
        """
        self.sync_works()
        return [self.works[k] for k in self.new_to_run_works]

    def get_current_works(self):
        """
        *** Function called by Marshaller agent.

        Current running works
        """
        self.sync_works()
        return [self.works[k] for k in self.current_running_works]

    def get_primary_initial_collection(self):
        """
        *** Function called by Clerk agent.
        """

        if self.primary_initial_work:
            return self.get_works_template()[self.primary_initial_work].get_primary_input_collection()
        elif self.initial_works:
            return self.get_works_template()[self.initial_works[0]].get_primary_input_collection()
        elif self.independent_works:
            return self.get_works_template()[self.independent_works[0]].get_primary_input_collection()
        else:
            keys = self.get_works_template().keys()
            return self.get_works_template()[keys[0]].get_primary_input_collection()
        return None

    def first_initialize(self):
        # set new_to_run works
        if not self.first_initial:
            self.first_initial = True
            if self.initial_works:
                tostart_works = self.initial_works
            elif self.independent_works:
                tostart_works = self.independent_works
            else:
                tostart_works = list(self.get_works_template().keys())
                tostart_works = [tostart_works[0]]

            for work_id in tostart_works:
                self.get_new_work_from_template(work_id)

    def sync_works(self):
        self.first_initialize()

        for work in [self.works[k] for k in self.new_to_run_works]:
            if work.transforming:
                self.new_to_run_works.remove(work.get_internal_id())
                self.current_running_works.append(work.get_internal_id())

        for work in [self.works[k] for k in self.current_running_works]:
            if work.is_terminated():
                self.log_info("Work %s is terminated" % work.get_internal_id())
                self.log_debug("Work conditions: %s" % json_dumps(self.work_conds, sort_keys=True, indent=4))
                if work.get_template_id() not in self.work_conds:
                    # has no next work
                    self.log_info("Work %s has no condition dependencies" % work.get_internal_id())
                    self.terminated_works.append(work.get_internal_id())
                    self.current_running_works.remove(work.get_internal_id())
                else:
                    self.log_debug("Work %s has condition dependencies %s" % (work.get_internal_id(),
                                                                              json_dumps(self.work_conds[work.get_template_id()], sort_keys=True, indent=4)))
                    for cond in self.work_conds[work.get_template_id()]:
                        self.enable_next_work(work, cond)
                    self.terminated_works.append(work.get_internal_id())
                    self.current_running_works.remove(work.get_internal_id())

                if work.is_finished():
                    self.num_finished_works += 1
                elif work.is_subfinished():
                    self.num_subfinished_works += 1
                elif work.is_failed():
                    self.num_failed_works += 1
                elif work.is_expired():
                    self.num_expired_works += 1
                elif work.is_cancelled():
                    self.num_cancelled_works += 1
                elif work.is_suspended():
                    self.num_suspended_works += 1

    def get_exact_workflows(self):
        """
        *** Function called by Clerk agent.

        TODO: The primary dataset for the initial work is a dataset with '*'.
        workflow.primary_initial_collection = 'some datasets with *'
        collections = get_collection(workflow.primary_initial_collection)
        wfs = []
        for coll in collections:
            wf = self.copy()
            wf.name = self.name + "_" + number
            wf.primary_initial_collection = coll
            wfs.append(wf)
        return wfs
        """
        return [self]

    def is_terminated(self):
        """
        *** Function called by Marshaller agent.
        """
        self.sync_works()
        if len(self.new_to_run_works) == 0 and len(self.current_running_works) == 0:
            return True
        return False

    def is_finished(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and self.num_finished_works == self.num_total_works

    def is_subfinished(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_finished_works > 0 and self.num_finished_works < self.num_total_works)

    def is_failed(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_failed_works > 0) and (self.num_cancelled_works == 0) and (self.num_suspended_works == 0) and (self.num_expired_works == 0)

    def is_expired(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_expired_works > 0)

    def is_cancelled(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_cancelled_works > 0)

    def is_suspended(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_suspended_works > 0)

    def get_terminated_msg(self):
        """
        *** Function called by Marshaller agent.
        """
        if self.last_work:
            return self.works[self.last_work].get_terminated_msg()
        return None

    def add_proxy(self):
        self.proxy = get_proxy()
        if not self.proxy:
            raise Exception("Cannot get local proxy")

    def get_proxy(self):
        return self.proxy
