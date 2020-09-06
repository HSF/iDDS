#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import datetime
import inspect
import random
import time

from idds.common.utils import json_dumps
from .base import Base


class Condition(object):
    def __init__(self, cond, current_work, true_work, false_work=None):
        """
        Condition.
        if cond() is true, return true_work, else return false_work.

        :param cond: executable return true or false.
        :param work: The current Work instance.
        :param true_work: Work instance.
        :param false_work: Work instance.
        """
        self.cond = cond
        self.current_work = None
        if current_work:
            self.current_work = current_work.get_internal_id()
        self.true_work = None
        if true_work:
            self.true_work = true_work.get_internal_id()
        self.false_work = None
        if false_work:
            self.false_work = false_work.get_internal_id()

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

    def get_next_work(self):
        if callable(self.cond):
            if self.cond():
                return self.true_work
            else:
                return self.false_work
        else:
            if self.cond:
                return self.true_work
            else:
                return self.false_work


class Workflow(Base):

    def __init__(self, name=None, workload_id=None):
        """
        Init a workflow.
        """
        if name:
            self.name = name + "." + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))
        else:
            self.name = 'idds.workflow.' + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))

        self.workload_id = workload_id
        if self.workload_id is None:
            self.workload_id = time.time()

        self.works = {}
        self.terminated_works = []
        self.initial_works = []
        # if the primary initial_work is not set, it's the first initial work.
        self.primary_initial_work = None
        self.new_works = []
        self.current_works = []
        self.work_conds = {}

        self.num_finished_works = 0
        self.num_total_works = 0

        self.last_work = None

        # user defined Condition class
        self.user_defined_conditions = {}

    def get_name(self):
        return self.name

    def register_user_defined_condition(self, condition):
        cond_src = inspect.getsource(condition)
        self.user_defined_conditions[condition.__name__] = cond_src

    def load_user_defined_condition(self):
        try:
            Condition()
        except NameError:
            global Condition
            import Condition

        for cond_src_name in self.user_defined_conditions:
            global cond_src_name
            exec(self.user_defined_conditions[cond_src_name])

    def set_workload_id(self, workload_id):
        self.workload_id = workload_id

    def get_workload_id(self):
        return self.workload_id

    def add_work(self, work, initial=False, primary=False):
        self.num_total_works += 1
        self.works[work.get_internal_id()] = work
        if initial:
            if primary:
                self.primary_initial_work = work.get_internal_id()
            self.initial_works.append(work.get_internal_id())

        if self.primary_initial_work is None:
            self.primary_initial_work = work.get_internal_id()
        self.new_works.append(work.get_internal_id())

    def enable_work(self, work):
        assert(work.get_internal_id() in self.works)
        if work.get_internal_id() not in self.new_works:
            self.new_works.append(work.get_internal_id())

    def add_condition(self, cond):
        cond_works = cond.all_works()
        for cond_work in cond_works:
            assert(cond_work in self.works)
        # for next_work in cond.all_next_works():
        #     if next_work in self.current_works:
        #         del self.current_works[next_work]

        if cond.current_work not in self.work_conds:
            self.work_conds[cond.current_work] = []
        self.work_conds[cond.current_work].append(cond)

        # if a work is a true_work or false_work of a condition,
        # should remove it from new_works
        cond_next_works = cond.all_next_works()
        for next_work in cond_next_works:
            if next_work in self.new_works:
                self.new_works.remove(next_work)
        if self.primary_initial_work not in self.new_works:
            self.primary_initial_work = self.new_works[0]

    def __eq__(self, wf):
        if self.name == wf.name:
            return True
        else:
            return False

    def __str__(self):
        return str(json_dumps(self))

    def get_new_works(self):
        """
        *** Function called by Marshaller agent.

        new works to be ready to start
        """
        return [self.works[k] for k in self.new_works]

    def get_current_works(self):
        """
        *** Function called by Marshaller agent.

        Current running works
        """
        self.sync_works()
        return [self.works[k] for k in self.current_works]

    def get_primary_initial_collection(self):
        """
        *** Function called by Clerk agent.
        """

        if self.primary_initial_work:
            return self.works[self.primary_initial_work].get_primary_input_collection()
        return None

    def update_work_status(self, work, status):
        assert(work in self.current_works)
        work.set_status(status)
        if work.is_terminated():
            if work.is_finished():
                self.num_finished_works += 1
            else:
                # self.num_finished_works
                pass
            if work not in self.work_conds:
                # has no next work
                work_cp = work.copy()
                self.last_work = work_cp
                self.terminated_works.append(work_cp)
                self.current_works.remove(work)
            else:
                backup_work = work.copy()
                for cond in self.work_conds[work]:
                    next_work = cond.get_next_work()
                    if next_work is not None:
                        next_work.initialize_work()
                        self.current_works.append(next_work)
                        backup_work.add_next_work(next_work)
                self.last_work = backup_work
                self.terminated_works.append(backup_work)
                self.current_works.remove(work)

    def work_status_update_trigger(self, work, status):
        assert(work in self.new_works + self.current_works)
        if work in self.new_works and work.is_running():
            self.new_works.remove(work)
            self.current_works.append(work)

        if work.is_terminated():
            if work not in self.work_conds:
                # has no next work
                self.terminated_works.append(work.copy())
                self.current_works.remove(work)
            else:
                backup_work = work.copy()
                for cond in self.work_conds[work]:
                    next_work = cond.get_next_work()
                    if next_work is not None:
                        next_work.initialize_work()
                        # self.current_works.append(next_work)
                        self.new_works.append(next_work)
                        backup_work.add_next_work(next_work)
                self.terminated_works.append(backup_work)
                self.current_works.remove(work)

    def sync_works(self):
        for work in [self.works[k] for k in self.new_works]:
            if work.transforming:
                self.new_works.remove(work.get_internal_id())
                self.current_works.append(work.get_internal_id())

        for work in [self.works[k] for k in self.current_works]:
            if work.is_terminated():
                if work.get_internal_id() not in self.work_conds:
                    # has no next work
                    self.terminated_works.append(work.get_internal_id())
                    self.current_works.remove(work.get_internal_id())
                else:
                    for cond in self.work_conds[work.get_internal_id()]:
                        next_work = cond.get_next_work()
                        if next_work is not None:
                            next_work.initialize_work()
                            # self.current_works.append(next_work)
                            # self.new_works.append(next_work.get_internal_id())
                            self.enable_work(next_work)
                            work.add_next_work(next_work.get_internal_id())
                    self.terminated_works.append(work.get_internal_id())
                    self.current_works.remove(work.get_internal_id())
                if work.is_finished():
                    self.num_finished_works += 1

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
        if len(self.new_works) == 0 and len(self.current_works) == 0:
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
        return self.is_terminated() and (self.num_finished_works < self.num_total_works) and (self.num_finished_works > 0)

    def is_failed(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_finished_works == 0)

    def get_terminated_msg(self):
        """
        *** Function called by Marshaller agent.
        """
        if self.last_work:
            return self.last_work.get_terminated_msg()
        return None
