#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


class Condition(object):
    def __init__(self, cond, prework, true_work, false_work=None):
        """
        Condition.
        if cond() is true, return true_work, else return false_work.

        :param cond: executable return true or false.
        :param prework: Work instance.
        :param true_work: Work instance.
        :param false_work: Work instance.
        """
        self.cond = cond
        self.prework = prework
        self.true_work = true_work
        self.false_work = false_work

    def all_works(self):
        works = []
        works.append(self.prework)
        works.append(self.true_work)
        if self.false_work:
            works.append(self.false_work)
        return works

    def all_next_works(self):
        works = []
        works.append(self.true_work)
        if self.false_work:
            works.append(self.false_work)
        return works

    def get_next_work(self):
        if self.cond():
            return self.true_work
        else:
            return self.false_work


class Workflow(object):

    def __init__(self):
        """
        Init a workflow.
        """
        self.works = []
        self.terminated_works = []
        self.initial_works = []
        self.current_works = []
        self.work_conds = {}

    def add_work(self, work, initial=False):
        self.works.append(work)
        if initial:
            self.initial_works.append(work)
            self.current_works.append(work)

    def add_condition(self, cond):
        cond_works = cond.all_works()
        for cond_work in cond_works:
            assert(cond_work in self.works)
        # for next_work in cond.all_next_works():
        #     if next_work in self.current_works:
        #         del self.current_works[next_work]

        if cond.prework not in self.work_conds:
            self.work_conds[cond.prework] = []
        self.work_conds[cond.prework].append(cond)

    def get_current_works(self):
        return self.current_works

    def update_work_status(self, work, status):
        assert(work in self.current_works)
        work.status = status
        if work.is_terminated():
            if work not in self.work_conds:
                # has no next work
                self.terminated_works.append(work.copy())
                del self.current_works[work]
            else:
                backup_work = work.copy()
                for cond in self.work_conds[work]:
                    next_work = cond.get_next_work()
                    if next_work is not None:
                        next_work.initialize_work()
                        self.current_works.append(next_work)
                        backup_work.add_next_work(next_work)
                self.terminated_works.append(backup_work)
                del self.current_works[work]
