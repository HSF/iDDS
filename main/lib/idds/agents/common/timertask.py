#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


import time
import traceback


class TimerTask(object):
    """
    The base class for Task which will be executed after some time
    """

    def __init__(self, task_func, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1, logger=None):
        self.to_execute_time = time.time()
        self.delay_time = delay_time
        self.priority = priority
        self.task_func = task_func
        self.task_output_queue = task_output_queue
        self.task_args = task_args
        self.task_kwargs = task_kwargs

        self.logger = logger

    def __eq__(one, two):
        return (one.to_execute_time, one.priority) == (two.to_execute_time, two.priority)

    def __lt__(one, two):
        return (one.to_execute_time, one.priority) < (two.to_execute_time, two.priority)

    def __le__(one, two):
        return (one.to_execute_time, one.priority) <= (two.to_execute_time, two.priority)

    def __gt__(one, two):
        return (one.to_execute_time, one.priority) > (two.to_execute_time, two.priority)

    def __ge__(one, two):
        return (one.to_execute_time, one.priority) >= (two.to_execute_time, two.priority)

    def is_ready(self):
        if self.to_execute_time <= time.time():
            return True
        return False

    def execute(self):
        try:
            # set it to avoid an exception
            self.to_execute_time = time.time() + self.delay_time

            ret = self.task_func(*self.task_args, **self.task_kwargs)
            if self.task_output_queue and ret is not None:
                for ret_item in ret:
                    self.task_output_queue.put(ret_item)

            # if there is no exception, this one is the correct one.
            self.to_execute_time = time.time() + self.delay_time
        except:
            if self.logger:
                self.logger.error('Failed to execute task func: %s, %s' % (self.task_func, traceback.format_exc()))
            else:
                print('Failed to execute task func: %s, %s' % (self.task_func, traceback.format_exc()))
