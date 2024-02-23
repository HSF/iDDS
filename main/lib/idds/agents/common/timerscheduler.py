#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2023


import heapq
import threading
import traceback
from concurrent import futures

from .timertask import TimerTask


class IDDSThreadPoolExecutor(futures.ThreadPoolExecutor):
    def __init__(self, max_workers=None, thread_name_prefix='',
                 initializer=None, initargs=()):
        self.futures = []
        self._lock = threading.RLock()
        super(IDDSThreadPoolExecutor, self).__init__(max_workers=max_workers,
                                                     thread_name_prefix=thread_name_prefix,
                                                     initializer=initializer,
                                                     initargs=initargs)

    def submit(self, fn, *args, **kwargs):
        future = super(IDDSThreadPoolExecutor, self).submit(fn, *args, **kwargs)
        with self._lock:
            self.futures.append(future)
        return future

    def get_max_workers(self):
        return self._max_workers

    def get_num_workers(self):
        with self._lock:
            for future in self.futures.copy():
                if future.done():
                    self.futures.remove(future)
            return len(self.futures)

    def has_free_workers(self):
        return self.get_num_workers() < self._max_workers

    def get_num_free_workers(self):
        return self._max_workers - self.get_num_workers()


class TimerScheduler(threading.Thread):
    """
    The base class to schedule Task which will be executed after some time
    """

    def __init__(self, num_threads, name=None, logger=None):
        super(TimerScheduler, self).__init__(name=name)
        self.num_threads = int(num_threads)
        if self.num_threads < 1:
            self.num_threads = 1
        self.graceful_stop = threading.Event()
        self.executor_name = name
        self.executors = IDDSThreadPoolExecutor(max_workers=self.num_threads,
                                                thread_name_prefix=name)

        self.executors_timer = IDDSThreadPoolExecutor(max_workers=1,
                                                      thread_name_prefix=name + "_Timer")
        self.timer_thread = None

        self._task_queue = []
        self._lock = threading.RLock()

        self.logger = logger

    def set_logger(self, logger):
        self.logger = logger

    def stop(self, signum=None, frame=None):
        self.graceful_stop.set()

    def create_executors(self, name, max_workers=1):
        executors = IDDSThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name)
        return executors

    def create_task(self, task_func, task_output_queue=None, task_args=tuple(), task_kwargs={}, delay_time=10, priority=1):
        return TimerTask(task_func, task_output_queue, task_args, task_kwargs, delay_time, priority, self.logger)

    def add_task(self, task):
        with self._lock:
            heapq.heappush(self._task_queue, task)

    def remove_task(self, task):
        with self._lock:
            self._task_queue.remove(task)
            heapq.heapify(self._task_queue)

    def remove_all(self):
        with self._lock:
            self._task_queue = []

    def get_ready_task(self):
        with self._lock:
            if not self._task_queue:
                return None
            task = self._task_queue[0]
            if task.is_ready():
                heapq.heappop(self._task_queue)
                return task
        return None

    def execute_task(self, task):
        # self.logger.info('execute task: %s' % task)
        task.execute()
        self.add_task(task)

    def execute_local(self):
        while not self.graceful_stop.is_set():
            try:
                task = self.get_ready_task()
                if task:
                    self.executors_timer.submit(self.execute_task, task)
                else:
                    self.graceful_stop.wait(0.0001)
            except Exception as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def execute(self):
        self.execute_local()

    def execute_once(self):
        try:
            task = self.get_ready_task()
            if task:
                self.executors_timer.submit(self.execute_task, task)
        except Exception as error:
            self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def execute_timer_schedule(self):
        try:
            task = self.get_ready_task()
            if task:
                self.executors_timer.submit(self.execute_task, task)
        except Exception as error:
            self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def execute_timer_schedule_thread(self):
        if self.timer_thread is None:
            self.timer_thread = threading.Thread(target=self.execute_local)
            self.timer_thread.start()
