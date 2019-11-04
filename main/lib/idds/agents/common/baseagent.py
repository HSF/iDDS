#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import threading
import traceback
import Queue

from concurrent import futures
# from multiprocessing import Process
from threading import Thread

from idds.common.constants import Sections
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.plugin.plugin_base import PluginBase
from idds.common.plugin.plugin_utils import load_plugins, load_plugin_sequence
from idds.common.utils import setup_logging


setup_logging(__name__)


class BaseAgent(Thread, PluginBase):
    """
    The base IDDS agent class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(BaseAgent, self).__init__()

        self.name = self.__class__.__name__
        self.num_threads = num_threads
        self.graceful_stop = threading.Event()
        self.executors = futures.ThreadPoolExecutor(max_workers=num_threads)
        self.tasks = Queue.Queue()
        self.finished_tasks = Queue.Queue()

        self.config_section = Sections.Common

        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.plugins = {}
        self.plugin_sequence = []

        self.logger = None
        self.setup_logger()

        self.messaging_queue = Queue.Queue()

    def stop(self, signum=None, frame=None):
        """
        Graceful exit.
        """
        self.graceful_stop.set()

    def load_plugin_sequence(self):
        self.plugin_sequence = load_plugin_sequence(self.config_section)

    def load_plugins(self):
        self.plugins = load_plugins(self.config_section)
        for plugin_name in self.plugin_sequence:
            if plugin_name not in self.plugins:
                raise AgentPluginError("Plugin %s is defined in plugin_sequence but no plugin is defined with this name")
        for plugin_name in self.plugins:
            if plugin_name not in self.plugin_sequence:
                raise AgentPluginError("Plugin %s is defined but it is not defined in plugin_sequence")

    def get_tasks(self):
        """
        Get tasks to process
        """
        tasks = []
        self.logger.info("Main thread get %s tasks" % len(tasks))
        for task in tasks:
            self.tasks.put(task)

    def process_task(self, task):
        """
        Process task
        """
        for plugin_name in self.plugin_sequence:
            plugin = self.plugins[plugin_name]
            task = plugin(task)
        return task

    def finish_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        while not self.finished_tasks.empty():
            task = self.finished_tasks.get()
            self.logger.info("Main thread finishing task: %s" % task)

    def run_tasks(self, thread_id):
        log_prefix = "[Thread %s]: " % thread_id
        self.logger.info(log_prefix + "Starting worker thread")

        while not self.graceful_stop.is_set():
            try:
                if not self.tasks.empty():
                    task = self.tasks.get()
                    self.logger.info(log_prefix + "Got task: %s" % task)

                    try:
                        self.logger.info(log_prefix + "Processing task: %s" % task)
                        task = self.process_task(task)
                    except IDDSException as error:
                        self.logger.error(log_prefix + "Caught an IDDSException: %s" % str(error))
                    except Exception as error:
                        self.logger.critical(log_prefix + "Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

                    if task:
                        self.logger.info(log_prefix + "Put task to finished queue: %s" % task)
                        self.finished_tasks.put(task)
                else:
                    self.graceful_stop.wait(1)
            except Exception as error:
                self.logger.critical(log_prefix + "Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def sleep_for_tasks(self):
        """
        Sleep for tasks
        """
        if self.finished_tasks.empty() and self.tasks.empty():
            self.logger.info("Main thread will sleep 4 seconds")
            self.graceful_stop.wait(4)
        else:
            self.logger.info("Main thread will sleep 2 seconds")
            self.graceful_stop.wait(2)

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            for i in range(self.num_threads):
                self.executors.submit(self.run_tasks, i)

            while not self.graceful_stop.is_set():
                try:
                    self.get_tasks()
                    self.finish_tasks()
                    self.sleep_for_tasks()
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
        except KeyboardInterrupt:
            self.stop()

    def __calll__(self):
        self.run()


if __name__ == '__main__':
    agent = BaseAgent()
    agent()
