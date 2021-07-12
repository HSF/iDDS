#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

import os
import socket
import threading

from idds.common.constants import Sections
from idds.common.constants import (MessageType, MessageTypeStr,
                                   MessageStatus, MessageSource)
from idds.common.plugin.plugin_base import PluginBase
from idds.common.plugin.plugin_utils import load_plugins, load_plugin_sequence
from idds.common.utils import setup_logging
from idds.core import health as core_health, messages as core_messages
from idds.agents.common.timerscheduler import TimerScheduler


setup_logging(__name__)


class BaseAgent(TimerScheduler, PluginBase):
    """
    The base IDDS agent class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(BaseAgent, self).__init__(num_threads)
        self.name = self.__class__.__name__
        self.logger = None
        self.setup_logger()
        self.set_logger(self.logger)

        self.config_section = Sections.Common

        for key in kwargs:
            setattr(self, key, kwargs[key])

        if not hasattr(self, 'heartbeat_delay'):
            self.heartbeat_delay = 600

        if not hasattr(self, 'poll_operation_time_period'):
            self.poll_operation_time_period = 120
        else:
            self.poll_operation_time_period = int(self.poll_operation_time_period)

        self.plugins = {}
        self.plugin_sequence = []

        self.agent_attributes = self.load_agent_attributes(kwargs)

        self.logger.info("agent_attributes: %s" % self.agent_attributes)

    def get_name(self):
        return self.name

    def load_agent_attributes(self, kwargs):
        rets = {}
        for key in kwargs:
            if '.' not in key:
                continue
            key_items = key.split('.')

            ret_items = rets
            for item in key_items[:-1]:
                if item not in ret_items:
                    ret_items[item] = {}
                ret_items = ret_items[item]
            ret_items[key_items[-1]] = kwargs[key]
        return rets

    def load_plugin_sequence(self):
        self.plugin_sequence = load_plugin_sequence(self.config_section)

    def load_plugins(self):
        self.plugins = load_plugins(self.config_section)
        """
        for plugin_name in self.plugin_sequence:
            if plugin_name not in self.plugins:
                raise AgentPluginError("Plugin %s is defined in plugin_sequence but no plugin is defined with this name")
        for plugin_name in self.plugins:
            if plugin_name not in self.plugin_sequence:
                raise AgentPluginError("Plugin %s is defined but it is not defined in plugin_sequence" % plugin_name)
        """

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            self.execute()
        except KeyboardInterrupt:
            self.stop()

    def __call__(self):
        self.run()

    def terminate(self):
        self.stop()

    def health_heartbeat(self):
        hostname = socket.getfqdn()
        pid = os.getpid()
        hb_thread = threading.current_thread()
        thread_id = hb_thread.ident
        thread_name = hb_thread.name
        core_health.add_health_item(agent=self.get_name(), hostname=hostname, pid=pid,
                                    thread_id=thread_id, thread_name=thread_name, payload=None)

    def add_default_tasks(self):
        task = self.create_task(task_func=self.health_heartbeat, task_output_queue=None,
                                task_args=tuple(), task_kwargs={}, delay_time=self.heartbeat_delay,
                                priority=1)
        self.add_task(task)

    def generate_health_messages(self):
        core_health.clean_health(older_than=self.heartbeat_delay * 2)
        items = core_health.retrieve_health_items()
        msg_content = {'msg_type': MessageTypeStr.HealthHeartbeat.value,
                       'agents': items}
        num_msg_content = len(items)

        message = {'msg_type': MessageType.HealthHeartbeat,
                   'status': MessageStatus.New,
                   'source': MessageSource.Conductor,
                   'request_id': None,
                   'workload_id': None,
                   'transform_id': None,
                   'num_contents': num_msg_content,
                   'msg_content': msg_content}
        core_messages.add_message(msg_type=message['msg_type'],
                                  status=message['status'],
                                  source=message['source'],
                                  request_id=message['request_id'],
                                  workload_id=message['workload_id'],
                                  transform_id=message['transform_id'],
                                  num_contents=message['num_contents'],
                                  msg_content=message['msg_content'])

    def add_health_message_task(self):
        task = self.create_task(task_func=self.generate_health_messages, task_output_queue=None,
                                task_args=tuple(), task_kwargs={}, delay_time=self.heartbeat_delay,
                                priority=1)
        self.add_task(task)

    def get_request_message(self, request_id, bulk_size=1):
        return core_messages.retrieve_request_messages(request_id, bulk_size=bulk_size)

    def get_transform_message(self, transform_id, bulk_size=1):
        return core_messages.retrieve_transform_messages(transform_id, bulk_size=bulk_size)

    def get_processing_message(self, processing_id, bulk_size=1):
        return core_messages.retrieve_processing_messages(processing_id, bulk_size=bulk_size)


if __name__ == '__main__':
    agent = BaseAgent()
    agent()
