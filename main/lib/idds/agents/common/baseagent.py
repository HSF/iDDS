#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2025

import logging
import math
import os
import traceback
import threading
import uuid

from idds.common import exceptions
from idds.common.constants import Sections
from idds.common.constants import (MessageType, MessageTypeStr,
                                   MessageStatus, MessageSource)
from idds.common.plugin.plugin_base import PluginBase
from idds.common.plugin.plugin_utils import load_plugins, load_plugin_sequence
from idds.common.utils import get_process_thread_info
from idds.common.utils import setup_logging, pid_exists, json_dumps, json_loads
from idds.core import health as core_health, messages as core_messages, requests as core_requests
from idds.agents.common.timerscheduler import TimerScheduler
from idds.agents.common.eventbus.eventbus import EventBus
from idds.agents.common.cache.redis import get_redis_cache


setup_logging(__name__)


class PrefixFilter(logging.Filter):
    """
    A logging filter that adds a prefix to every log record.
    """
    def __init__(self, prefix):
        super().__init__()
        self.prefix = prefix

    def filter(self, record):
        record.prefix = self.prefix
        return True


class BaseAgentWorker(object):
    """
    Agent Worker classes
    """
    def __init__(self, **kwargs):
        super(BaseAgentWorker, self).__init__()

    def get_class_name(self):
        return self.__class__.__name__

    def get_logger(self, log_prefix=None):
        """
        Set up and return a process-aware logger.
        The logger name includes class name + process ID for uniqueness.
        """
        class_name = self.get_class_name()
        pid = os.getpid()
        logger_name = f"{class_name}-{pid}"

        logger = logging.getLogger(logger_name)

        if not log_prefix:
            log_prefix = class_name

        # Optional: configure if not already configured
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(process)d] [%(levelname)s] %(prefix)s %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            handler.addFilter(PrefixFilter(log_prefix))
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

        return logger


class BaseAgent(TimerScheduler, PluginBase):
    """
    The base IDDS agent class
    """

    min_request_id = None
    min_request_id_cache = {}
    checking_min_request_id_times = 0
    poll_new_min_request_id_times = 0
    poll_running_min_request_id_times = 0

    def __init__(self, num_threads=1, name="BaseAgent", logger=None, use_process_pool=False, **kwargs):
        super(BaseAgent, self).__init__(num_threads, name=name, use_process_pool=use_process_pool)
        self.name = self.__class__.__name__
        self.id = str(uuid.uuid4())[:8]
        self.logger = logger
        self.setup_logger(self.logger)

        self.thread_id = None
        self.thread_name = None

        self.config_section = Sections.Common

        for key in kwargs:
            setattr(self, key, kwargs[key])

        if not hasattr(self, 'heartbeat_delay'):
            self.heartbeat_delay = 60

        if not hasattr(self, 'health_message_delay'):
            self.health_message_delay = 600

        if not hasattr(self, 'poll_operation_time_period'):
            self.poll_operation_time_period = 120
        else:
            self.poll_operation_time_period = int(self.poll_operation_time_period)

        if not hasattr(self, 'event_interval_delay'):
            self.event_interval_delay = 0.0001
        else:
            self.event_interval_delay = int(self.event_interval_delay)

        if not hasattr(self, 'max_worker_exec_time'):
            self.max_worker_exec_time = 3600
        else:
            self.max_worker_exec_time = int(self.max_worker_exec_time)
        self.num_hang_workers, self.num_active_workers = 0, 0

        self.plugins = {}
        self.plugin_sequence = []

        self.agent_attributes = self.load_agent_attributes(kwargs)

        self.logger.info("agent_attributes: %s" % self.agent_attributes)

        self.event_bus = EventBus()
        self.event_func_map = {}
        self.event_futures = {}

        self.cache = get_redis_cache()

    def set_max_workers(self):
        self.number_workers = 0
        if not hasattr(self, 'max_number_workers') or not self.max_number_workers:
            self.max_number_workers = 3
        else:
            self.max_number_workers = int(self.max_number_workers)

    def get_event_bus(self):
        self.event_bus

    def get_name(self):
        return self.name

    def init_thread_info(self):
        hb_thread = threading.current_thread()
        self.thread_id = hb_thread.ident
        self.thread_name = hb_thread.name

    def get_thread_id(self):
        return self.thread_id

    def get_thread_name(self):
        return self.thread_name

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
        self.plugins = load_plugins(self.config_section, logger=self.logger)
        self.logger.info("plugins: %s" % str(self.plugins))
        """
        for plugin_name in self.plugin_sequence:
            if plugin_name not in self.plugins:
                raise AgentPluginError("Plugin %s is defined in plugin_sequence but no plugin is defined with this name")
        for plugin_name in self.plugins:
            if plugin_name not in self.plugin_sequence:
                raise AgentPluginError("Plugin %s is defined but it is not defined in plugin_sequence" % plugin_name)
        """

    def get_plugin(self, plugin_name):
        if plugin_name in self.plugins and self.plugins[plugin_name]:
            return self.plugins[plugin_name]
        raise exceptions.AgentPluginError("No corresponding plugin configured for %s" % plugin_name)

    def load_min_request_id(self):
        try:
            min_request_id = core_requests.get_min_request_id()
            self.logger.info(f"loaded min_request_id: {min_request_id}")
        except Exception as ex:
            self.logger.error(f"failed to load min_request_id: {ex}")
            min_request_id = 1
        self.logger.info(f"Set min_request_id to : {min_request_id}")
        BaseAgent.min_request_id = min_request_id

    def get_num_hang_active_workers(self):
        return self.num_hang_workers, self.num_active_workers

    def get_event_bulk_size(self):
        return math.ceil(self.get_num_free_workers() / 2)

    def init_event_function_map(self):
        self.event_func_map = {}

    def get_event_function_map(self):
        return self.event_func_map

    def execute_event_schedule(self):
        event_funcs = self.get_event_function_map()
        for event_type in event_funcs:
            exec_func = event_funcs[event_type]['exec_func']
            bulk_size = self.get_event_bulk_size()
            if bulk_size > 0:
                events = self.event_bus.get(event_type, num_events=bulk_size, wait=2, callback=None)
                for event in events:
                    self.submit(exec_func, event)

    def execute_schedules(self):
        # self.execute_timer_schedule()
        self.execute_timer_schedule_thread()
        self.execute_event_schedule()

    def execute(self):
        while not self.graceful_stop.is_set():
            try:
                # self.execute_timer_schedule()
                self.execute_timer_schedule_thread()
                self.execute_event_schedule()
                self.graceful_stop.wait(0.00001)
            except Exception as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.init_thread_info()
            self.load_plugins()

            self.execute()
        except KeyboardInterrupt:
            self.stop()

    def __call__(self):
        self.run()

    def stop(self):
        super(BaseAgent, self).stop()
        try:
            self.event_bus.stop()
        except Exception:
            pass

    def terminate(self):
        self.stop()

    def get_hostname(self):
        hostname, pid, thread_id, thread_name = get_process_thread_info()
        return hostname

    def get_process_thread_info(self):
        hostname, pid, thread_id, thread_name = get_process_thread_info()
        return hostname, pid, thread_id, thread_name

    def is_self(self, health_item):
        hostname, pid, thread_id, thread_name = get_process_thread_info()
        ret = False
        if ('hostname' in health_item and 'pid' in health_item and 'agent' in health_item
            and 'thread_id' in health_item and health_item['hostname'] == hostname        # noqa W503
            and health_item['pid'] == pid and health_item['agent'] == self.get_name()):     # noqa W503
            ret = True
        if not ret:
            pass
            # self.logger.debug("is_self: hostname %s, pid %s, thread_id %s, agent %s, health %s" % (hostname, pid, thread_id, self.get_name(), health_item))
        return ret

    def get_health_payload(self):
        num_hang_workers, num_active_workers = self.get_num_hang_active_workers()
        return {'num_hang_workers': num_hang_workers, 'num_active_workers': num_active_workers}

    def is_ready(self):
        return True

    def health_heartbeat(self, heartbeat_delay=None):
        if heartbeat_delay:
            self.heartbeat_delay = heartbeat_delay
        hostname, pid, thread_id, thread_name = get_process_thread_info()
        payload = self.get_health_payload()
        if payload:
            payload = json_dumps(payload)
        if self.is_ready():
            self.logger.debug("health heartbeat: agent %s, pid %s, thread %s, delay %s, payload %s" % (self.get_name(), pid, thread_name, self.heartbeat_delay, payload))
            core_health.add_health_item(agent=self.get_name(), hostname=hostname, pid=pid,
                                        thread_id=thread_id, thread_name=thread_name, payload=payload)
            core_health.clean_health(older_than=self.heartbeat_delay * 3)

            health_items = core_health.retrieve_health_items()
            pids, pid_not_exists = [], []
            for health_item in health_items:
                if health_item['hostname'] == hostname:
                    pid = health_item['pid']
                    if pid not in pids:
                        pids.append(pid)
            for pid in pids:
                if not pid_exists(pid):
                    pid_not_exists.append(pid)
            if pid_not_exists:
                core_health.clean_health(hostname=hostname, pids=pid_not_exists, older_than=None)

    def get_health_items(self):
        try:
            hostname, pid, thread_id, thread_name = get_process_thread_info()
            core_health.clean_health(older_than=self.heartbeat_delay * 3)
            health_items = core_health.retrieve_health_items()
            pids, pid_not_exists = [], []
            for health_item in health_items:
                if health_item['hostname'] == hostname:
                    pid = health_item['pid']
                    if pid not in pids:
                        pids.append(pid)
            for pid in pids:
                if not pid_exists(pid):
                    pid_not_exists.append(pid)
            if pid_not_exists:
                core_health.clean_health(hostname=hostname, pids=pid_not_exists, older_than=None)

            health_items = core_health.retrieve_health_items()
            return health_items
        except Exception as ex:
            self.logger.warn("Failed to get health items: %s" % str(ex))

        return []

    def get_availability(self):
        try:
            availability = {}
            health_items = self.get_health_items()
            hostname, pid, thread_id, thread_name = get_process_thread_info()
            for item in health_items:
                if item['hostname'] == hostname:
                    if item['agent'] not in availability:
                        availability[item['agent']] = {}
                    payload = item['payload']
                    num_hang_workers = 0
                    num_active_workers = 0
                    if payload:
                        payload = json_loads(payload)
                        num_hang_workers = payload.get('num_hang_workers', 0)
                        num_active_workers = payload.get('num_active_workers', 0)

                    availability[item['agent']]['num_hang_workers'] = num_hang_workers
                    availability[item['agent']]['num_active_workers'] = num_active_workers

            return availability
        except Exception as ex:
            self.logger.warn("Failed to get availability: %s" % str(ex))
        return {}

    def add_default_tasks(self):
        task = self.create_task(task_func=self.health_heartbeat, task_output_queue=None,
                                task_args=tuple(), task_kwargs={}, delay_time=self.heartbeat_delay,
                                priority=1)
        self.add_task(task)

    def generate_health_messages(self):
        core_health.clean_health(older_than=self.heartbeat_delay * 3)
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
                                task_args=tuple(), task_kwargs={}, delay_time=self.health_message_delay,
                                priority=1)
        self.add_task(task)

    def get_request_message(self, request_id, bulk_size=1):
        return core_messages.retrieve_request_messages(request_id, bulk_size=bulk_size)

    def get_transform_message(self, request_id, transform_id, bulk_size=1):
        return core_messages.retrieve_transform_messages(request_id=request_id, transform_id=transform_id, bulk_size=bulk_size)

    def get_processing_message(self, request_id, processing_id, bulk_size=1):
        return core_messages.retrieve_processing_messages(request_id=request_id, processing_id=processing_id, bulk_size=bulk_size)


if __name__ == '__main__':
    agent = BaseAgent()
    agent()
