#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024

import os
import socket
import time
import traceback
import threading
import uuid

from idds.common import exceptions
from idds.common.constants import Sections
from idds.common.constants import (MessageType, MessageTypeStr,
                                   MessageStatus, MessageSource,
                                   ReturnCode)
from idds.common.plugin.plugin_base import PluginBase
from idds.common.plugin.plugin_utils import load_plugins, load_plugin_sequence
from idds.common.utils import setup_logging, pid_exists, json_dumps, json_loads
from idds.core import health as core_health, messages as core_messages
from idds.agents.common.timerscheduler import TimerScheduler
from idds.agents.common.eventbus.eventbus import EventBus
from idds.agents.common.cache.redis import get_redis_cache


setup_logging(__name__)


class BaseAgent(TimerScheduler, PluginBase):
    """
    The base IDDS agent class
    """

    min_request_id = None
    min_request_id_cache = {}
    checking_min_request_id_times = 0
    poll_new_min_request_id_times = 0
    poll_running_min_request_id_times = 0

    def __init__(self, num_threads=1, name=None, logger=None, **kwargs):
        super(BaseAgent, self).__init__(num_threads, name=name)
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
            self.heartbeat_delay = 600

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

    def get_num_hang_active_workers(self):
        return self.num_hang_workers, self.num_active_workers

    def init_event_function_map(self):
        self.event_func_map = {}

    def get_event_function_map(self):
        return self.event_func_map

    def execute_event_schedule(self):
        event_ids = list(self.event_futures.keys())
        self.num_hang_workers, self.num_active_workers = 0, len(event_ids)

        for event_id in event_ids:
            event, future, start_time = self.event_futures[event_id]
            if future.done():
                ret = future.result()
                status = "finished"
                end_time = time.time()
                if ret is None or ret == 0:
                    self.event_bus.clean_event(event)
                elif ret == ReturnCode.Locked.value:
                    status = "locked"
                    self.event_bus.fail_event(event)
                else:
                    status = "failed"
                    self.event_bus.fail_event(event)
                del self.event_futures[event_id]
                self.event_bus.send_report(event, status, start_time, end_time, self.get_hostname(), ret)
                if status == 'locked':
                    self.logger.warning("Corresponding resource is locked, put the event back again: %s" % json_dumps(event))
                    event.requeue()
                    self.event_bus.send(event)
            else:
                if time.time() - start_time > self.max_worker_exec_time:
                    self.num_hang_workers += 1

        event_funcs = self.get_event_function_map()
        for event_type in event_funcs:
            exec_func = event_funcs[event_type]['exec_func']
            # pre_check = event_funcs[event_type]['pre_check']
            to_exec_at = event_funcs[event_type].get("to_exec_at", None)
            if to_exec_at is None or to_exec_at < time.time():
                # if pre_check():
                num_free_workers = self.executors.get_num_free_workers()
                if num_free_workers > 0:
                    events = self.event_bus.get(event_type, num_free_workers)
                    if events:
                        for event in events:
                            future = self.executors.submit(exec_func, event)
                            self.event_futures[event._id] = (event, future, time.time())
                event_funcs[event_type]["to_exec_at"] = time.time() + self.event_interval_delay

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
        hostname = socket.getfqdn()
        return hostname

    def is_self(self, health_item):
        hostname = socket.getfqdn()
        pid = os.getpid()
        thread_id = self.get_thread_id()
        ret = False
        if ('hostname' in health_item and 'pid' in health_item and 'agent' in health_item
            and 'thread_id' in health_item and health_item['hostname'] == hostname        # noqa W503
            and health_item['pid'] == pid and health_item['agent'] == self.get_name()     # noqa W503
            and health_item['thread_id'] == thread_id):                                              # noqa W503
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
        hostname = socket.getfqdn()
        pid = os.getpid()
        thread_id = self.get_thread_id()
        thread_name = self.get_thread_name()
        payload = self.get_health_payload()
        if payload:
            payload = json_dumps(payload)
        if self.is_ready():
            self.logger.debug("health heartbeat: agent %s, pid %s, thread %s, delay %s, payload %s" % (self.get_name(), pid, thread_name, self.heartbeat_delay, payload))
            core_health.add_health_item(agent=self.get_name(), hostname=hostname, pid=pid,
                                        thread_id=thread_id, thread_name=thread_name, payload=payload)
            core_health.clean_health(older_than=self.heartbeat_delay * 2)

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
            hostname = socket.getfqdn()
            core_health.clean_health(older_than=self.heartbeat_delay * 2)
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
            hostname = socket.getfqdn()
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

    def get_transform_message(self, request_id, transform_id, bulk_size=1):
        return core_messages.retrieve_transform_messages(request_id=request_id, transform_id=transform_id, bulk_size=bulk_size)

    def get_processing_message(self, request_id, processing_id, bulk_size=1):
        return core_messages.retrieve_processing_messages(request_id=request_id, processing_id=processing_id, bulk_size=bulk_size)


if __name__ == '__main__':
    agent = BaseAgent()
    agent()
