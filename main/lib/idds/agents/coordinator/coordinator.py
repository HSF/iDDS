#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2024

import random
import time
import threading
import traceback

from idds.common.constants import (Sections)
from idds.common.exceptions import IDDSException
from idds.common.event import EventPriority
from idds.common.utils import setup_logging, get_logger, json_loads
from idds.core import health as core_health
from idds.agents.common.baseagent import BaseAgent


setup_logging(__name__)


class Coordinator(BaseAgent):
    """
    Coordinator works to schedule agents to process different events
    """

    def __init__(self, num_threads=1, coordination_interval_delay=300,
                 interval_delay=5, min_queued_events=10, max_queued_events=20,
                 max_total_files_for_small_task=1000,
                 interval_delay_for_big_task=60,
                 max_boost_interval_delay=3,
                 show_queued_events_time_interval=300, **kwargs):
        super(Coordinator, self).__init__(num_threads=num_threads, name='Coordinator', **kwargs)
        self.config_section = Sections.Coordinator

        self.coordination_interval_delay = coordination_interval_delay
        if self.coordination_interval_delay:
            self.coordination_interval_delay = int(self.coordination_interval_delay)
        else:
            self.coordination_interval_delay = 300

        self._lock = threading.RLock()

        self.events = {}
        self.events_index = {}
        self.events_ids = {}
        self.report = {}
        self.accounts = {}

        self.interval_delay = interval_delay
        if self.interval_delay:
            self.interval_delay = int(self.interval_delay)
        else:
            self.interval_delay = 30
        self.max_boost_interval_delay = int(max_boost_interval_delay)
        self.min_queued_events = min_queued_events
        if self.min_queued_events:
            self.min_queued_events = int(self.min_queued_events)
        else:
            self.min_queued_events = 10
        self.max_queued_events = max_queued_events
        if self.max_queued_events:
            self.max_queued_events = int(self.max_queued_events)
        else:
            self.max_queued_events = 20
        self.max_total_files_for_small_task = max_total_files_for_small_task
        if not self.max_total_files_for_small_task:
            self.max_total_files_for_small_task = 1000
        else:
            self.max_total_files_for_small_task = int(self.max_total_files_for_small_task)
        self.interval_delay_for_big_task = interval_delay_for_big_task
        if not self.interval_delay_for_big_task:
            self.interval_delay_for_big_task = 300
        else:
            self.interval_delay_for_big_task = int(self.interval_delay_for_big_task)
        self.logger = get_logger(self.__class__.__name__)

        self.show_queued_events_time = None
        self.show_queued_events_time_interval = int(show_queued_events_time_interval)

    def __del__(self):
        self.stop_coordinator()

    def is_ready(self):
        # manager = self.event_bus.get_manager()
        # if (self.manager and manager['connect'] and manager['username'] and manager['password']):
        #     return True
        return True

    def get_health_payload(self):
        payload = super(Coordinator, self).get_health_payload()

        manager = self.event_bus.get_manager()
        if payload:
            payload['manager'] = manager
        else:
            payload = {'manager': manager}
        # payload = json_dumps(payload)
        return payload

    def select_coordinator(self):
        self.health_heartbeat(self.coordination_interval_delay)
        self.selected_coordinator = core_health.select_agent(name='Coordinator', newer_than=self.coordination_interval_delay * 2)
        self.logger.debug("Selected coordinator: %s" % self.selected_coordinator)
        payload = json_loads(self.selected_coordinator['payload'])
        self.event_bus.set_manager(payload['manager'])
        self.event_bus.set_coordinator(self)

    def get_schedule_time(self, event, interval_delay):
        last_start_time = self.report.get(event.get_event_id(), {}).get("event_types", {}).get(event._event_type.name, {}).get('start_time', None)
        last_end_time = self.report.get(event.get_event_id(), {}).get("event_types", {}).get(event._event_type.name, {}).get('end_time', None)
        time_delay = None
        requeue_counter = event.get_requeue_counter()
        if requeue_counter:
            if requeue_counter <= 3:
                time_delay = interval_delay * requeue_counter
            else:
                time_delay = interval_delay * random.randint(3, requeue_counter)
        else:
            if last_start_time and last_end_time:
                time_delay = last_end_time - last_start_time
            if not time_delay or time_delay < interval_delay:
                time_delay = interval_delay
        if last_end_time:
            scheduled_time = last_end_time + time_delay
            if scheduled_time < time.time():
                scheduled_time = time.time()
        else:
            scheduled_time = time.time()
        return scheduled_time

    def get_scheduled_prio_time(self, event):
        if event.is_terminating():
            priority = EventPriority.High
            return priority, time.time()
        elif event.has_updates():
            priority = EventPriority.Medium
        else:
            priority = EventPriority.Low

        total_queued_events = self.accounts.get(event._event_type, {}).get('total_queued_events', 0)
        # total_processed_events = self.accounts.get(event._event_type, {}).get('total_processed_events', 0)
        total_files = self.report.get(event.get_event_id(), {}).get("total_files", None)
        processed_files = self.report.get(event.get_event_id(), {}).get("processed_files", None)
        # lack_events = self.accounts.get(event._event_type, {}).get('lack_events', False)

        big_task = False
        to_finish = False
        if total_files and total_files > self.max_total_files_for_small_task:
            big_task = True
        if total_files and processed_files and total_files - processed_files <= self.max_total_files_for_small_task:
            to_finish = True

        if total_queued_events < self.min_queued_events:
            interval_delay = 0
        elif big_task and not to_finish:
            interval_delay = self.interval_delay_for_big_task
        elif total_queued_events and total_queued_events > self.max_queued_events:
            boost_times = 2 * min(int(total_queued_events * 1.0 / self.max_queued_events), self.max_boost_interval_delay)
            interval_delay = self.interval_delay * boost_times
        else:
            interval_delay = self.interval_delay
        scheduled_time = self.get_schedule_time(event, interval_delay)
        return priority, scheduled_time

    def get_event_position(self, event_index, event):
        if not event_index:
            return 0
        for i, event_id in enumerate(event_index):
            if self.events[event_id].scheduled_time > event.scheduled_time:
                return i
        return len(event_index)

    def insert_event(self, event):
        event.scheduled_priority, event.scheduled_time = self.get_scheduled_prio_time(event)
        merge = False
        for old_event_id in self.events_ids.get(event.get_event_id(), []):
            old_event = self.events[old_event_id]
            if old_event.able_to_merge(event):
                old_scheduled_priority = old_event.scheduled_priority
                old_scheduled_time = old_event.scheduled_time

                old_event.merge(event)

                self.events[old_event_id] = old_event
                # old_event.scheduled_priority, old_event.scheduled_time = self.get_scheduled_prio_time(old_event)
                new_scheduled_priority, new_scheduled_time = self.get_scheduled_prio_time(old_event)

                if old_scheduled_priority != new_scheduled_priority or old_scheduled_time != new_scheduled_time:
                    self.events_index[old_event._event_type][old_scheduled_priority].remove(old_event._id)
                    old_event.scheduled_priority = new_scheduled_priority
                    old_event.scheduled_time = new_scheduled_time
                    if old_event.scheduled_priority not in self.events_index[old_event._event_type]:
                        self.events_index[old_event._event_type][old_event.scheduled_priority] = []
                    insert_pos = self.get_event_position(self.events_index[old_event._event_type][old_event.scheduled_priority], old_event)
                    self.events_index[old_event._event_type][old_event.scheduled_priority].insert(insert_pos, old_event._id)
                merge = True
                self.logger.debug("New event %s is merged to old event %s" % (event.to_json(strip=True), old_event.to_json(strip=True)))
                break
        if not merge:
            if event._event_type not in self.events_index:
                self.events_index[event._event_type] = {}
            if event.scheduled_priority not in self.events_index[event._event_type]:
                self.events_index[event._event_type][event.scheduled_priority] = []
            if event.get_event_id() not in self.events_ids:
                self.events_ids[event.get_event_id()] = []

            self.events[event._id] = event
            self.logger.debug("New event %s" % (event.to_json(strip=True)))

            insert_pos = self.get_event_position(self.events_index[event._event_type][event.scheduled_priority], event)
            self.events_index[event._event_type][event.scheduled_priority].insert(insert_pos, event._id)
            self.events_ids[event.get_event_id()].append(event._id)

            if event._event_type not in self.accounts:
                self.accounts[event._event_type] = {'total_queued_events': 1, 'total_processed_events': 0, 'lack_events': False}
            else:
                self.accounts[event._event_type]['total_queued_events'] += 1

    def send(self, event):
        with self._lock:
            try:
                self.insert_event(event)
            except Exception as ex:
                self.logger.error(f"Failed to send event: {ex}")
                self.logger.error(traceback.format_exc())

    def send_bulk(self, events):
        with self._lock:
            for event in events:
                try:
                    self.insert_event(event)
                except Exception as ex:
                    self.logger.error(f"Failed to send event: {ex}")
                    self.logger.error(traceback.format_exc())

    def get(self, event_type, num_events=1, wait=0):
        with self._lock:
            events = []
            for i in range(num_events):
                try:
                    if event_type in self.events_index:
                        for scheduled_priority in [EventPriority.High, EventPriority.Medium, EventPriority.Low]:
                            if (scheduled_priority in self.events_index[event_type] and self.events_index[event_type][scheduled_priority]):
                                event_id = self.events_index[event_type][scheduled_priority][0]
                                if event_id not in self.events:
                                    self.events_index[event_type][scheduled_priority].pop(0)
                                else:
                                    event = self.events[event_id]
                                    if event.scheduled_time <= time.time():
                                        event_id = self.events_index[event_type][scheduled_priority].pop(0)
                                        event = self.events[event_id]
                                        del self.events[event_id]
                                        self.events_ids[event.get_event_id()].remove(event_id)

                                        if event._event_type in self.accounts:
                                            self.accounts[event._event_type]['total_queued_events'] -= 1
                                            self.accounts[event._event_type]['total_processed_events'] += 1

                                        self.logger.debug("Get event %s" % (event.to_json(strip=True)))
                                        events.append(event)
                except Exception as ex:
                    self.logger.error(f"Failed to send event: {ex}")
                    self.logger.error(traceback.format_exc())

            if not events:
                try:
                    if event_type in self.accounts:
                        if self.accounts[event_type]['total_queued_events'] == 0:
                            self.accounts[event_type]['lack_events'] = True
                except Exception as ex:
                    self.logger.error(f"Failed to send event: {ex}")
                    self.logger.error(traceback.format_exc())

            return events

    def send_report(self, event, status, start_time, end_time, source, result):
        try:
            event_id = event.get_event_id()
            event_ret_status = status
            event_name = event._event_type.name
            if not event_ret_status and result:
                event_ret_status = result.get("status", None)
            if event_id not in self.report:
                self.report[event_id] = {"status": event_ret_status,
                                         "total_files": None,
                                         "processed_files": None,
                                         "event_types": {}}
            self.report[event_id]['status'] = event_ret_status
            self.report[event_id]['event_types'][event_name] = {'start_time': start_time,
                                                                'end_time': end_time,
                                                                'source': source,
                                                                'status': event_ret_status,
                                                                'result': result}
        except Exception as ex:
            self.logger.error(f"Failed to send event: {ex}")
            self.logger.error(traceback.format_exc())

    def clean_cache_info(self):
        with self._lock:
            try:
                event_ids = list(self.events_ids.keys())
                for event_id in event_ids:
                    if not self.events_ids[event_id]:
                        del self.events_ids[event_id]

                event_ids = list(self.report.keys())
                for event_id in event_ids:
                    event_types = list(self.report[event_id]['event_types'].keys())
                    for event_type in event_types:
                        end_time = self.report[event_id]['event_types'][event_type].get('end_time', None)
                        if not end_time or end_time < time.time() - 86400 * 10:
                            del self.report[event_id]['event_types'][event_type]
                    if not self.report[event_id]['event_types']:
                        del self.report[event_id]
            except Exception as ex:
                self.logger.error(f"Failed to send event: {ex}")
                self.logger.error(traceback.format_exc())

    def show_queued_events(self):
        try:
            if self.show_queued_events_time is None or self.show_queued_events_time + self.show_queued_events_time_interval < time.time():
                self.show_queued_events_time = time.time()
                for event_type in self.events_index:
                    self.logger.info("Number of events has processed: %s: %s" % (event_type.name, self.accounts.get(event_type, {}).get('total_processed_events', None)))
                    for prio in self.events_index[event_type]:
                        self.logger.info("Number of queued events: %s %s: %s" % (event_type.name, prio.name, len(self.events_index[event_type][prio])))
        except Exception as ex:
            self.logger.error(f"Failed to send event: {ex}")
            self.logger.error(traceback.format_exc())

    def coordinate(self):
        self.select_coordinator()
        self.clean_cache_info()
        self.show_queued_events()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()
            # self.load_plugins()

            # coordinator will do the heartbeat by itself.
            # self.add_default_tasks()

            # self.add_health_message_task()

            while not self.graceful_stop.is_set():
                try:
                    self.execute_schedules()
                    self.coordinate()
                    time.sleep(self.coordination_interval_delay)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
                # time.sleep(random.randint(5, self.random_delay))
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        super(Coordinator, self).stop()


if __name__ == '__main__':
    agent = Coordinator()
    agent()
