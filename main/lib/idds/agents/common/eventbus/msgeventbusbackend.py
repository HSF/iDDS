#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

import logging
import random
import socket
import string
import time
import threading
import traceback
import uuid

import zmq
from zmq.auth.thread import ThreadAuthenticator

from idds.common.utils import json_dumps, json_loads

from .event import StateClaimEvent, EventBusState, TestEvent
from .baseeventbusbackend import BaseEventBusBackend


class MsgEventBusBackendReceiver(threading.Thread):
    def __init__(self, name="MsgEventBusBackendReceiver", logger=None, debug=False,
                 graceful_stop=None, coordinator=None, coordinator_socket=None, **kwargs):
        threading.Thread.__init__(self, name=name)
        self.logger = logger
        self.graceful_stop = graceful_stop
        self.coordinator = coordinator
        self.coordinator_socket = coordinator_socket

        self._events = {}
        self._events_index = {}
        self._events_act_id_index = {}
        self._events_history = {}
        self._events_history_clean_time = time.time()
        self._events_insert_time = {}

        self.max_delay = 180

        self._stop = threading.Event()
        self._lock = threading.RLock()

        self.debug = debug

    def set_coordinator(self, coordinator):
        self.coordinator = coordinator

    def stop(self):
        self._stop.set()

    def run(self):
        while not self.graceful_stop.is_set():
            try:
                if self._stop.is_set():
                    return

                try:
                    req = self.coordinator_socket.recv_string()
                    if self.debug:
                        self.logger.debug("MsgEventBusBackendReceiver received: %s" % req)
                except Exception as error:
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                    self.coordinator_socket.close()

                try:
                    req = json_loads(req)
                    reply = {'ret': None}
                    if self.coordinator:
                        if req['type'] == 'send_event':
                            event = req['event']
                            ret = self.coordinator.send(event)
                            reply = {'type': 'send_event_ret', 'ret': ret}
                        elif req['type'] == 'send_bulk':
                            events = req['events']
                            ret = self.coordinator.send_bulk(events)
                            reply = {'type': 'send_bulk_ret', 'ret': ret}
                        elif req['type'] == 'get_event':
                            event_type = req['event_type']
                            num_events = req['num_events']
                            wait = req['wait']
                            ret = self.coordinator.get(event_type, num_events=num_events, wait=wait)
                            reply = {'type': 'get_event_ret', 'ret': ret}
                    else:
                        if req['type'] == 'send_event':
                            event = req['event']
                            ret = self.send(event)
                            reply = {'type': 'send_event_ret', 'ret': ret}
                        elif req['type'] == 'send_bulk':
                            events = req['events']
                            ret = self.send_bulk(events)
                            reply = {'type': 'send_bulk_ret', 'ret': ret}
                        elif req['type'] == 'get_event':
                            event_type = req['event_type']
                            num_events = req['num_events']
                            wait = req['wait']
                            ret = self.get(event_type, num_events=num_events, wait=wait)
                            reply = {'type': 'get_event_ret', 'ret': ret}
                except Exception as error:
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                    reply = {'type': 'error', 'ret': None}

                reply = json_dumps(reply)
                try:
                    if self.debug:
                        self.logger.debug("MsgEventBusBackendReceiver reply: %s" % reply)
                    self.coordinator_socket.send_string(reply)
                except Exception as error:
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                    self.coordinator_socket.close()

                self.graceful_stop.wait(0.1)
            except Exception as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def insert_event(self, event):
        if event._event_type not in self._events:
            self._events[event._event_type] = {}
            self._events_index[event._event_type] = []
            self._events_act_id_index[event._event_type] = {}
            self._events_history[event._event_type] = {}
            self._events_insert_time[event._event_type] = {}

        self.logger.debug("All events: %s" % self._events)

        merged = False
        event_act_id = event.get_event_id()
        if event_act_id not in self._events_act_id_index[event._event_type]:
            self._events_act_id_index[event._event_type][event_act_id] = [event._id]
        else:
            old_event_ids = self._events_act_id_index[event._event_type][event_act_id].copy()
            for old_event_id in old_event_ids:
                if old_event_id not in self._events[event._event_type]:
                    self._events_act_id_index[event._event_type][event_act_id].remove(old_event_id)
                else:
                    old_event = self._events[event._event_type][old_event_id]
                    if event.able_to_merge(old_event):
                        old_event.merge(event)
                        self._events[event._event_type][old_event_id] = old_event
                        self.logger.debug("New event %s is merged to old event %s" % (event, old_event))
                        merged = True
            if not merged:
                self._events_act_id_index[event._event_type][event_act_id].append(event._id)

        if not merged:
            if event_act_id not in self._events_history[event._event_type]:
                self._events[event._event_type][event._id] = event
                self._events_index[event._event_type].insert(0, event._id)
                self._events_insert_time[event._event_type][event._id] = time.time()
                self.logger.debug("Insert new event: %s" % event)
            else:
                hist_time = self._events_history[event._event_type][event_act_id]
                insert_loc = len(self._events_index[event._event_type])
                q_event_ids = self._events_index[event._event_type].copy()
                q_event_ids.reverse()
                for q_event_id in q_event_ids:
                    q_event = self._events[event._event_type][q_event_id]
                    q_event_act_id = q_event.get_event_id()
                    if (q_event_act_id not in self._events_history[event._event_type] or self._events_insert_time[event._event_type][q_event_id] + self.max_delay < time.time()):
                        break
                    elif self._events_history[event._event_type][q_event_act_id] > hist_time:
                        insert_loc -= 1
                    else:
                        break
                self._events[event._event_type][event._id] = event
                self._events_index[event._event_type].insert(insert_loc, event._id)
                self._events_insert_time[event._event_type][event._id] = time.time()
                self.logger.debug("Insert new event: %s" % event)

    def clean_events(self):
        if self._events_history_clean_time + 3600 * 4 < time.time():
            self._events_history_clean_time = time.time()
            for event_type in self._events_index:
                event_act_ids = []
                for event_id in self._events_index[event_type]:
                    event = self._events[event_type][event_id]
                    act_id = event.get_event_id()
                    event_act_ids.append(act_id)

                event_history_keys = list(self._events_history[event_type].keys())
                for key in event_history_keys:
                    if key not in event_act_ids:
                        del self._events_history[event_type][key]

                act_id_keys = list(self._events_act_id_index[event_type].keys())
                for act_id_key in act_id_keys:
                    act_id2ids = self._events_act_id_index[event_type][act_id_key].copy()
                    for q_id in act_id2ids:
                        if q_id not in self._events_index[event_type]:
                            self._events_act_id_index[event_type][act_id_key].remove(q_id)
                    if not self._events_act_id_index[event_type][act_id_key]:
                        del self._events_act_id_index[event_type][act_id_key]

    def send(self, event):
        with self._lock:
            self.insert_event(event)
            self.clean_events()

    def send_bulk(self, events):
        with self._lock:
            for event in events:
                self.insert_event(event)
            self.clean_events()

    def get(self, event_type, num_events=1, wait=0):
        with self._lock:
            events = []
            for i in range(num_events):
                if event_type in self._events_index and self._events_index[event_type]:
                    event_id = self._events_index[event_type].pop(0)
                    event = self._events[event_type][event_id]
                    event_act_id = event.get_event_id()
                    self._events_history[event_type][event_act_id] = time.time()
                    del self._events[event_type][event_id]
                    del self._events_insert_time[event._event_type][event._id]
                    events.append(event)
            return events


class MsgEventBusBackend(BaseEventBusBackend):
    """
    Msg Event Bus Backend
    """

    def __init__(self, logger=None, coordinator_port=5556, socket_timeout=10, debug=False,
                 timeout_threshold=5, failure_threshold=5, failure_timeout=180,
                 num_of_set_failed_at_threshold=10, **kwargs):
        super(MsgEventBusBackend, self).__init__()
        self._id = str(uuid.uuid4())[:8]
        self._state_claim_wait = 60
        self._state_claim = StateClaimEvent(self._id, EventBusState.New, time.time())

        self.graceful_stop = threading.Event()

        self._lock = threading.RLock()

        self.max_delay = 180

        self._username = 'idds'
        self._password = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(20))

        self._is_ok = True
        self._failed_at = None
        self._failure_timeout = int(failure_timeout)
        self._num_of_set_failed_at = 0
        self._num_of_set_failed_at_threshold = int(num_of_set_failed_at_threshold)
        self.num_success = 0
        self.num_failures = 0
        self.num_timeout = 0
        self.cache_events = []

        self.setup_logger(logger)

        self.socket_timeout = int(socket_timeout)
        self.timeout_threshold = int(timeout_threshold)
        self.failure_threshold = int(failure_threshold)

        self.coordinator_port = int(coordinator_port)
        self.context = None
        self.auth = None
        self.coordinator_socket = None
        self.coordinator_con_string = None

        self.processor = None

        self.manager = None
        self.manager_socket = None

        self.debug = debug

        self.init_msg_channel()

    def setup_logger(self, logger=None):
        """
        Setup logger
        """
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(self.get_class_name())

    def get_class_name(self):
        return self.__class__.__name__

    def stop(self, signum=None, frame=None):
        self.logger.debug("graceful stop")
        self.graceful_stop.set()
        if self.auth:
            self.logger.debug("auth stop")
            self.auth.stop()

    def init_msg_channel(self):
        with self._lock:
            try:
                if not self.context:
                    self.context = zmq.Context()
                    if self.auth:
                        self.auth.stop()

                    self.auth = ThreadAuthenticator(self.context)
                    self.auth.start()
                    # self.auth.allow('127.0.0.1')
                    self.auth.allow()
                    # Instruct authenticator to handle PLAIN requests
                    self.auth.configure_plain(domain='*', passwords={self._username: self._password})

                if not self.coordinator_socket or self.coordinator_socket.closed:
                    self.coordinator_socket = self.context.socket(zmq.REP)
                    self.coordinator_socket.plain_server = True
                    self.coordinator_socket.bind("tcp://*:%s" % self.coordinator_port)

                    hostname = socket.getfqdn()
                    self.coordinator_con_string = "tcp://%s:%s" % (hostname, self.coordinator_port)

                    if self.processor:
                        self.processor.stop()

                    self.processor = MsgEventBusBackendReceiver(logger=self.logger,
                                                                graceful_stop=self.graceful_stop,
                                                                debug=self.debug,
                                                                coordinator_socket=self.coordinator_socket,
                                                                coordinator=self.coordinator)
                    self.processor.start()
            except (zmq.error.ZMQError, zmq.Again) as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.num_failures += 1
            except Exception as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.num_failures += 1

            try:
                if not self.manager_socket or self.manager_socket.closed:
                    manager = self.get_manager()
                    self.manager_socket = self.context.socket(zmq.REQ)
                    self.manager_socket.plain_username = manager['username'].encode('utf-8')
                    self.manager_socket.plain_password = manager['password'].encode('utf-8')
                    self.manager_socket.connect(manager['connect'])
            except (zmq.error.ZMQError, zmq.Again) as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.num_failures += 1
            except Exception as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.num_failures += 1

    def set_manager(self, manager):
        if not manager:
            manager = self.get_manager()

        if (not self.manager and not manager['connect'] and not manager['username'] and not manager['password']):
            if (self.manager['connect'] != manager['connect']
                or self.manager['username'] != manager['username']                        # noqa W503, E129
                or self.manager['password'] != manager['password']):                      # noqa W503, E129
                with self._lock:
                    try:
                        self.manager = manager
                        self.manager_socket = self.context.socket(zmq.REQ)
                        self.manager_socket.plain_username = manager['username'].encode('utf-8')
                        self.manager_socket.plain_password = manager['password'].encode('utf-8')
                        self.manager_socket.connect(manager['connect'])
                    except (zmq.error.ZMQError, zmq.Again) as error:
                        self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                        self.num_failures += 1
                    except Exception as error:
                        self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                        self.num_failures += 1

    def get_manager(self, myself=False):
        if not myself:
            if (self.manager and self.manager['connect'] and self.manager['username'] and self.manager['password']):
                return self.manager
        manager = {'connect': self.coordinator_con_string,
                   'username': self._username,
                   'password': self._password}
        return manager

    def set_coordinator(self, coordinator):
        self.coordinator = coordinator
        if self.processor:
            self.processor.set_coordinator(coordinator)

    def get_coordinator(self):
        return self.coordinator

    def send(self, event):
        with self._lock:
            try:
                req = {'type': 'send_event', 'event': event}
                req = json_dumps(req)
                # self.logger.debug("send:send %s" % req)
                if self.debug:
                    self.logger.debug("MsgEventBusBackend send event: %s" % req)

                self.manager_socket.send_string(req)
                if self.manager_socket.poll(self.socket_timeout * 1000):
                    reply = self.manager_socket.recv_string()
                    # self.logger.debug("send:recv %s" % reply)
                    if self.debug:
                        self.logger.debug("MsgEventBusBackend send event reply: %s" % reply)
                    reply = json_loads(reply)
                    ret = reply['ret']

                    # refresh failures when there are successful requests
                    self.num_failures = 0
                    self.num_timeout = 0
                    self.num_success += 1
                else:
                    ret = None
                    self.cache_events.append(event)
                    self.num_timeout += 1
                    self.logger.critical("timeout to receive a message")

                return ret
            except (zmq.error.ZMQError, zmq.Again) as error:
                if not self.graceful_stop.is_set():
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.manager_socket.close()
                self.cache_events.append(event)
                self.num_failures += 1
            except Exception as error:
                if not self.graceful_stop.is_set():
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.manager_socket.close()
                self.cache_events.append(event)
                self.num_failures += 1

    def send_bulk(self, events):
        with self._lock:
            try:
                req = {'type': 'send_bulk', 'events': events}
                req = json_dumps(req)
                # self.logger.debug("send:send %s" % req)
                if self.debug:
                    self.logger.debug("MsgEventBusBackend send bulk event: %s" % req)

                self.manager_socket.send_string(req)
                if self.manager_socket.poll(self.socket_timeout * 1000):
                    reply = self.manager_socket.recv_string()
                    # self.logger.debug("send:recv %s" % reply)
                    if self.debug:
                        self.logger.debug("MsgEventBusBackend send bulk event reply: %s" % reply)
                    reply = json_loads(reply)
                    ret = reply['ret']

                    # refresh failures when there are successful requests
                    self.num_failures = 0
                    self.num_timeout = 0
                    self.num_success += 1
                else:
                    ret = None
                    for event in events:
                        self.cache_events.append(event)
                    self.num_timeout += 1
                    self.logger.critical("timeout to receive a message")

                return ret
            except (zmq.error.ZMQError, zmq.Again) as error:
                if not self.graceful_stop.is_set():
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.manager_socket.close()
                for event in events:
                    self.cache_events.append(event)
                self.num_failures += 1
            except Exception as error:
                if not self.graceful_stop.is_set():
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.manager_socket.close()
                for event in events:
                    self.cache_events.append(event)
                self.num_failures += 1

    def get(self, event_type, num_events=1, wait=0):
        with self._lock:
            try:
                req = {'type': 'get_event', 'event_type': event_type, 'num_events': num_events, 'wait': wait}
                req = json_dumps(req)
                # self.logger.debug("get:send %s" % req)

                if self.debug:
                    self.logger.debug("MsgEventBusBackend get event: %s" % req)
                self.manager_socket.send_string(req)

                if self.manager_socket.poll(10 * 1000):
                    reply = self.manager_socket.recv_string()
                    # self.logger.debug("send:recv %s" % reply)
                    if self.debug:
                        self.logger.debug("MsgEventBusBackend get event reply: %s" % reply)
                    reply = json_loads(reply)
                    ret = reply['ret']

                    # refresh failures when there are successful requests
                    self.num_failures = 0
                    self.num_success += 1
                    self.num_timeout = 0
                else:
                    ret = None
                    self.num_timeout += 1
                    self.logger.critical("timeout to receive a message")

                return ret
            except (zmq.error.ZMQError, zmq.Again) as error:
                if not self.graceful_stop.is_set():
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.manager_socket.close()
                self.num_failures += 1
            except Exception as error:
                if not self.graceful_stop.is_set():
                    self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
                self.manager_socket.close()
                self.num_failures += 1
        return []

    def test(self):
        if self.num_failures > 0 or self.num_timeout > 0:
            event = TestEvent()
            self.send(event)
            self.get(event._event_type)
        if self.num_timeout > 5:
            if not self.manager_socket.closed:
                self.logger.critical("The number of timeout reached a threshold, close connection.")
                self.manager_socket.close()

    def send_report(self, event, status, start_time, end_time, source, result):
        if self.get_coordinator():
            return self.get_coordinator().send_report(event, status, start_time, end_time, source, result)

    def clean_event(self, event):
        pass

    def fail_event(self, event):
        pass

    def is_ok(self):
        if self._num_of_set_failed_at < self._num_of_set_failed_at_threshold and self._failed_at and self._failed_at + self._failure_timeout < time.time():
            self._is_ok = True
            self._failed_at = None
            self.num_failures = 0
            self.num_timeout = 0
        elif self.num_failures > self.failure_threshold or self.num_timeout > self.timeout_threshold:
            self._is_ok = False
            if not self._failed_at:
                self._failed_at = time.time()
                self._num_of_set_failed_at += 1
        else:
            self._is_ok = True
        return self._is_ok

    def replay_cache_events(self):
        cache_events = self.cache_events
        self.cache_events = []
        for event in cache_events:
            self.send(event)

    def execute(self):
        while not self.graceful_stop.is_set():
            try:
                self.init_msg_channel()
                self.test()
                if self.is_ok():
                    self.replay_cache_events()
                    self.graceful_stop.wait(1)
                else:
                    if self.num_failures > 20:
                        self.graceful_stop.wait(300)
                    else:
                        self.graceful_stop.wait(60)
            except Exception as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))
        self.stop()

    def run(self):
        self.execute()
