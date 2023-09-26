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
import time
import threading
import uuid

from .event import StateClaimEvent, EventBusState
from .baseeventbusbackend import BaseEventBusBackend


class BaseEventBusBackendOpt(BaseEventBusBackend):
    """
    Base Event Bus Backend
    """

    def __init__(self, logger=None, **kwargs):
        super(BaseEventBusBackendOpt, self).__init__()
        self._id = str(uuid.uuid4())[:8]
        self._state_claim_wait = 60
        self._state_claim = StateClaimEvent(self._id, EventBusState.New, time.time())

        self.graceful_stop = threading.Event()

        self._events = {}
        self._events_index = {}
        self._events_act_id_index = {}
        self._events_history = {}
        self._events_history_clean_time = time.time()
        self._events_insert_time = {}
        self._lock = threading.RLock()

        self.max_delay = 180

        self.setup_logger(logger)

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
        if self.get_coordinator():
            return self.get_coordinator().send(event)
        else:
            with self._lock:
                self.insert_event(event)
                self.clean_events()

    def send_bulk(self, events):
        if self.get_coordinator():
            return self.get_coordinator().send_bulk(events)
        else:
            with self._lock:
                for event in events:
                    self.insert_event(event)
                self.clean_events()

    def get(self, event_type, num_events=1, wait=0):
        if self.get_coordinator():
            return self.get_coordinator().get(event_type, num_events=num_events, wait=wait)
        else:
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
                    else:
                        break
                return events
