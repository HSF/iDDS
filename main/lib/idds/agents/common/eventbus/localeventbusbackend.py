#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022

import logging
import time
import threading
import traceback
import uuid

from .event import StateClaimEvent, EventBusState


class LocalEventBusBackend(threading.Thread):
    """
    Local Event Bus Backend
    """

    def __init__(self, logger=None, **kwargs):
        super(LocalEventBusBackend, self).__init__()
        self._id = str(uuid.uuid4())[:8]
        self._state_claim_wait = 60
        self._state_claim = StateClaimEvent(self._id, EventBusState.New, time.time())

        self.graceful_stop = threading.Event()

        self._events = {}
        self._events_index = {}

        self._lock = threading.RLock()

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

    def stop(self, signum=None, frame=None):
        self.graceful_stop.set()

    def send(self, event):
        with self._lock:
            if event._event_type not in self._events:
                self._events[event._event_type] = {}
                self._events_index[event._event_type] = []
            self._events[event._event_type][event._id] = event
            self._events_index[event._event_type].append(event._id)

    def get(self, event_type, wait=0):
        with self._lock:
            if event_type in self._events_index and self._events_index[event_type]:
                event_id = self._events_index[event_type].pop(0)
                event = self._events[event_type][event_id]
                del self._events[event_type][event_id]
                return event
            return None

    def execute(self):
        while not self.graceful_stop.is_set():
            try:
                self.graceful_stop.wait(0.1)
            except Exception as error:
                self.logger.critical("Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def run(self):
        self.execute()
