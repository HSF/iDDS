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

from idds.common.event import StateClaimEvent, EventBusState
from idds.core import events as core_events

from .baseeventbusbackend import BaseEventBusBackend


class DBEventBusBackend(BaseEventBusBackend):
    """
    Database Event Bus Backend
    """

    def __init__(self, logger=None, to_archive=True, **kwargs):
        super(DBEventBusBackend, self).__init__()
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

        self.to_archive = to_archive

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
        ret = core_events.add_event(event)
        self.logger.info("add event: %s, ret: %s" % (event, ret))

    def send_bulk(self, events):
        for event in events:
            ret = core_events.add_event(event)
            self.logger.info("add event: %s, ret: %s" % (event, ret))

    def get(self, event_type, num_events=1, wait=0):
        event = core_events.get_event_for_processing(event_type=event_type, num_events=num_events)
        return event

    def clean_event(self, event):
        core_events.clean_event(event, to_archive=self.to_archive)

    def fail_event(self, event):
        core_events.fail_event(event, to_archive=self.to_archive)
