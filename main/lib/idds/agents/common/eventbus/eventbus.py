#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022 - 2023

import logging
import uuid

from idds.common.constants import Sections
from idds.common.config import config_has_section, config_list_options

# from .localeventbusbackend import LocalEventBusBackend
from .baseeventbusbackendopt import BaseEventBusBackendOpt
from .dbeventbusbackend import DBEventBusBackend
from .msgeventbusbackend import MsgEventBusBackend


class Singleton(object):
    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
            class_._instance._initialized = False
        return class_._instance


class EventBus(Singleton):
    """
    Event Bus
    """

    def __init__(self, logger=None):
        if not self._initialized:
            self._initialized = True

            super(EventBus, self).__init__()
            self._id = str(uuid.uuid4())[:8]
            self.setup_logger(logger)
            self.config_section = Sections.EventBus
            attrs = self.load_attributes()
            self.attrs = attrs
            self._backend = None
            self._orig_backend = None
            self._backup_backend = BaseEventBusBackendOpt(logger=self.logger, **attrs)
            if 'backend' in attrs:
                if attrs['backend'] == 'message':
                    self.backend = MsgEventBusBackend(logger=self.logger, **attrs)
                elif attrs['backend'] == "database":
                    if 'to_archive' not in attrs:
                        attrs['to_archive'] = True
                    self.backend = DBEventBusBackend(**attrs)
            if self.backend is None:
                self.backend = BaseEventBusBackendOpt(logger=self.logger, **attrs)
            self.logger.info("EventBus backend : %s" % self.backend)
            self._orig_backend = self.backend
            self.backend.start()

    @property
    def backend(self):
        if self._backend and isinstance(self._backend, MsgEventBusBackend) and not self._backend.is_ok():
            # self._orig_backend = self._backend
            # self._backend = BaseEventBusBackendOpt(logger=self.logger, **self.attrs)
            self._backend = self._backup_backend
            self.logger.critical("MsgEventBusBackend failed, switch to use BaseEventBusBackendOpt")
        elif self._orig_backend and isinstance(self._orig_backend, MsgEventBusBackend) and self._orig_backend.is_ok():
            if self._backend != self._orig_backend:
                self.logger.critical("MsgEventBusBackend is ok, switch back to use it")
            self._backend = self._orig_backend
            # self._orig_backend = None
        return self._backend

    @backend.setter
    def backend(self, value):
        self._backend = value

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

    def load_attributes(self):
        self.logger.info("Loading config for section: %s" % self.config_section)
        attrs = {}
        if config_has_section(self.config_section):
            options = config_list_options(self.config_section)
            for option, value in options:
                if isinstance(value, str) and value.lower() == 'true':
                    value = True
                if isinstance(value, str) and value.lower() == 'false':
                    value = False
                attrs[option] = value
        return attrs

    def publish_event(self, event):
        self.backend.send(event)

    def get_event(self, event_type, num_events=1):
        # demand_event = DemandEvent(event._event_type, self._id)
        event = self.backend.get(event_type, num_events=num_events, wait=10)
        return event

    def get(self, event_type, num_events=1):
        return self.get_event(event_type, num_events=num_events)

    def send(self, event):
        return self.publish_event(event)

    def send_bulk(self, events):
        self.backend.send_bulk(events)

    def send_report(self, event, status, start_time, end_time, source, result):
        return self.backend.send_report(event, status, start_time, end_time, source, result)

    def clean_event(self, event):
        self.backend.clean_event(event)

    def fail_event(self, event):
        self.backend.fail_event(event)

    def set_manager(self, manager):
        self.backend.set_manager(manager)
        if self._orig_backend:
            self._orig_backend.set_manager(manager)

    def get_manager(self):
        if self._orig_backend:
            return self._orig_backend.get_manager()
        return self.backend.get_manager()

    def get_coordinator(self):
        return self.backend.get_coordinator()

    def set_coordinator(self, coordinator):
        self.backend.set_coordinator(coordinator)

    def stop(self):
        self.backend.stop()
