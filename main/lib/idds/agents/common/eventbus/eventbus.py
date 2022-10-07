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
import uuid

from idds.common.constants import Sections
from idds.common.config import config_has_section, config_list_options

from .localeventbusbackend import LocalEventBusBackend


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
            if 'backend' in attrs and attrs['backend'] == 'message':
                # ToBeDone
                # self.backend = MsgEventBusBackend(**attrs)
                pass
            else:
                self.backend = LocalEventBusBackend(logger=self.logger, **attrs)

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

    def get_event(self, event_type):
        # demand_event = DemandEvent(event._event_type, self._id)
        event = self.backend.get(event_type, wait=10)
        return event

    def get(self, event_type):
        return self.get_event(event_type)

    def send(self, event):
        return self.publish_event(event)

    def stop(self):
        self.backend.stop()
