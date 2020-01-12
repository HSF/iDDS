#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

import time
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import Sections
from idds.common.exceptions import IDDSException
from idds.common.utils import setup_logging
from idds.common.config import (config_has_section, config_has_option,
                                config_get)
from idds.core import messages as core_messages
from idds.agents.common.baseagent import BaseAgent
from idds.agents.conductor.messaging import MessagingSender


setup_logging(__name__)


class Conductor(BaseAgent):
    """
    Conductor works to notify workload management that the data is available.
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Conductor, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Conductor
        self.message_queue = Queue()
        self.notify_method_config_name = 'notify_method'
        self.notify_method = self.get_notify_method()

    def get_notify_method(self):
        if config_has_section(self.config_section) and config_has_option(self.config_section, self.notify_method_config_name):
            self.notify_method = config_get(self.config_section, self.notify_method_config_name)
        else:
            self.notify_method = 'messaging'

        return self.notify_method

    def start_messaging_broker(self):
        if not self.notify_method == 'messaging':
            self.logger.warn("The notify method is %s, not messaing, will not start messaging broker" % self.notify_method)
            return

        try:
            self.logger.info("Starting messaging broker")
            self.msg_broker = MessagingSender()
            self.msg_broker.set_request_queue(self.message_queue)
            self.msg_broker.start()
            self.logger.info("Messaging broker started")
        except Exception as error:
            self.logger.error("Failed to start messaing broker: %s, %s" % (error, traceback.format_exc()))

    def stop_messaging_broker(self):
        if hasattr(self, 'msg_broker') and self.msg_broker:
            try:
                self.logger.info("Stopping messaging broker")
                self.msg_broker.stop()
                self.logger.info("Messaging broker stopped")
            except Exception as error:
                self.logger.error("Failed to stop messaing broker: %s, %s" % (error, traceback.format_exc()))

    def get_messages(self):
        """
        Get messages
        """
        messages = core_messages.retrive_messages()
        self.logger.info("Main thread get %s messages" % len(messages))

        return messages

    def clean_messages(self, msgs):
        core_messages.delete_messages(msgs)

    def start_notifier(self):
        if self.notify_method == 'messaging':
            self.start_messaging_broker()

    def stop_notifier(self):
        if self.notify_method == 'messaging':
            self.stop_messaging_broker()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.start_notifier()
            while not self.graceful_stop.is_set():
                try:
                    messages = self.get_messages()
                    for message in messages:
                        self.message_queue.append(message)
                    while not self.message_queue.empty():
                        time.sleep(1)
                    self.clean_messages(messages)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
                time.sleep(5)
        except KeyboardInterrupt:
            self.stop_notifier()
            self.stop()


if __name__ == '__main__':
    agent = Conductor()
    agent()
