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

from idds.common.constants import (Sections, MessageStatus, MessageDestination)
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging
from idds.core import messages as core_messages
from idds.agents.common.baseagent import BaseAgent


setup_logging(__name__)


class Conductor(BaseAgent):
    """
    Conductor works to notify workload management that the data is available.
    """

    def __init__(self, num_threads=1, retrieve_bulk_size=None, **kwargs):
        super(Conductor, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Conductor
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_queue = Queue()

    def __del__(self):
        self.stop_notifier()

    def get_messages(self):
        """
        Get messages
        """
        messages = core_messages.retrieve_messages(status=MessageStatus.New,
                                                   bulk_size=self.retrieve_bulk_size,
                                                   destination=MessageDestination.Outside)

        self.logger.debug("Main thread get %s new messages" % len(messages))
        if messages:
            self.logger.info("Main thread get %s new messages" % len(messages))

        return messages

    def clean_messages(self, msgs):
        # core_messages.delete_messages(msgs)
        to_updates = []
        for msg in msgs:
            to_updates.append({'msg_id': msg['msg_id'],
                               'status': MessageStatus.Delivered})
        core_messages.update_messages(to_updates)

    def start_notifier(self):
        if 'notifier' not in self.plugins:
            raise AgentPluginError('Plugin notifier is required')
        self.notifier = self.plugins['notifier']

        self.logger.info("Starting notifier: %s" % self.notifier)
        self.notifier.set_request_queue(self.message_queue)
        self.notifier.start()

    def stop_notifier(self):
        if hasattr(self, 'notifier') and self.notifier:
            self.logger.info("Stopping notifier: %s" % self.notifier)
            self.notifier.stop()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.load_plugins()

            self.start_notifier()

            self.add_health_message_task()

            while not self.graceful_stop.is_set():
                # execute timer task
                self.execute_once()

                try:
                    messages = self.get_messages()
                    for message in messages:
                        self.message_queue.put(message)
                    while not self.message_queue.empty():
                        time.sleep(1)
                    self.clean_messages(messages)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
                time.sleep(5)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        super(Conductor, self).stop()
        self.stop_notifier()


if __name__ == '__main__':
    agent = Conductor()
    agent()
