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

from idds.common.constants import (Sections, MessageStatus)
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging
from idds.core import messages as core_messages
from idds.agents.common.baseagent import BaseAgent


setup_logging(__name__)


class Consumer(BaseAgent):
    """
    Consumer works to notify workload management that the data is available.
    """

    def __init__(self, num_threads=1, retrieve_bulk_size=None, **kwargs):
        super(Consumer, self).__init__(num_threads=num_threads, **kwargs)
        self.config_section = Sections.Consumer
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_queue = Queue()

    def __del__(self):
        self.stop_receiver()

    def get_messages(self):
        """
        Get messages
        """
        messages = core_messages.retrieve_messages(status=MessageStatus.New, bulk_size=self.retrieve_bulk_size)

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

    def start_receiver(self):
        if 'receiver' not in self.plugins:
            raise AgentPluginError('Plugin receiver is required')
        self.receiver = self.plugins['receiver']

        self.logger.info("Starting receiver: %s" % self.receiver)
        self.receiver.set_request_queue(self.message_queue)
        self.receiver.start()

    def stop_receiver(self):
        if hasattr(self, 'receiver') and self.receiver:
            self.logger.info("Stopping receiver: %s" % self.receiver)
            self.receiver.stop()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.load_plugins()

            self.start_receiver()

            while not self.graceful_stop.is_set():
                try:
                    pass
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
                time.sleep(5)
            self.stop()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        super(Consumer, self).stop()
        self.stop_receiver()


if __name__ == '__main__':
    agent = Consumer()
    agent()
