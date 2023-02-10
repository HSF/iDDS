#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2023

import time
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import Sections
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging, get_logger
from idds.common.utils import json_dumps
from idds.core import messages as core_messages, catalog as core_catalog
from idds.agents.common.baseagent import BaseAgent
# from idds.agents.common.eventbus.event import TerminatedProcessingEvent
from idds.agents.common.eventbus.event import TriggerProcessingEvent

from .utils import handle_messages_processing

setup_logging(__name__)


class Receiver(BaseAgent):
    """
    Receiver works to receive workload management messages to update task/job status.
    """

    def __init__(self, num_threads=1, bulk_message_delay=5, bulk_message_size=2000,
                 random_delay=None, **kwargs):
        super(Receiver, self).__init__(num_threads=num_threads, name='Receiver', **kwargs)
        self.config_section = Sections.Carrier
        self.bulk_message_delay = int(bulk_message_delay)
        self.bulk_message_size = int(bulk_message_size)
        self.message_queue = Queue()
        self.logger = get_logger(self.__class__.__name__)

    def __del__(self):
        self.stop_receiver()

    def start_receiver(self):
        if 'receiver' not in self.plugins:
            raise AgentPluginError('Plugin receiver is required')
        self.receiver = self.plugins['receiver']

        self.logger.info("Starting receiver: %s" % self.receiver)
        self.receiver.set_output_queue(self.message_queue)
        self.setup_logger(self.logger)
        self.receiver.start()

    def stop_receiver(self):
        if hasattr(self, 'receiver') and self.receiver:
            self.logger.info("Stopping receiver: %s" % self.receiver)
            self.receiver.stop()

    def get_output_messages(self):
        msgs = []
        try:
            while not self.message_queue.empty():
                msg = self.message_queue.get(False)
                if msg:
                    self.logger.debug("Received message: %s" % str(msg))
                    msgs.append(msg)
        except Exception as error:
            self.logger.error("Failed to get output messages: %s, %s" % (error, traceback.format_exc()))
        return msgs

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.load_plugins()

            self.start_receiver()

            self.add_health_message_task()

            log_prefix = "<Message>"

            while not self.graceful_stop.is_set():
                try:
                    time_start = time.time()
                    output_messages = self.get_output_messages()
                    update_processings, terminated_processings, update_contents, msgs = handle_messages_processing(output_messages,
                                                                                                                   logger=self.logger,
                                                                                                                   log_prefix=log_prefix)

                    if msgs:
                        # self.logger.debug(log_prefix + "adding messages[:3]: %s" % json_dumps(msgs[:3]))
                        core_messages.add_messages(msgs, bulk_size=self.bulk_message_size)

                    if update_contents:
                        self.logger.info(log_prefix + "update_contents[:3]: %s" % json_dumps(update_contents[:3]))
                        core_catalog.update_contents(update_contents)

                    for pr_id in update_processings:
                        # self.logger.info(log_prefix + "TerminatedProcessingEvent(processing_id: %s)" % pr_id)
                        # event = TerminatedProcessingEvent(publisher_id=self.id, processing_id=pr_id)
                        self.logger.info(log_prefix + "TriggerProcessingEvent(processing_id: %s)" % pr_id)
                        event = TriggerProcessingEvent(publisher_id=self.id, processing_id=pr_id)
                        self.event_bus.send(event)

                    for pr_id in terminated_processings:
                        self.logger.info(log_prefix + "TriggerProcessingEvent(processing_id: %s)" % pr_id)
                        event = TriggerProcessingEvent(publisher_id=self.id, processing_id=pr_id, content={'Terminated': True, 'source': 'Receiver'})
                        self.event_bus.send(event)

                    time_delay = self.bulk_message_delay - (time.time() - time_start)
                    if time_delay > 0:
                        time.sleep(time_delay)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        super(Receiver, self).stop()
        self.stop_receiver()


if __name__ == '__main__':
    agent = Receiver()
    agent()
