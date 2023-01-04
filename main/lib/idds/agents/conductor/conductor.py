#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2022

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
from idds.common.utils import setup_logging, get_logger
from idds.core import messages as core_messages
from idds.agents.common.baseagent import BaseAgent


setup_logging(__name__)


class Conductor(BaseAgent):
    """
    Conductor works to notify workload management that the data is available.
    """

    def __init__(self, num_threads=1, retrieve_bulk_size=1000, threshold_to_release_messages=None,
                 random_delay=None, delay=60, replay_times=3, **kwargs):
        super(Conductor, self).__init__(num_threads=num_threads, name='Conductor', **kwargs)
        self.config_section = Sections.Conductor
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.message_queue = Queue()
        self.output_message_queue = Queue()
        if threshold_to_release_messages is None:
            self.threshold_to_release_messages = None
        else:
            self.threshold_to_release_messages = int(threshold_to_release_messages)
        if random_delay is None:
            self.random_delay = None
        else:
            self.random_delay = int(random_delay)
            if self.random_delay < 5:
                self.random_delay = 5
        if delay is None:
            delay = 60
        self.delay = int(delay)
        if replay_times is None:
            replay_times = 3
        self.replay_times = int(replay_times)
        self.logger = get_logger(self.__class__.__name__)

    def __del__(self):
        self.stop_notifier()

    def get_messages(self):
        """
        Get messages
        """
        destination = [MessageDestination.Outside, MessageDestination.ContentExt]
        messages = core_messages.retrieve_messages(status=MessageStatus.New,
                                                   bulk_size=self.retrieve_bulk_size,
                                                   destination=destination)

        # self.logger.debug("Main thread get %s new messages" % len(messages))
        if messages:
            self.logger.info("Main thread get %s new messages" % len(messages))

        retry_messages = []
        for retry in range(1, self.replay_times + 1):
            delay = int(self.delay) * (retry ** 3)

            messages_d = core_messages.retrieve_messages(status=MessageStatus.Delivered,
                                                         retries=retry, delay=delay,
                                                         bulk_size=self.retrieve_bulk_size,
                                                         destination=destination)
            if messages_d:
                self.logger.info("Main thread get %s retries messages" % len(messages_d))
                retry_messages += messages_d

        return messages + retry_messages

    def clean_messages(self, msgs):
        # core_messages.delete_messages(msgs)
        to_updates = []
        for msg in msgs:
            to_updates.append({'msg_id': msg['msg_id'],
                               'retries': msg['retries'] + 1,
                               'status': MessageStatus.Delivered})
        core_messages.update_messages(to_updates)

    def start_notifier(self):
        if 'notifier' not in self.plugins:
            raise AgentPluginError('Plugin notifier is required')
        self.notifier = self.plugins['notifier']

        self.logger.info("Starting notifier: %s" % self.notifier)
        self.notifier.set_request_queue(self.message_queue)
        self.notifier.set_response_queue(self.output_message_queue)
        self.notifier.set_logger(self.logger)
        self.notifier.start()

    def stop_notifier(self):
        if hasattr(self, 'notifier') and self.notifier:
            self.logger.info("Stopping notifier: %s" % self.notifier)
            self.notifier.stop()

    def get_output_messages(self):
        msgs = []
        try:
            while not self.output_message_queue.empty():
                msg = self.output_message_queue.get(False)
                if msg:
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

            self.start_notifier()

            # self.add_health_message_task()

            while not self.graceful_stop.is_set():
                # execute timer task
                self.execute_once()

                try:
                    num_contents = 0
                    messages = self.get_messages()
                    for message in messages:
                        message['destination'] = message['destination'].name

                        num_contents += message['num_contents']
                        self.message_queue.put(message)
                    while not self.message_queue.empty():
                        time.sleep(1)
                    output_messages = self.get_output_messages()
                    self.clean_messages(output_messages)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
                # time.sleep(random.randint(5, self.random_delay))
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        super(Conductor, self).stop()
        self.stop_notifier()


if __name__ == '__main__':
    agent = Conductor()
    agent()
