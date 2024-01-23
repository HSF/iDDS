#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2023

import datetime
import random
import time
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import (Sections, MessageStatus, MessageDestination, MessageType,
                                   ProcessingStatus, ContentStatus, ContentRelationType)
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging, get_logger
from idds.core import (messages as core_messages,
                       catalog as core_catalog,
                       processings as core_processings,
                       health as core_health)
from idds.agents.common.baseagent import BaseAgent


setup_logging(__name__)


class Conductor(BaseAgent):
    """
    Conductor works to notify workload management that the data is available.
    """

    def __init__(self, num_threads=1, retrieve_bulk_size=200, threshold_to_release_messages=None,
                 random_delay=None, delay=300, interval_delay=10, max_retry_delay=3600,
                 max_normal_retries=10, max_retries=30, replay_times=2, mode='multiple', **kwargs):
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
        if not max_retry_delay:
            max_retry_delay = 3600
        self.max_retry_delay = int(max_retry_delay)

        self.max_normal_retries = int(max_normal_retries)
        self.max_retries = int(max_retries)

        if replay_times is None:
            replay_times = 2
        self.replay_times = int(replay_times)
        if not interval_delay:
            interval_delay = 10
        self.interval_delay = int(interval_delay)
        self.logger = get_logger(self.__class__.__name__)

        self.mode = mode
        self.selected = None
        self.selected_conductor = None

    def __del__(self):
        self.stop_notifier()

    def is_selected(self):
        selected = None
        if not self.selected_conductor:
            selected = True
        else:
            selected = self.is_self(self.selected_conductor)
        if self.selected is None or self.selected != selected:
            self.logger.info("is_selected changed from %s to %s" % (self.selected, selected))
        self.selected = selected
        return self.selected

    def monitor_conductor(self):
        if self.mode == "single":
            self.logger.info("Conductor single mode")
            self.selected_conductor = core_health.select_agent(name='Conductor', newer_than=self.heartbeat_delay * 2)
            self.logger.debug("Selected conductor: %s" % self.selected_conductor)

    def add_conductor_monitor_task(self):
        task = self.create_task(task_func=self.monitor_conductor, task_output_queue=None,
                                task_args=tuple(), task_kwargs={}, delay_time=self.heartbeat_delay,
                                priority=1)
        self.add_task(task)

    def get_messages(self):
        """
        Get messages
        """
        if BaseAgent.min_request_id is None:
            return []

        destination = [MessageDestination.Outside, MessageDestination.ContentExt]
        messages = core_messages.retrieve_messages(status=MessageStatus.New,
                                                   min_request_id=BaseAgent.min_request_id,
                                                   bulk_size=self.retrieve_bulk_size,
                                                   destination=destination)

        # self.logger.debug("Main thread get %s new messages" % len(messages))
        if messages:
            self.logger.info("Main thread get %s new messages" % len(messages))

        # msg_type = [MessageType.StageInCollection, MessageType.StageInWork,
        #             MessageType.ActiveLearningCollection, MessageType.ActiveLearningWork,
        #             MessageType.HyperParameterOptCollection, MessageType.HyperParameterOptWork,
        #             MessageType.ProcessingCollection, MessageType.ProcessingWork,
        #             MessageType.UnknownCollection, MessageType.UnknownWork]

        retry_messages = []
        messages_d = core_messages.retrieve_messages(status=MessageStatus.Delivered,
                                                     min_request_id=BaseAgent.min_request_id,
                                                     use_poll_period=True,
                                                     bulk_size=self.retrieve_bulk_size,
                                                     destination=destination)    # msg_type=msg_type)
        if messages_d:
            self.logger.info("Main thread get %s retries messages" % len(messages_d))
            retry_messages += messages_d

        return messages + retry_messages

    def clean_messages(self, msgs, confirm=False):
        # core_messages.delete_messages(msgs)
        msg_status = MessageStatus.Delivered
        if confirm:
            msg_status = MessageStatus.ConfirmDelivered
        to_updates = []
        for msg in msgs:
            retries = msg['retries']
            if retries < self.max_normal_retries:
                rand_num = random.randint(1, retries + 1)
                delay = int(self.delay) * rand_num
                delay = min(delay, self.max_retry_delay)
            else:
                delay = self.max_retry_delay
            to_updates.append({'msg_id': msg['msg_id'],
                               'request_id': msg['request_id'],
                               'retries': msg['retries'] + 1,
                               'poll_period': datetime.timedelta(seconds=delay),
                               'status': msg_status})
        core_messages.update_messages(to_updates, min_request_id=BaseAgent.min_request_id)

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

    def is_message_processed(self, message):
        retries = message['retries']
        try:
            if message['status'] in [MessageStatus.New]:
                return False
            if retries >= self.max_retries:
                self.logger.info("message %s has reached max retries %s" % (message['msg_id'], self.max_retries))
                return True
            msg_type = message['msg_type']
            if msg_type not in [MessageType.ProcessingFile]:
                if retries < self.replay_times:
                    return False
                else:
                    return True
            else:
                msg_content = message['msg_content']
                request_id = message['request_id']
                transform_id = message['transform_id']
                if 'files' not in msg_content or not msg_content['files']:
                    return True
                if 'relation_type' not in msg_content or msg_content['relation_type'] != 'input':
                    return True

                workload_id = msg_content['workload_id']
                processings = core_processings.get_processings_by_transform_id(transform_id=transform_id)
                find_processing = None
                if processings:
                    for processing in processings:
                        if processing['workload_id'] == workload_id:
                            find_processing = processing
                if find_processing and find_processing['status'] in [ProcessingStatus.Finished, ProcessingStatus.Failed,
                                                                     ProcessingStatus.Lost, ProcessingStatus.SubFinished,
                                                                     ProcessingStatus.Cancelled, ProcessingStatus.Expired,
                                                                     ProcessingStatus.Suspended, ProcessingStatus.Broken]:
                    return True

                files = msg_content['files']
                files_map_id = [f['map_id'] for f in files]
                contents = core_catalog.get_contents_by_request_transform(request_id=request_id,
                                                                          transform_id=transform_id)
                proc_conents = {}
                for content in contents:
                    if content['content_relation_type'] == ContentRelationType.Output:
                        if content['map_id'] not in proc_conents:
                            proc_conents[content['map_id']] = []
                        if content['status'] not in proc_conents[content['map_id']]:
                            proc_conents[content['map_id']].append(content['status'])
                all_map_id_processed = True
                for map_id in files_map_id:
                    content_statuses = proc_conents.get(map_id, [])
                    if not content_statuses:
                        pass
                    if (len(content_statuses) == 1 and content_statuses == [ContentStatus.New]) or ContentStatus.Missing in content_statuses:
                        all_map_id_processed = False
                        return all_map_id_processed
                return all_map_id_processed
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())

            if retries < self.replay_times:
                return False
        return False

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()
            self.load_plugins()

            self.add_default_tasks()

            if self.mode == "single":
                self.logger.debug("single mode")
                self.add_conductor_monitor_task()

            self.start_notifier()

            # self.add_health_message_task()

            while not self.graceful_stop.is_set():
                # execute timer task
                self.execute_schedules()

                try:
                    num_contents = 0
                    if self.is_selected():
                        messages = self.get_messages()
                        if not messages:
                            time.sleep(self.interval_delay)
                    else:
                        messages = []

                    to_discard_messages = []
                    for message in messages:
                        message['destination'] = message['destination'].name

                        num_contents += message['num_contents']
                        if self.is_message_processed(message):
                            self.logger.debug("message (msg_id: %s) is already processed, not resend it again" % message['msg_id'])
                            to_discard_messages.append(message)
                        else:
                            self.message_queue.put(message)
                    if to_discard_messages:
                        self.clean_messages(to_discard_messages, confirm=True)

                    while not self.message_queue.empty():
                        time.sleep(1)
                    output_messages = self.get_output_messages()
                    self.clean_messages(output_messages)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                    self.logger.error(traceback.format_exc())
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
