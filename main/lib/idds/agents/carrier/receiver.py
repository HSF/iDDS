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
import threading
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.constants import Sections, ReturnCode
from idds.common.exceptions import AgentPluginError, IDDSException
from idds.common.utils import setup_logging, get_logger
from idds.common.utils import json_dumps
from idds.core import messages as core_messages, catalog as core_catalog
from idds.core import health as core_health
from idds.agents.common.baseagent import BaseAgent
# from idds.agents.common.eventbus.event import TerminatedProcessingEvent
from idds.agents.common.eventbus.event import (EventType, MessageEvent,
                                               TriggerProcessingEvent)

from .utils import handle_messages_processing

setup_logging(__name__)


class Receiver(BaseAgent):
    """
    Receiver works to receive workload management messages to update task/job status.
    """

    def __init__(self, receiver_num_threads=8, num_threads=1, bulk_message_delay=30, bulk_message_size=2000,
                 random_delay=None, update_processing_interval=300, mode='single', **kwargs):
        super(Receiver, self).__init__(num_threads=receiver_num_threads, name='Receiver', **kwargs)
        self.config_section = Sections.Carrier
        self.bulk_message_delay = int(bulk_message_delay)
        self.bulk_message_size = int(bulk_message_size)
        self.message_queue = Queue()
        self.logger = get_logger(self.__class__.__name__)
        self.update_processing_interval = update_processing_interval
        if self.update_processing_interval:
            self.update_processing_interval = int(self.update_processing_interval)
        else:
            self.update_processing_interval = 300

        self.mode = mode
        self.selected = None
        self.selected_receiver = None

        self.log_prefix = ''

        self._lock = threading.RLock()

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
            self.receiver = None

    def suspend_receiver(self):
        if hasattr(self, 'receiver') and self.receiver:
            self.logger.info("Stopping receiver: %s" % self.receiver)
            self.receiver.suspend()

    def resume_receiver(self):
        if hasattr(self, 'receiver') and self.receiver:
            self.logger.info("Resuming receiver: %s" % self.receiver)
            self.receiver.resume()

    def is_receiver_started(self):
        if hasattr(self, 'receiver') and self.receiver and self.receiver.is_processing():
            return True
        return False

    def get_num_queued_messages(self):
        return self.message_queue.qsize()

    def get_output_messages(self):
        with self._lock:
            msgs = []
            try:
                msg_size = 0
                while not self.message_queue.empty():
                    msg = self.message_queue.get(False)
                    if msg:
                        if msg_size < 10:
                            self.logger.debug("Received message(only log first 10 messages): %s" % str(msg))
                        msgs.append(msg)
                        msg_size += 1
                        if msg_size >= self.bulk_message_size:
                            break
            except Exception as error:
                self.logger.error("Failed to get output messages: %s, %s" % (error, traceback.format_exc()))
            if msgs:
                total_msgs = self.get_num_queued_messages()
                self.logger.info("process_messages: Get %s messages, left %s messages" % (len(msgs), total_msgs))
            return msgs

    def is_selected(self):
        selected = None
        if not self.selected_receiver:
            selected = True
        else:
            selected = self.is_self(self.selected_receiver)
        if self.selected is None or self.selected != selected:
            self.logger.info("is_selected changed from %s to %s" % (self.selected, selected))
        self.selected = selected
        return self.selected

    def monitor_receiver(self):
        if self.mode == "single":
            self.logger.info("Receiver single mode")
            self.selected_receiver = core_health.select_agent(name='Receiver', newer_than=self.heartbeat_delay * 2)
            self.logger.debug("Selected receiver: %s" % self.selected_receiver)

    def add_receiver_monitor_task(self):
        task = self.create_task(task_func=self.monitor_receiver, task_output_queue=None,
                                task_args=tuple(), task_kwargs={}, delay_time=self.heartbeat_delay,
                                priority=1)
        self.add_task(task)

    def handle_messages(self, output_messages, log_prefix):
        ret_msg_handle = handle_messages_processing(output_messages,
                                                    logger=self.logger,
                                                    log_prefix=log_prefix,
                                                    update_processing_interval=self.update_processing_interval)

        update_processings, update_processings_by_job, terminated_processings, update_contents, msgs = ret_msg_handle
        if msgs:
            # self.logger.debug(log_prefix + "adding messages[:3]: %s" % json_dumps(msgs[:3]))
            core_messages.add_messages(msgs, bulk_size=self.bulk_message_size)

        num_to_update_contents = 0
        if update_contents:
            self.logger.info(log_prefix + "update_contents[:3]: %s" % json_dumps(update_contents[:3]))
            # instead of update contents directly, add contents to contents_update table.
            # core_catalog.update_contents(update_contents)
            core_catalog.add_contents_update(update_contents)
            num_to_update_contents = len(update_contents)

        for pr_id in update_processings_by_job:
            # self.logger.info(log_prefix + "TerminatedProcessingEvent(processing_id: %s)" % pr_id)
            # event = TerminatedProcessingEvent(publisher_id=self.id, processing_id=pr_id)
            # self.logger.info(log_prefix + "MsgTriggerProcessingEvent(processing_id: %s)" % pr_id)
            self.logger.info(log_prefix + "TriggerProcessingEvent(processing_id: %s)" % pr_id)
            event = TriggerProcessingEvent(publisher_id=self.id, processing_id=pr_id)
            self.event_bus.send(event)

        for pr_id in update_processings:
            # self.logger.info(log_prefix + "TerminatedProcessingEvent(processing_id: %s)" % pr_id)
            # event = TerminatedProcessingEvent(publisher_id=self.id, processing_id=pr_id)
            self.logger.info(log_prefix + "TriggerProcessingEvent(processing_id: %s)" % pr_id)
            event = TriggerProcessingEvent(publisher_id=self.id, processing_id=pr_id,
                                           content={'num_to_update_contents': num_to_update_contents})
            event.set_has_updates()
            self.event_bus.send(event)

        for pr_id in terminated_processings:
            self.logger.info(log_prefix + "TriggerProcessingEvent(processing_id: %s)" % pr_id)
            event = TriggerProcessingEvent(publisher_id=self.id,
                                           processing_id=pr_id,
                                           content={'Terminated': True, 'source': 'Receiver'})
            event.set_terminating()
            self.event_bus.send(event)

    def process_messages(self, log_prefix=None):
        output_messages = self.get_output_messages()
        has_messages = False
        if output_messages:
            self.logger.info("process_messages: Received %s messages" % (len(output_messages)))
            self.handle_messages(output_messages, log_prefix=log_prefix)
            self.logger.info("process_messages: Handled %s messages" % len(output_messages))
            has_messages = True
        return has_messages

    def worker(self, log_prefix):
        while not self.graceful_stop.is_set():
            try:
                has_messages = self.process_messages(log_prefix)
                if not has_messages:
                    time.sleep(1)
            except IDDSException as error:
                self.logger.error("Worker thread IDDSException: %s" % str(error))
            except Exception as error:
                self.logger.critical("Worker thread exception: %s\n%s" % (str(error), traceback.format_exc()))

    def is_ok_to_run_more_workers(self):
        if self.executors.has_free_workers():
            return True
        return False

    def process_messages_event(self, event):
        try:
            pro_ret = ReturnCode.Ok.value
            if event:
                output_messages = event.get_message()
                if output_messages:
                    self.logger.info("process_messages: Received %s messages" % (len(output_messages)))
                    self.handle_messages(output_messages, log_prefix=self.log_prefix)
                    self.logger.info("process_messages: Handled %s messages" % len(output_messages))
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            pro_ret = ReturnCode.Failed.value
        return pro_ret

    def init_event_function_map(self):
        self.event_func_map = {
            EventType.Message: {
                'pre_check': self.is_ok_to_run_more_workers,
                'exec_func': self.process_messages_event
            }
        }

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()

            self.add_default_tasks()

            if self.mode == "single":
                self.logger.debug("single mode")
                self.add_receiver_monitor_task()

            self.load_plugins()

            self.add_health_message_task()

            log_prefix = "<Message>"
            self.log_prefix = log_prefix

            # [self.executors.submit(self.worker, log_prefix) for i in range(self.executors.get_max_workers())]
            self.init_event_function_map()

            self.start_receiver()

            time_start = None
            while not self.graceful_stop.is_set():
                try:
                    self.execute_schedules()

                    if not time_start or time.time() > time_start + self.bulk_message_delay:
                        if self.is_selected():
                            if not self.is_receiver_started():
                                self.resume_receiver()

                        if not self.is_selected():
                            if self.is_receiver_started():
                                self.suspend_receiver()

                        time_start = time.time()
                        msg = self.get_output_messages()
                        if msg:
                            event = MessageEvent(message=msg)
                            self.event_bus.send(event)

                    self.graceful_stop.wait(0.00001)
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
