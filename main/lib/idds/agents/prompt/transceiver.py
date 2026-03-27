#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""
Prompt Transceiver Agent

Subscribes to /topic/panda.workflow and dispatches incoming workflow-task
messages to the appropriate handler:

  msg_type                handler
  ----------------------  --------------------------------
  create_workflow_task    handle_create_workflow_task
  adjust_worker           handle_adjust_worker
  close_workflow_task     handle_close_workflow_task

Message format (all types):
{
    "msg_type":   "<type>",
    "run_id":     "<run_id>",
    "created_at": "<ISO 8601 UTC>",
    "content":    { ... }
}
"""

import time
import threading
import traceback

from idds.common.constants import Sections
from idds.common.exceptions import IDDSException
from idds.common.utils import setup_logging, json_loads
from idds.agents.common.baseagent import BaseAgent

from idds.prompt.brokers.activemq import Subscriber
from idds.prompt.handlers.workflowtaskhandler import (  # type: ignore[import-untyped]
    handle_create_workflow_task,
    handle_adjust_worker,
    handle_close_workflow_task,
)

setup_logging(__name__)


class Transceiver(BaseAgent):
    """
    Transceiver subscribes to /topic/panda.workflow and processes
    create_workflow_task, adjust_worker, and close_workflow_task messages.
    """

    def __init__(
        self,
        namespace=None,
        num_threads=8,
        panda_workflow_subscriber_broker=None,
        **kwargs,
    ):
        super(Transceiver, self).__init__(
            num_threads=num_threads, name="Transceiver", **kwargs
        )
        self.config_section = Sections.Prompt
        self._lock = threading.RLock()
        self.namespace = namespace

        try:
            self.panda_workflow_subscriber_broker = json_loads(panda_workflow_subscriber_broker)
        except Exception as e:
            self.logger.error(f"Error loading panda_workflow_subscriber_broker: {e}")
            self.panda_workflow_subscriber_broker = None

    def __del__(self):
        self.stop()

    def panda_workflow_handler(self, _header, msg, _handler_kwargs={}):
        """
        Dispatch handler for messages received on /topic/panda.workflow.

        Supported msg_type values:
          - create_workflow_task
          - adjust_worker
          - close_workflow_task
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")

        self.logger.debug(
            f"Received panda.workflow message: msg_type={msg_type}, run_id={run_id}"
        )

        try:
            if msg_type == "create_workflow_task":
                handle_create_workflow_task(msg, logger=self.logger)
            elif msg_type == "adjust_worker":
                handle_adjust_worker(msg, logger=self.logger)
            elif msg_type == "close_workflow_task":
                handle_close_workflow_task(msg, logger=self.logger)
            else:
                self.logger.warning(
                    f"Unknown msg_type on /topic/panda.workflow: {msg_type}, run_id={run_id}"
                )
        except Exception as error:
            self.logger.critical(
                f"panda_workflow_handler exception for msg_type={msg_type}, "
                f"run_id={run_id}: {error}\n{traceback.format_exc()}"
            )

    def run_worker(self):
        """Spawn one worker thread hosting the panda.workflow subscriber."""
        self.logger.info("Starting worker thread")

        panda_workflow_subscriber = None

        try:
            if self.panda_workflow_subscriber_broker:
                panda_workflow_subscriber = Subscriber(
                    name="PandaWorkflowSubscriber",
                    namespace=self.namespace,
                    broker=self.panda_workflow_subscriber_broker,
                    handler=self.panda_workflow_handler,
                    handler_kwargs={},
                    logger=self.logger,
                )

            while not self.graceful_stop.is_set():
                try:
                    if panda_workflow_subscriber:
                        panda_workflow_subscriber.monitor()
                    self.graceful_stop.wait(1)
                except IDDSException as error:
                    self.logger.error("Worker loop IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical(
                        "Worker loop exception: %s\n%s"
                        % (str(error), traceback.format_exc())
                    )
        except Exception as error:
            self.logger.critical(
                "Worker setup exception: %s\n%s" % (str(error), traceback.format_exc())
            )
        finally:
            if panda_workflow_subscriber:
                panda_workflow_subscriber.stop()

    def run(self):
        """Main run function."""
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()

            time_check = time.time()
            while not self.graceful_stop.is_set():
                try:
                    num_free_workers = self.get_num_free_workers()
                    max_workers = self.get_max_workers()
                    running_workers = self.get_num_workers()

                    if time.time() - time_check > 600:
                        self.logger.info(
                            f"max_workers: {max_workers}, running_workers: {running_workers}, "
                            f"num_free_workers: {num_free_workers}"
                        )
                        time_check = time.time()

                    if num_free_workers:
                        self.logger.debug(
                            f"max_workers: {max_workers}, running_workers: {running_workers}, "
                            f"num_free_workers: {num_free_workers}"
                        )
                        self.logger.info("has free workers, will submit more workers")
                        self.submit(self.run_worker)
                    self.graceful_stop.wait(1)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical(
                        "Main thread exception: %s\n%s"
                        % (str(error), traceback.format_exc())
                    )
        except KeyboardInterrupt:
            self.stop()


if __name__ == "__main__":
    agent = Transceiver()
    agent()
