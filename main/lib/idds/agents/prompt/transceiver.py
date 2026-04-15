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

Subscribes to worker_subscriber_broker (/topic/panda.workers, messages from
swf-panda-workers) and dispatches incoming workflow-task messages to the
appropriate handler:

  msg_type                handler
  ----------------------  --------------------------------
  create_workflow_task    handle_create_workflow_task
  adjust_worker           handle_adjust_worker
  close_workflow_task     handle_close_workflow_task

Outbound messages are published to worker_publisher_broker.

Config is loaded from config_default/idds.cfg, section [prompt].

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

from idds.prompt.brokers.activemq import Subscriber, Publisher
from idds.prompt.handlers.workflowtaskhandler import (  # type: ignore[import-untyped]
    handle_create_workflow_task,
    handle_adjust_worker,
    handle_close_workflow_task,
)

setup_logging(__name__)


class Transceiver(BaseAgent):
    """
    Transceiver subscribes to worker_subscriber_broker (/topic/panda.workers,
    messages originating from swf-panda-workers) and processes
    create_workflow_task, adjust_worker, and close_workflow_task messages.
    Outbound messages are sent to worker_publisher_broker.
    """

    def __init__(
        self,
        namespace=None,
        num_threads=8,
        worker_subscriber_broker=None,
        worker_publisher_broker=None,
        **kwargs,
    ):
        super(Transceiver, self).__init__(
            num_threads=num_threads, name="Transceiver", **kwargs
        )
        self.config_section = Sections.Prompt
        self._lock = threading.RLock()
        self.namespace = namespace

        try:
            self.worker_subscriber_broker = json_loads(worker_subscriber_broker)
        except Exception as e:
            self.logger.error(f"Error loading worker_subscriber_broker: {e}")
            self.worker_subscriber_broker = None

        try:
            self.worker_publisher_broker = json_loads(worker_publisher_broker)
        except Exception as e:
            self.logger.error(f"Error loading worker_publisher_broker: {e}")
            self.worker_publisher_broker = None

        self._worker_publisher = None

    def __del__(self):
        self.stop()

    def _publish_result(self, result_type, run_id, result):
        """Publish a result message to worker_publisher_broker."""
        import datetime
        with self._lock:
            publisher = self._worker_publisher
        if publisher is None:
            self.logger.warning(
                f"No worker_publisher available; dropping result msg_type={result_type}, run_id={run_id}"
            )
            return
        if isinstance(result, list) or isinstance(result, tuple):
            result = {"results": result}
        msg = {
            "msg_type": result_type,
            "run_id": run_id,
            "created_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "content": result,
        }
        try:
            publisher.publish(msg)
        except Exception as ex:
            self.logger.error(
                f"Failed to publish result msg_type={result_type}, run_id={run_id}: {ex}",
                exc_info=True,
            )

    def worker_message_handler(self, _header, msg, _handler_kwargs={}):
        """
        Dispatch handler for messages received on worker_subscriber_broker
        (/topic/panda.workers).

        Supported msg_type values:
          - create_workflow_task
          - adjust_worker
          - close_workflow_task
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")

        self.logger.debug(
            f"Received worker message: msg_type={msg_type}, run_id={run_id}"
        )

        try:
            if msg_type == "create_workflow_task":
                result = handle_create_workflow_task(msg, logger=self.logger)
                result_type = "created_workflow_task"
            elif msg_type == "adjust_worker":
                result = handle_adjust_worker(msg, logger=self.logger)
                result_type = "adjusted_worker"
            elif msg_type == "close_workflow_task":
                result = handle_close_workflow_task(msg, logger=self.logger)
                result_type = "closed_workflow_task"
            else:
                self.logger.warning(
                    f"Unknown msg_type from worker: {msg_type}, run_id={run_id}"
                )
                return

            self._publish_result(result_type, run_id, result)
        except Exception as error:
            self.logger.critical(
                f"worker_message_handler exception for msg_type={msg_type}, "
                f"run_id={run_id}: {error}\n{traceback.format_exc()}"
            )

    def run_worker(self):
        """Subscribe to worker_subscriber_broker, publish to worker_publisher_broker."""
        self.logger.info("Starting worker thread")

        worker_subscriber = None
        worker_publisher = None

        try:
            if self.worker_publisher_broker:
                try:
                    worker_publisher = Publisher(
                        name="WorkerPublisher",
                        namespace=self.namespace,
                        broker=self.worker_publisher_broker,
                        logger=self.logger,
                    )
                    with self._lock:
                        self._worker_publisher = worker_publisher
                except Exception as ex:
                    self.logger.error(f"Failed to create worker publisher: {ex}")
                    worker_publisher = None

            if self.worker_subscriber_broker:
                worker_subscriber = Subscriber(
                    name="WorkerSubscriber",
                    namespace=self.namespace,
                    broker=self.worker_subscriber_broker,
                    handler=self.worker_message_handler,
                    handler_kwargs={},
                    logger=self.logger,
                )

            while not self.graceful_stop.is_set():
                try:
                    if worker_subscriber:
                        worker_subscriber.monitor()
                    if worker_publisher:
                        worker_publisher.monitor()
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
            try:
                if worker_subscriber:
                    worker_subscriber.stop()
            except Exception:
                pass
            try:
                if worker_publisher:
                    worker_publisher.stop()
            except Exception:
                pass

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
