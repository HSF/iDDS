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

This agent receives and processes messages from the SWF Processing Agent
and manages workflow tasks, workers, and transformers.

Message Format Specification:
See main/prompt.md for complete message format specifications.

All messages should contain:
- 'msg_type': Type of message ('run_imminent', 'slice', 'run_stop', etc.)
- 'run_id': Unique run identifier (e.g., 20250914185722)
- 'created_at': Timestamp when message was created
- 'content': Message-specific content

Headers should contain:
- 'persistent': 'true'
- 'ttl': Time to live in milliseconds (default: 12 * 3600 * 1000)
- 'vo': 'eic'
- 'msg_type': Message type
- 'run_id': Run identifier
"""

import time
import threading
import traceback

from cachetools import TTLCache

from idds.common.constants import Sections
from idds.common.exceptions import IDDSException
from idds.common.utils import setup_logging, get_logger
from idds.agents.common.baseagent import BaseAgent

# from idds.agents.common.eventbus.event import TerminatedProcessingEvent

from idds.prompt.brokers.activemq import Publisher, Subscriber
from .handlers.slicehandler import slice_handler
from .handlers.workerhandler import worker_handler

setup_logging(__name__)


class Transceiver(BaseAgent):
    """
    Transceiver works to receive workflow management messages.
    """

    def __init__(
        self,
        receiver_num_threads=8,
        timetolive=12 * 3600 * 1000,
        worker_broker=None,
        result_broker=None,
        transformer_broadcast_broker=None,
        harvester_broker=None,
        **kwargs,
    ):
        super(Transceiver, self).__init__(
            num_threads=receiver_num_threads, name="PromptTransceiver", **kwargs
        )
        self.config_section = Sections.Prompt
        self.logger = get_logger(self.__class__.__name__)
        self._lock = threading.RLock()

        self.timetolive = (
            timetolive  # default time to live for messages in milliseconds
        )

        self.worker_broker = worker_broker
        self.result_broker = result_broker
        # self.transformer_broker = transformer_broker  # queue for slice messages to transformers
        self.transformer_broadcast_broker = (
            transformer_broadcast_broker  # topic for broadcast (stop/heartbeat)
        )
        self.harvester_broker = harvester_broker  # queue for adjuster_worker messages

        self.run_to_task_cache = TTLCache(
            maxsize=200, ttl=3600 * 24 * 3
        )  # cache expiration 3 days

        # Extract panda.* attributes from kwargs
        self.panda_attributes = {}
        for key, value in kwargs.items():
            if key.startswith("panda."):
                # Remove 'panda.' prefix and store
                attr_name = key[6:]  # Remove 'panda.' prefix
                self.panda_attributes[attr_name] = value
                self.logger.debug(f"Added panda attribute: {attr_name}={value}")
        for k, v in kwargs.items():
            if k.startswith("panda."):
                new_key = k.replace("panda.", "")
                self.panda_attributes[new_key] = v

    def __del__(self):
        self.stop()

    def get_run_id(self, msg):
        """
        Get run_id from message.
        According to prompt.md, all messages should contain 'run_id' field.
        """
        return msg.get("run_id", None)

    def cache_task_id(self, msg, task_id):
        """Cache task_id for a given run_id."""
        run_id = self.get_run_id(msg)
        if run_id:
            self.run_to_task_cache[run_id] = task_id
            self.logger.debug(f"Cached task_id={task_id} for run_id={run_id}")

    def get_task_id_from_cache(self, msg):
        """Retrieve cached task_id for a given run_id."""
        run_id = self.get_run_id(msg)
        task_id = self.run_to_task_cache.get(run_id, None)
        if not task_id:
            self.logger.warning(f"No cached task_id found for run_id={run_id}")
        return task_id

    def worker_handler(self, header, msg, handler_kwargs={}):
        """
        Handle input messages from SWF Processing Agent.

        Based on prompt.md, supported message types:
        - 'run_imminent': Start workers and create workflow
        - 'run_end' or 'run_stop': Stop workers
        - 'slice': Forward to transformer

        All messages should contain 'run_id' in the message body.
        Headers should also contain 'run_id'.
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")

        self.logger.debug(
            f"Received input message: msg_type={msg_type}, run_id={run_id}"
        )

        try:
            if msg_type == "run_imminent":
                # create workflow task and start workers
                ret = worker_handler(header, msg, None, handler_kwargs)
                if ret and "task_id" in ret:
                    self.cache_task_id(msg, ret["task_id"])
            elif msg_type in ("run_end", "run_stop"):
                task_id = self.get_task_id_from_cache(msg)
                worker_handler(header, msg, task_id, handler_kwargs)
            else:
                self.logger.warning(
                    f"Unknown msg_type received in input_handler: {msg_type}"
                )
        except Exception as error:
            self.logger.critical(
                f"input_handler exception for msg_type={msg_type}: {error}\n{traceback.format_exc()}"
            )

    def result_handler(self, header, msg, handler_kwargs={}):
        """Handle result / lifecycle messages.

        Types:
          - 'slice_result': processed slice output
          - 'transformer_start', 'transformer_heartbeat', 'transformer_end': lifecycle
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")
        self.logger.debug(
            f"Received result message: msg_type={msg_type}, run_id={run_id}"
        )

        try:
            if msg_type == "slice_result":
                task_id = self.get_task_id_from_cache(msg)
                slice_handler(header, msg, task_id, handler_kwargs)
            elif msg_type in (
                "transformer_start",
                "transformer_heartbeat",
                "transformer_end",
            ):
                task_id = self.get_task_id_from_cache(msg)
                # reuse worker_handler for logging lifecycle (does not change state)
                worker_handler(header, msg, task_id, handler_kwargs)
            else:
                self.logger.warning(
                    f"Unknown msg_type received in result_handler: {msg_type}"
                )
        except Exception as error:
            self.logger.critical(
                f"result_handler exception for msg_type={msg_type}: {error}\n{traceback.format_exc()}"
            )

    def run_worker(self):
        """Spawn one worker thread hosting publishers/subscribers and monitoring them."""
        self.logger.info("Starting worker thread")

        # subscribe to worker messages from SWF Processing Agent
        worker_subscriber = None

        # manage to tell harvester to adjust workers (worker will run transformer)
        harvester_publisher = None

        # broadcast messages to transformers (e.g., stop/heartbeat)
        transformer_broadcaster = None

        # subscribe to result messages from transformers
        result_subscriber = None

        try:

            if self.transformer_broadcast_broker:
                transformer_broadcaster = Publisher(
                    name="TransformerBroadcaster",
                    broker=self.transformer_broadcast_broker,
                    broadcast=True,
                )
            if self.harvester_broker:
                harvester_publisher = Publisher(
                    name="HarvesterPublisher", broker=self.harvester_broker
                )

            handler_kwargs = {
                "transformer_broadcaster": transformer_broadcaster,
                "harvester_publisher": harvester_publisher,
                "panda_attributes": self.panda_attributes,
                "timetolive": self.timetolive,  # 12 * 3600 * 1000
            }

            if self.worker_broker:
                worker_subscriber = Subscriber(
                    name="WorkerSubscriber",
                    broker=self.worker_broker,
                    handler=self.worker_handler,
                    handler_kwargs=handler_kwargs,
                )
            if self.result_broker:
                result_subscriber = Subscriber(
                    name="ResultSubscriber",
                    broker=self.result_broker,
                    handler=self.result_handler,
                    handler_kwargs=handler_kwargs,
                )

            while not self.graceful_stop.is_set():
                try:
                    # monitor publishers (re-connect if needed)
                    if transformer_broadcaster:
                        transformer_broadcaster.monitor()
                    if harvester_publisher:
                        harvester_publisher.monitor()

                    # monitor subscribers (re-subscribe if needed)
                    if worker_subscriber:
                        worker_subscriber.monitor()
                    if result_subscriber:
                        result_subscriber.monitor()

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
            # orderly shutdown
            if transformer_broadcaster:
                transformer_broadcaster.stop()
            if harvester_publisher:
                harvester_publisher.stop()
            if worker_subscriber:
                worker_subscriber.stop()
            if result_subscriber:
                result_subscriber.stop()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()

            self.setup()

            time_check = time.time()
            while not self.graceful_stop.is_set():
                try:
                    num_free_workers = self.get_num_free_workers()
                    max_workers = self.get_max_workers()
                    running_workers = self.get_num_workers()

                    if time.time() - time_check > 600:
                        self.logger.info(
                            f"max_workers: {max_workers}, running_workers: {running_workers}, num_free_workers: {num_free_workers}"
                        )
                        time_check = time.time()

                    if num_free_workers:
                        self.logger.debug(
                            f"max_workers: {max_workers}, running_workers: {running_workers}, num_free_workers: {num_free_workers}"
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
