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
- 'instance': 'dev', 'prod', etc.
- 'msg_type': Message type
- 'run_id': Run identifier
"""

import time
import threading
import traceback

from cachetools import TTLCache

from idds.common.constants import Sections
from idds.common.exceptions import IDDSException
from idds.common.utils import setup_logging, json_loads
from idds.agents.common.baseagent import BaseAgent

# from idds.agents.common.eventbus.event import TerminatedProcessingEvent

from idds.prompt.brokers.activemq import Publisher, Subscriber
from .handlers.slicehandler import slice_handler
from .handlers.workerhandler import worker_handler

setup_logging(__name__)


class Transceiver(BaseAgent):
    """
    Transceiver works to receive workflow management messages.

    1. worker handler: receive messages from SWF Processing Agent
        <!--
                 a. EIC -> 'run imminent' -> idds.worker_handler
                 b. idds.worker_handler -> 'adjust_worker' -> harvester.adjust_worker(start pilots)
                 c. transformer -> 'heartbeat' -> idds.worker_handler
                 d. EIC -> 'run end' -> idds.worker_handler
                 e. idds.worker_handler -> broadcast 'run end' to /topic/panda.transformers
         -->
         <address name="/topic/panda.workers">
           <multicast/>
         </address>

         <address name="/topic/panda.transformer">
           <multicast/>
         </address>
    """

    def __init__(
        self,
        namespace=None,
        num_threads=8,
        timetolive=12 * 3600 * 1000,
        worker_publisher_broker=None,
        worker_subscriber_broker=None,
        transformer_broadcast_broker=None,
        slice_idds_subscriber_broker=None,
        result_idds_subscriber_broker=None,
        **kwargs,
    ):
        super(Transceiver, self).__init__(
            num_threads=num_threads, name="Transceiver", **kwargs
        )
        self.config_section = Sections.Prompt
        self._lock = threading.RLock()
        self.namespace = namespace

        self.timetolive = (
            timetolive  # default time to live for messages in milliseconds
        )

        # worker related brokers
        try:
            self.worker_publisher_broker = json_loads(worker_publisher_broker)
        except Exception as e:
            self.logger.error(f"Error loading worker_publisher_broker with json: {e}")
            self.worker_publisher_broker = None
        try:
            self.worker_subscriber_broker = json_loads(worker_subscriber_broker)
        except Exception as e:
            self.logger.error(f"Error loading worker_subscriber_broker with json: {e}")
            self.worker_subscriber_broker = None
        # self.transformer_broker = transformer_broker  # queue for slice messages to transformers

        try:
            self.transformer_broadcast_broker = json_loads(transformer_broadcast_broker)
        except Exception as e:
            self.logger.error(f"Error loading transformer_broadcast_broker with json: {e}")
            self.transformer_broadcast_broker = None

        # slice related brokers

        try:
            self.slice_idds_subscriber_broker = json_loads(slice_idds_subscriber_broker)
        except Exception as e:
            self.logger.error(f"Error loading slice_idds_subscriber_broker with json: {e}")
            self.slice_idds_subscriber_broker = None
        try:
            self.result_idds_subscriber_broker = json_loads(result_idds_subscriber_broker)
        except Exception as e:
            self.logger.error(f"Error loading result_idds_subscriber_broker with json: {e}")
            self.result_idds_subscriber_broker = None

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
        - 'transformer_heartbeat': heartbeat from transformer

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
                ret = worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
                if ret and "task_id" in ret:
                    self.cache_task_id(msg, ret["task_id"])
            elif msg_type in ("run_end", "end_run", "run_stop"):
                task_id = self.get_task_id_from_cache(msg)
                worker_handler(header, msg, task_id, handler_kwargs, logger=self.logger)
            elif msg_type == "transformer_heartbeat":
                worker_handler(header, msg, None, handler_kwargs, logger=self.logger)
            else:
                self.logger.warning(
                    f"Unknown msg_type received in worker_handler: {msg_type}"
                )
        except Exception as error:
            self.logger.critical(
                f"input_handler exception for msg_type={msg_type}: {error}\n{traceback.format_exc()}"
            )

    def slice_handler(self, header, msg, handler_kwargs={}):
        """Handle result / lifecycle messages.

        Types:
          - 'slice': slice payload
          - 'slice_result': processed slice output
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")
        self.logger.debug(
            f"Received slice message: msg_type={msg_type}, run_id={run_id}"
        )

        try:
            if msg_type == "slice":
                task_id = self.get_task_id_from_cache(msg)
                slice_handler(header, msg, task_id, handler_kwargs, logger=self.logger)
            elif msg_type == "slice_result":
                task_id = self.get_task_id_from_cache(msg)
                slice_handler(header, msg, task_id, handler_kwargs, logger=self.logger)
            else:
                self.logger.warning(
                    f"Unknown msg_type received in slice_handler: {msg_type}"
                )
        except Exception as error:
            self.logger.critical(
                f"slice_handler exception for msg_type={msg_type}: {error}\n{traceback.format_exc()}"
            )

    def run_worker(self):
        """Spawn one worker thread hosting publishers/subscribers and monitoring them."""
        self.logger.info("Starting worker thread")

        # subscribe to worker messages from SWF Processing Agent
        worker_subscriber = None

        # manage to publish worker message to adjust workers (worker will run transformer)
        worker_publisher = None

        # broadcast messages to transformers (e.g., stop/heartbeat)
        transformer_broadcaster = None

        # subscribe to slice messages
        slice_subscriber = None

        # subscribe to result messages from transformers
        result_subscriber = None

        try:

            if self.transformer_broadcast_broker:
                transformer_broadcaster = Publisher(
                    name="TransformerBroadcaster",
                    namespace=self.namespace,
                    broker=self.transformer_broadcast_broker,
                    broadcast=True,
                    logger=self.logger,
                )
            if self.worker_publisher_broker:
                worker_publisher = Publisher(
                    name="WorkerPublisher",
                    namespace=self.namespace,
                    broker=self.worker_publisher_broker,
                    logger=self.logger,
                )

            worker_handler_kwargs = {
                "transformer_broadcaster": transformer_broadcaster,
                "worker_publisher": worker_publisher,
                "panda_attributes": self.panda_attributes,
                "timetolive": self.timetolive,  # 12 * 3600 * 1000
            }

            if self.worker_subscriber_broker:
                worker_subscriber = Subscriber(
                    name="WorkerSubscriber",
                    namespace=self.namespace,
                    broker=self.worker_subscriber_broker,
                    handler=self.worker_handler,
                    handler_kwargs=worker_handler_kwargs,
                    logger=self.logger,
                )

            slice_handler_kwargs = {
                "timetolive": self.timetolive,  # 12 * 3600 * 1000
            }
            if self.slice_idds_subscriber_broker:
                slice_subscriber = Subscriber(
                    name="SliceSubscriber",
                    namespace=self.namespace,
                    broker=self.slice_idds_subscriber_broker,
                    handler=self.slice_handler,
                    handler_kwargs=slice_handler_kwargs,
                    logger=self.logger,
                )
            if self.result_idds_subscriber_broker:
                result_subscriber = Subscriber(
                    name="ResultSubscriber",
                    namespace=self.namespace,
                    broker=self.result_idds_subscriber_broker,
                    handler=self.slice_handler,
                    handler_kwargs=slice_handler_kwargs,
                    logger=self.logger,
                )

            while not self.graceful_stop.is_set():
                try:
                    # monitor publishers (re-connect if needed)
                    if transformer_broadcaster:
                        transformer_broadcaster.monitor()
                    if worker_publisher:
                        worker_publisher.monitor()

                    # monitor subscribers (re-subscribe if needed)
                    if worker_subscriber:
                        worker_subscriber.monitor()
                    if slice_subscriber:
                        slice_subscriber.monitor()
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
            if worker_publisher:
                worker_publisher.stop()
            if worker_subscriber:
                worker_subscriber.stop()
            if slice_subscriber:
                slice_subscriber.stop()
            if result_subscriber:
                result_subscriber.stop()

    def run(self):
        """
        Main run function.
        """
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
