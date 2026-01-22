#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2025


import datetime
import logging
import os
import socket
import time
import traceback

from idds.common.utils import setup_logging, json_loads, is_panda_client_verbose

try:
    # prefer local package layout
    from .brokers.activemq import Subscriber, Publisher
    from .payload_process import process_payload
except Exception:
    # fallback to installed package layout
    from idds.prompt.brokers.activemq import Subscriber, Publisher
    from idds.prompt.payload_process import process_payload


setup_logging(__name__)


class Transformer:
    def __init__(self, run_id=None, workdir=None, namespace=None, idle_timeout=1800):
        self._transformer_broker = None
        self._transformer_broadcast_broker = None
        self._result_broker = None
        self._broker_initialized = False

        self._run_id = run_id
        self._workdir = workdir
        self._namespace = namespace
        self._to_stop = False
        self.idle_timeout = idle_timeout

        self.last_message_time = time.time()
        self.logger = logging.getLogger(self.__class__.__name__)

    def init_brokers(self):
        if not self._broker_initialized:
            self.logger.info("To initialize broker")

            self.logger.info("Getting brokers information from central service")
            broker_info = self.get_broker_info()
            self.logger.debug(f"Broker information received: {broker_info}")
            if broker_info:
                transformer_broker = broker_info.get("transformer_broker", None)
                transformer_broadcast_broker = broker_info.get(
                    "transformer_broadcast_broker", None
                )
                result_broker = broker_info.get("result_broker", None)

                if (
                    transformer_broker
                    and transformer_broadcast_broker
                    and result_broker
                ):
                    self._transformer_broker = transformer_broker
                    self._transformer_broadcast_broker = transformer_broadcast_broker
                    self._result_broker = result_broker
                    self._broker_initialized = True
                    self.logger.info("Initialized brokers from central service")
                else:
                    self.logger.warning(
                        "Broker information from the central service is missing, will not initialize it"
                    )
        if not self._broker_initialized:
            self.logger.warning("Broker is not initialized")
        return self._broker_initialized

    def get_idds_server(self):
        return os.environ.get("IDDS_HOST", None)

    def get_broker_info_from_panda_server(self):
        """
        Get broker infomation from the iDDS server through PanDA service.
        :returns broker_info: `dict` broker information.
            {'transformer_broker': ,
             'transformer_broadcast_broker': transformer_broadcast_broker,
             'result_broker': result_broker}

        :raise Exception when failing to get broker information.
        """
        self.logger.info("Get broker information through panda server.")

        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api

        idds_server = self.get_idds_server()
        client = idds_api.get_api(
            idds_utils.json_dumps,
            idds_host=idds_server,
            compress=True,
            verbose=is_panda_client_verbose(),
            manager=True,
        )
        ret = client.get_metainfo(name="prompt_broker")
        self.logger.debug(f"get_metainfo returned: {type(ret)} {ret}")
        if ret[0] == 0 and ret[1][0]:
            idds_ret = ret[1][1]
            if type(idds_ret) in (list, tuple) and idds_ret[0] in (0, True):
                meta_info = idds_ret[1]
                if type(meta_info) in [dict]:
                    pass
                elif type(meta_info) in [str]:
                    try:
                        meta_info = json_loads(meta_info)
                    except Exception as ex:
                        self.logger.warning(
                            "Failed to json loads meta info(%s): %s" % (meta_info, ex)
                        )
            else:
                meta_info = None
                self.logger.warning("Failed to get meta info from iDDS: %s" % str(ret))
        else:
            meta_info = None
            self.logger.warning("Failed to get meta info from Panda server: %s" % str(ret))

        return meta_info

    def get_broker_info(self):
        try:
            return self.get_broker_info_from_panda_server()
        except Exception as ex:
            self.logger.error("Failed to get broker info: %s" % str(ex))

    def transformer_broadcast_handler(self, header, msg, handler_kwargs={}):
        """Handle broadcast messages.

        Types:
          - 'stop_transformer': update transformer configuration
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")
        self.logger.debug(
            f"Received broadcast message: msg_type={msg_type}, run_id={run_id}"
        )

        try:
            if msg_type == "stop_transformer":
                if self._run_id == run_id:
                    self.logger.info(
                        f"Received stop_transformer broadcast for current run_id={run_id}, stopping transformer"
                    )
                    # Implement stopping logic if needed
                    self._to_stop = True
                else:
                    self.logger.info(
                        f"Ignoring stop_transformer broadcast for run_id={run_id}, current run_id={self._run_id}"
                    )
            else:
                self.logger.warning(
                    f"Unknown msg_type received in broadcast_handler: {msg_type}"
                )
        except Exception as error:
            self.logger.critical(
                f"broadcast_handler exception for msg_type={msg_type}: {error}\n{traceback.format_exc()}"
            )

    def transformer_handler(self, header, msg, handler_kwargs={}):
        """Receive slice message and process the payload.

        message = {
            'msg_type': 'slice',
            'run_id': self.current_run_id,
            'created_at': datetime.utcnow().isoformat(),
            'content': {
                'run_id': self.current_run_id,
                'execution_id': self.current_execution_id,
                'req_id': str(uuid.uuid4()),
                'filename': slice_data['stf_filename'],
                'tf_filename': slice_data['tf_filename'],
                'slice_id': slice_data['slice_id'],
                'start': slice_data['tf_first'],
                'end': slice_data['tf_last'],
                'tf_count': slice_data['tf_count'],
                'state': 'queued',
                'substate': 'new'
            }
        }

        Types:
          - 'slice': processed slice payload
        """
        msg_type = msg.get("msg_type")
        run_id = msg.get("run_id")
        self.logger.debug(
            f"Received result message: msg_type={msg_type}, run_id={run_id}"
        )

        result_publisher = None
        try:
            if isinstance(handler_kwargs, dict):
                result_publisher = handler_kwargs.get("result_publisher")

            processing_start_at = datetime.datetime.utcnow()
            status = False
            result = None
            error = None

            if msg_type == "slice":
                content = msg.get("content") or {}
                try:
                    status, result, error = process_payload(content)
                    if status:
                        self.logger.info(
                            f"Processed slice message successfully: run_id={run_id}, result={result}, error={error}"
                        )
                    else:
                        self.logger.error(
                            f"Failed to process slice message: run_id={run_id}, result={result}, error={error}"
                        )
                except Exception as ex:
                    error = str(ex)
                    self.logger.exception(
                        f"Exception while processing payload for run_id={run_id}: {ex}"
                    )
            else:
                self.logger.warning(
                    f"Unknown msg_type received in result_handler: {msg_type}"
                )

            slice_result_msg = {
                "msg_type": "slice_result",
                "run_id": run_id,
                "created_at": datetime.datetime.utcnow(),
                "content": {
                    "requested_at": msg.get("created_at"),
                    "processing_start_at": processing_start_at,
                    "processed_at": datetime.datetime.utcnow(),
                    "state": "done" if status else "failed",
                    "hostname": socket.getfqdn(),
                    "panda_task_id": os.environ.get("PanDA_TaskID"),
                    "panda_id": os.environ.get("PANDAID"),
                    "harvester_id": os.environ.get("HARVESTER_WORKER_ID"),
                    "result": {"state": status, "result": result, "error": error},
                },
            }

            if result_publisher is None:
                self.logger.warning("No result_publisher provided in handler_kwargs; skipping publish")
            else:
                try:
                    result_publisher.publish(slice_result_msg)
                except Exception:
                    self.logger.exception("Failed to publish slice_result_msg")
        except Exception as error:
            self.logger.critical(
                f"result_handler exception for msg_type={msg_type}: {error}\n{traceback.format_exc()}"
            )
            # propagate to inform the message listener
            raise

    def run(self):
        """
        Run the transformer to process messages from transformer broker.
        :returns ret: 0 if run successfully.
        """
        try:
            if not self.init_brokers():
                self.logger.error("Brokers are not initialized, cannot run transformer")
                return False

            self.logger.info("Starting to listen to transformer broker")

            transformer_broadcast_subscriber = Subscriber(
                broker=self._transformer_broadcast_broker,
                handler=self.transformer_broadcast_handler,
                namespace=self._namespace
            )
            result_publisher = Publisher(broker=self._result_broker, namespace=self._namespace)

            selector = None
            if self._run_id is not None:
                selector = f"run_id = '{self._run_id}'"

            transformer_subscriber = Subscriber(
                broker=self._transformer_broker,
                handler=self.transformer_handler,
                selector=selector,
                namespace=self._namespace,
                handler_kwargs={"result_publisher": result_publisher},
            )

            while True:
                if transformer_subscriber.is_idle(idle_seconds=5) and self._to_stop:
                    self.logger.debug(
                        "No message received from transformer broker for 5 seconds and stop requested, stopping transformer."
                    )
                    break
                elif transformer_subscriber.is_idle(idle_seconds=self.idle_timeout):
                    self.logger.debug(
                        f"No message received from transformer broker for {self.idle_timeout} seconds, stopping transformer."
                    )
                    break

                transformer_broadcast_subscriber.monitor()
                transformer_subscriber.monitor()
                result_publisher.monitor()

                time.sleep(1)

            return 0
        except Exception as ex:
            self.logger.error("Error running transformer: %s" % str(ex), exc_info=True)
            return -1
