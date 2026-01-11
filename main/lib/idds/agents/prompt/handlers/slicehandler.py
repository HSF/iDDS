#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025


import datetime

from idds.common.utils import setup_logging


setup_logging(__name__)


def slice_handler(header, msg, task_id=None, logger=None, handler_kwargs={}):
    """
    Handle slice messages based on prompt.md specifications.

    Message types:
    1. 'slice': Forward to transformer queue
       Format: {
         'msg_type': 'slice',
         'run_id': 20250914185722,
         'created_at': datetime.datetime.utcnow(),
         'content': {
           "run_id": 20250914185722,
           "state": "no_beam",
           "substate": "calib",
           "filename": "swf.20250914.185724.767135.no_beam.calib.stf",
           "start": "20250914185722420185",
           "end": "20250914185724767135",
           "checksum": "ad:3915264619",
           "size": 191,
           "msg_type": "stf_gen",
           "req_id": 1
         }
       }

    2. 'slice_result': Log the result from transformer
    Format: {
         'msg_type': 'slice_result',
         'run_id': 20250914185722,
         'created_at': datetime.datetime.utcnow(),
         'content': {
           'requested_at': <copied from slice's created_at>,
           'processing_start_at': <utctime>,
           'processed_at': <utctime>,
           'result': {'state': ..., 'attribute': 'value'}
         }
       }

    :param header: Message header (should contain 'run_id')
    :param msg: Message content
    :param task_id: Optional task ID
    :param transformer_publisher: Publisher instance to send messages to transformer
    :param timetolive: Time to live for messages in milliseconds
    """
    msg_type = msg.get("msg_type")
    run_id = msg.get("run_id")
    timetolive = handler_kwargs.get("timetolive", 12 * 3600 * 1000)
    transformer_publisher = handler_kwargs.get("transformer_publisher", None)

    try:
        if msg_type == "slice":
            # Forward slice message to transformer queue
            tf_header = {
                "persistent": "true",
                "ttl": timetolive,
                "vo": "eic",
                "msg_type": "slice",
                "run_id": str(run_id),
            }

            # Forward the entire message to transformer
            if transformer_publisher:
                transformer_publisher.publish(msg, headers=tf_header)
                if logger:
                    logger.info(
                        f"Forwarded slice to transformer: run_id={run_id}, filename={msg.get('content', {}).get('filename')}"
                    )
            else:
                if logger:
                    logger.warning(
                        f"No transformer_publisher available to forward slice: run_id={run_id}"
                    )

        elif msg_type == "slice_result":
            # Log the slice result with timing information
            content = msg.get("content", {})
            requested_at = content.get("requested_at")
            processing_start_at = content.get("processing_start_at")
            processed_at = content.get("processed_at")
            result = content.get("result", {})

            if logger:
                logger.info(
                    f"Slice result: run_id={run_id}, requested_at={requested_at}, "
                    f"processing_start_at={processing_start_at}, processed_at={processed_at}, "
                    f"result={result}"
                )
            # Calculate processing delays if timestamps are available
            if requested_at and processed_at:
                try:
                    if isinstance(requested_at, str):
                        requested_dt = datetime.datetime.fromisoformat(requested_at)
                    else:
                        requested_dt = requested_at

                    if isinstance(processed_at, str):
                        processed_dt = datetime.datetime.fromisoformat(processed_at)
                    else:
                        processed_dt = processed_at

                    delay = (processed_dt - requested_dt).total_seconds()
                    if logger:
                        logger.info(
                            f"Slice processing delay: run_id={run_id}, delay={delay:.2f}s"
                        )
                except Exception as ex:
                    if logger:
                        logger.debug(f"Could not calculate delay: {ex}")
        else:
            if logger:
                logger.warning(
                    f"Unknown message type in slice_handler: {msg_type}, run_id={run_id}"
                )
    except Exception as ex:
        if logger:
            logger.error(
                f"Error in slice_handler for msg_type={msg_type}, run_id={run_id}: {ex}",
                exc_info=True,
            )
