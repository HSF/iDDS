#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025 - 2026

import logging


def process_payload(payload):
    """
    Process the payload.

    Args:
        payload (dict): The input payload to be processed.

    Returns:
        status (int): Status code indicating success (0) or failure (non-zero).
        result (dict): The processed result.
        error (str): Error message if any.
    """
    logger = logging.getLogger("PayloadProcessor")
    logger.info(f"Processing payload: {payload}")

    # Example processing: add a new key-value pair
    processed_payload = payload.copy()
    processed_payload["processed"] = True
    # Return True as status to indicate success (Transformer expects a truthy status)
    return True, processed_payload, None
