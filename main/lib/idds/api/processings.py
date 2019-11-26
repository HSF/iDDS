#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
operations related to Processings.
"""

from idds.core import processings


def add_processing(transform_id, status, submitter=None, granularity=None, granularity_type=None,
                   expired_at=None, processing_metadata=None):
    """
    Add a processing.

    :param transform_id: Transform id.
    :param status: processing status.
    :param submitter: submitter name.
    :param granularity: Granularity size.
    :param granularity_type: Granularity type.
    :param expired_at: The datetime when it expires.
    :param processing_metadata: The metadata as json.

    :returns: processing id.
    """
    kwargs = {'transform_id': transform_id, 'status': status, 'submitter': submitter,
              'granularity': granularity, 'granularity_type': granularity_type,
              'expired_at': expired_at, 'processing_metadata': processing_metadata}
    return processings.add_processing(**kwargs)


def get_processing(processing_id=None, transform_id=None, retries=0):
    """
    Get a  processing

    :param processing_id: Processing id.
    :param tranform_id: Transform id.
    :param retries: Transform retries.

    :returns: Processing.
    """
    return processings.get_processing(processing_id=processing_id, transform_id=transform_id,
                                      retries=retries)


def update_processing(processing_id, parameters):
    """
    update a processing.

    :param processing_id: the transform id.
    :param parameters: A dictionary of parameters.

    """
    return processings.update_processing(processing_id, parameters)


def delete_processing(processing_id=None):
    """
    delete a processing.

    :param processing_id: The id of the processing.
    """
    return processings.delete_processing(processing_id)
