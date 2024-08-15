#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


"""
operations related to Messages.
"""

import threading

from idds.common.constants import MessageDestination, MessageType, MessageStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm import messages as orm_messages


@transactional_session
def add_message(msg_type, status, source, request_id, workload_id, transform_id,
                num_contents, msg_content, internal_id=None, bulk_size=None, processing_id=0,
                destination=MessageDestination.Outside, session=None):
    """
    Add a message to be submitted asynchronously to a message broker.

    :param msg_type: The type of the msg as a number, e.g., finished_stagein.
    :param status: The status about the message
    :param source: The source where the message is from.
    :param msg_content: The message msg_content as JSON.
    :param session: The database session.
    """
    return orm_messages.add_message(msg_type=msg_type, status=status, source=source,
                                    request_id=request_id, workload_id=workload_id,
                                    transform_id=transform_id, num_contents=num_contents,
                                    destination=destination, processing_id=processing_id,
                                    internal_id=internal_id, bulk_size=bulk_size,
                                    msg_content=msg_content, session=session)


@transactional_session
def add_messages(messages, bulk_size=1000, session=None):
    return orm_messages.add_messages(messages, bulk_size=bulk_size, session=session)


@read_session
def retrieve_messages(bulk_size=None, msg_type=None, status=None, destination=None,
                      source=None, request_id=None, workload_id=None, transform_id=None,
                      processing_id=None, use_poll_period=False, retries=None, delay=None,
                      min_request_id=None, fetching_id=None, internal_id=None, session=None):
    """
    Retrieve up to $bulk messages.

    :param bulk: Number of messages as an integer.
    :param msg_type: Return only specified msg_type.
    :param status: The status about the message
    :param source: The source where the message is from.
    :param session: The database session.

    :returns messages: List of dictionaries
    """
    if fetching_id is None:
        hb_thread = threading.current_thread()
        fetching_id = hb_thread.ident

    return orm_messages.retrieve_messages(bulk_size=bulk_size, msg_type=msg_type,
                                          status=status, source=source, destination=destination,
                                          request_id=request_id, workload_id=workload_id,
                                          transform_id=transform_id, processing_id=processing_id,
                                          retries=retries, delay=delay, fetching_id=fetching_id,
                                          min_request_id=min_request_id, internal_id=internal_id,
                                          use_poll_period=use_poll_period, session=session)


@read_session
def retrieve_request_messages(request_id, bulk_size=1, session=None):
    return retrieve_messages(request_id=request_id,
                             msg_type=MessageType.IDDSCommunication,
                             status=MessageStatus.New,
                             bulk_size=bulk_size,
                             destination=MessageDestination.Clerk,
                             session=session)


@read_session
def retrieve_transform_messages(request_id, transform_id, bulk_size=1, session=None):
    return retrieve_messages(request_id=request_id,
                             transform_id=transform_id,
                             msg_type=MessageType.IDDSCommunication,
                             status=MessageStatus.New,
                             bulk_size=bulk_size,
                             destination=MessageDestination.Transformer,
                             session=session)


@read_session
def retrieve_processing_messages(request_id, processing_id, bulk_size=1, session=None):
    return retrieve_messages(request_id=request_id,
                             processing_id=processing_id,
                             msg_type=MessageType.IDDSCommunication,
                             status=MessageStatus.New,
                             bulk_size=bulk_size,
                             destination=MessageDestination.Carrier,
                             session=session)


@transactional_session
def delete_messages(messages, session=None):
    """
    Delete all messages with the given IDs.

    :param messages: The messages to delete as a list of dictionaries.
    """
    return orm_messages.delete_messages(messages=messages, session=session)


@transactional_session
def update_messages(messages, min_request_id=None, session=None):
    """
    Update all messages status with the given IDs.

    :param messages: The messages to be updated as a list of dictionaries.
    """
    return orm_messages.update_messages(messages=messages, min_request_id=min_request_id, session=session)


@transactional_session
def clean_old_messages(request_id, session=None):
    """
    Delete messages whose request id is older than request_id.

    :param request_id: request id..
    """
    return orm_messages.clean_old_messages(request_id=request_id, session=session)
