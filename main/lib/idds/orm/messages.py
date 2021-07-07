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
operations related to Messages.
"""

import re
import copy

from sqlalchemy import or_
from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.common.constants import MessageDestination
from idds.orm.base import models
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_message(msg_type, status, source, request_id, workload_id, transform_id,
                num_contents, msg_content, bulk_size=None, processing_id=None,
                destination=MessageDestination.Outside, session=None):
    """
    Add a message to be submitted asynchronously to a message broker.

    :param msg_type: The type of the msg as a number, e.g., finished_stagein.
    :param status: The status about the message
    :param source: The source where the message is from.
    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: The transform id.
    :param num_contents: Number of items in msg_content.
    :param msg_content: The message msg_content as JSON.
    :param session: The database session.
    """

    try:
        num_contents_list = []
        msg_content_list = []
        if bulk_size and num_contents > bulk_size:
            if 'files' in msg_content:
                files = msg_content['files']
                chunks = [files[i:i + bulk_size] for i in range(0, len(files), bulk_size)]
                for chunk in chunks:
                    new_msg_content = copy.deepcopy(msg_content)
                    new_msg_content['files'] = chunk
                    new_num_contents = len(chunk)
                    num_contents_list.append(new_num_contents)
                    msg_content_list.append(new_msg_content)
        else:
            num_contents_list.append(num_contents)
            msg_content_list.append(msg_content)

        msgs = []
        for msg_content, num_contents in zip(msg_content_list, num_contents_list):
            new_message = {'msg_type': msg_type, 'status': status, 'request_id': request_id,
                           'workload_id': workload_id, 'transform_id': transform_id,
                           'source': source, 'num_contents': num_contents,
                           'destination': destination, 'processing_id': processing_id,
                           'locking': 0, 'msg_content': msg_content}
            msgs.append(new_message)

        session.bulk_insert_mappings(models.Message, msgs)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for msg_content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist message, msg_content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist message: %s' % str(e))


@transactional_session
def add_messages(messages, session=None):
    try:
        session.bulk_insert_mappings(models.Message, messages)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for msg_content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist message, msg_content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist message: %s' % str(e))


@transactional_session
def update_messages(messages, session=None):
    try:
        session.bulk_update_mappings(models.Message, messages)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for msg_content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist message, msg_content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist message: %s' % str(e))


@read_session
def retrieve_messages(bulk_size=1000, msg_type=None, status=None, source=None,
                      destination=None, request_id=None, workload_id=None,
                      transform_id=None, processing_id=None, session=None):
    """
    Retrieve up to $bulk messages.

    :param bulk: Number of messages as an integer.
    :param msg_type: Return only specified msg_type.
    :param status: The status about the message
    :param source: The source where the message is from.
    :param session: The database session.

    :returns messages: List of dictionaries
    """
    messages = []
    try:
        query = session.query(models.Message)
        if msg_type is not None:
            query = query.filter_by(msg_type=msg_type)
        if status is not None:
            query = query.filter_by(status=status)
        if source is not None:
            query = query.filter_by(source=source)
        if destination is not None:
            query = query.filter_by(destination=destination)
        if request_id is not None:
            query = query.filter_by(request_id=request_id)
        if workload_id is not None:
            query = query.filter_by(workload_id=workload_id)
        if transform_id is not None:
            query = query.filter_by(transform_id=transform_id)
        if processing_id is not None:
            query = query.filter_by(processing_id=processing_id)

        if bulk_size:
            query = query.order_by(models.Message.created_at).limit(bulk_size)
        # query = query.with_for_update(nowait=True)

        tmp = query.all()
        if tmp:
            for t in tmp:
                messages.append(t.to_dict())
        return messages
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@transactional_session
def delete_messages(messages, session=None):
    """
    Delete all messages with the given IDs.

    :param messages: The messages to delete as a list of dictionaries.
    """
    message_condition = []
    for message in messages:
        message_condition.append(models.Message.msg_id == message['msg_id'])

    try:
        if message_condition:
            session.query(models.Message).\
                with_hint(models.Message, "index(messages MESSAGES_PK)", 'oracle').\
                filter(or_(*message_condition)).\
                delete(synchronize_session=False)
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


# @transactional_session
# def update_messages(messages, session=None):
#     """
#     Update all messages status with the given IDs.
#
#     :param messages: The messages to be updated as a list of dictionaries.
#     """
#     try:
#         for msg in messages:
#             session.query(models.Message).filter_by(msg_id=msg['msg_id']).update({'status': msg['status']}, synchronize_session=False)
#     except IntegrityError as e:
#         raise exceptions.DatabaseException(e.args)
