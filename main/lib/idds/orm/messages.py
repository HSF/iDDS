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

import datetime
import re
import copy

from sqlalchemy import or_, asc
from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.common.constants import MessageDestination
from idds.common.utils import group_list
from idds.orm.base import models
from idds.orm.base.session import transactional_session


@transactional_session
def add_message(msg_type, status, source, request_id, workload_id, transform_id,
                num_contents, msg_content, internal_id=None, bulk_size=None, processing_id=None,
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
        else:
            num_contents_list.append(num_contents)
            msg_content_list.append(msg_content)

        msgs = []
        for msg_content, num_contents in zip(msg_content_list, num_contents_list):
            new_message = {'msg_type': msg_type, 'status': status, 'request_id': request_id,
                           'workload_id': workload_id, 'transform_id': transform_id,
                           'internal_id': internal_id, 'source': source, 'num_contents': num_contents,
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
def add_messages(messages, bulk_size=1000, session=None):
    try:
        # session.bulk_insert_mappings(models.Message, messages)
        for msg in messages:
            add_message(**msg, bulk_size=bulk_size, session=session)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for msg_content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist message, msg_content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist message: %s' % str(e))


@transactional_session
def update_messages(messages, bulk_size=1000, use_bulk_update_mappings=False, request_id=None, transform_id=None, min_request_id=None, session=None):
    try:
        if use_bulk_update_mappings:
            session.bulk_update_mappings(models.Message, messages)
        else:
            groups = group_list(messages, key='msg_id')
            for group_key in groups:
                group = groups[group_key]
                keys = group['keys']
                items = group['items']
                query = session.query(models.Message)
                if request_id:
                    query = query.filter(models.Message.request_id == request_id)
                else:
                    if min_request_id:
                        query = query.filter(or_(models.Message.request_id >= min_request_id,
                                                 models.Message.request_id.is_(None)))
                if transform_id:
                    query = query.filter(models.Message.transform_id == transform_id)
                query = query.filter(models.Message.msg_id.in_(keys))\
                             .update(items, synchronize_session=False)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for msg_content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist message, msg_content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist message: %s' % str(e))


@transactional_session
def retrieve_messages(bulk_size=1000, msg_type=None, status=None, source=None,
                      destination=None, request_id=None, workload_id=None,
                      transform_id=None, processing_id=None, fetching_id=None,
                      min_request_id=None, use_poll_period=False, retries=None,
                      delay=None, internal_id=None, session=None):
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
        if destination is not None:
            if not isinstance(destination, (list, tuple)):
                destination = [destination]
            if len(destination) == 1:
                destination = [destination[0], destination[0]]
        if msg_type is not None:
            if not isinstance(msg_type, (list, tuple)):
                msg_type = [msg_type]
            if len(msg_type) == 1:
                msg_type = [msg_type[0], msg_type[0]]
        if status is not None:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]

        query = session.query(models.Message)

        if msg_type is not None:
            query = query.filter(models.Message.msg_type.in_(msg_type))
        if status is not None:
            query = query.filter(models.Message.status.in_(status))
        if source is not None:
            query = query.filter_by(source=source)
        if destination is not None:
            query = query.filter(models.Message.destination.in_(destination))
        if request_id is not None:
            query = query.filter_by(request_id=request_id)
        else:
            if min_request_id:
                query = query.filter(or_(models.Message.request_id >= min_request_id,
                                         models.Message.request_id.is_(None)))
        if workload_id is not None:
            query = query.filter_by(workload_id=workload_id)
        if transform_id is not None:
            query = query.filter_by(transform_id=transform_id)
        if processing_id is not None:
            query = query.filter_by(processing_id=processing_id)
        if internal_id is not None:
            query = query.filter_by(internal_id=internal_id)
        if retries:
            query = query.filter_by(retries=retries)
        if delay:
            query = query.filter(models.Message.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=delay))
        elif use_poll_period:
            query = query.filter(models.Message.updated_at + models.Message.poll_period <= datetime.datetime.utcnow())

        query = query.order_by(asc(models.Message.updated_at))

        if bulk_size:
            query = query.order_by(models.Message.created_at).limit(bulk_size)
        # query = query.with_for_update(nowait=True)

        tmp = query.all()
        if tmp:
            for t in tmp:
                message = t.to_dict()
                messages.append(message)
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
                filter(or_(*message_condition)).\
                delete(synchronize_session=False)
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@transactional_session
def clean_old_messages(request_id, session=None):
    """
    Delete messages whose request id is older than request_id.

    :param request_id: request id..
    """
    session.query(models.Message)\
           .filter(models.Message.request_id <= request_id)\
           .delete(synchronize_session=False)

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
