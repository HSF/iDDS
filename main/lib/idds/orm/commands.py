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
operations related to Commands.
"""

import re
import datetime

import sqlalchemy
from sqlalchemy import or_
from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.common.constants import CommandLocation, CommandLocking
from idds.orm.base import models
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_command(cmd_type, status, request_id, workload_id, transform_id,
                username=None, retries=0, processing_id=None,
                source=CommandLocation.Rest, destination=CommandLocation.Clerk,
                cmd_content=None, session=None):
    """
    Add a command to be submitted asynchronously to a command broker.

    :param cmd_type: The type of the cmd as a number, e.g., finished_stagein.
    :param status: The status about the command
    :param source: The source where the command is from.
    :param request_id: The request id.
    :param workload_id: The workload id.
    :param transform_id: The transform id.
    :param cmd_content: The command cmd_content as JSON.
    :param session: The database session.
    """

    try:
        cmd = models.Command(request_id=request_id, workload_id=workload_id,
                             transform_id=transform_id, cmd_type=cmd_type,
                             status=status, substatus=0, locking=0,
                             source=source, destination=destination,
                             username=username, retries=retries,
                             processing_id=processing_id,
                             cmd_content=cmd_content)

        cmd.save(session=session)
        cmd_id = cmd.cmd_id
        return cmd_id
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for cmd_content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist command, cmd_content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist command: %s' % str(e))


@transactional_session
def update_commands(commands, bulk_size=1000, session=None):
    try:
        session.bulk_update_mappings(models.Command, commands)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for cmd_content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist command, cmd_content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist command: %s' % str(e))


@read_session
def retrieve_command(cmd_type=None, status=None, source=None,
                     destination=None, request_id=None, workload_id=None,
                     transform_id=None, processing_id=None, bulk_size=None, session=None):
    """
    Retrieve up to $bulk command.

    :param bulk: Number of command as an integer.
    :param cmd_type: Return only specified cmd_type.
    :param status: The status about the command
    :param source: The source where the command is from.
    :param session: The database session.

    :returns command: List of dictionaries
    """
    command = []
    try:
        query = session.query(models.Command)

        if cmd_type is not None:
            query = query.filter_by(cmd_type=cmd_type)
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
            query = query.order_by(models.Command.created_at).limit(bulk_size)
        # query = query.with_for_update(nowait=True)

        tmp = query.all()
        if tmp:
            for t in tmp:
                command.append(t.to_dict())
        return command
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@transactional_session
def delete_command(command, session=None):
    """
    Delete all command with the given IDs.

    :param command: The command to delete as a list of dictionaries.
    """
    command_condition = []
    for command in command:
        command_condition.append(models.Command.cmd_id == command['cmd_id'])

    try:
        if command_condition:
            session.query(models.Command).\
                filter(or_(*command_condition)).\
                delete(synchronize_session=False)
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@transactional_session
def get_commands_by_status(status, locking=False, period=None, bulk_size=None, session=None):
    """
    Get commands

    :param status: Command status.
    :param locking: Whether only retrieves unlocked items.

    :param session: The database session in use.

    :returns: list of commands.
    """
    try:
        if status:
            if not isinstance(status, (list, tuple)):
                status = [status]
            if len(status) == 1:
                status = [status[0], status[0]]

        query = session.query(models.Command)
        if status:
            query = query.filter(models.Command.status.in_(status))

        if period:
            query = query.filter(models.Command.updated_at <= datetime.datetime.utcnow() - datetime.timedelta(seconds=period))

        if locking:
            query = query.filter(models.Command.locking == CommandLocking.Idle)
        # query = query.with_for_update(skip_locked=True)
        # query = query.order_by(asc(models.Command.updated_at))

        if bulk_size:
            query = query.limit(bulk_size)

        tmp = query.all()
        rets = []
        if tmp:
            rets = [t.to_dict() for t in tmp]
        return rets
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('No commands attached with status (%s): %s' %
                                  (status, error))
    except Exception as error:
        raise error
