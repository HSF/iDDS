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

from idds.common.constants import CommandLocation, CommandLocking, CommandStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm import commands as orm_commands


@transactional_session
def add_command(request_id, cmd_type, cmd_content,
                status=CommandStatus.New, workload_id=None,
                transform_id=None,
                username=None, retries=0, processing_id=0,
                source=CommandLocation.Rest,
                destination=CommandLocation.Clerk, session=None):
    """
    Add a command to be submitted asynchronously to a command broker.

    :param cmd_type: The type of the cmd as a number, e.g., finished_stagein.
    :param status: The status about the command
    :param source: The source where the command is from.
    :param cmd_content: The command cmd_content as JSON.
    :param session: The database session.
    """
    return orm_commands.add_command(cmd_type=cmd_type, status=status, source=source,
                                    request_id=request_id, workload_id=workload_id,
                                    transform_id=transform_id, username=username, retries=retries,
                                    destination=destination, processing_id=processing_id,
                                    cmd_content=cmd_content, session=session)


@read_session
def retrieve_commands(bulk_size=None, cmd_type=None, status=None, destination=None,
                      source=None, request_id=None, workload_id=None, transform_id=None,
                      processing_id=None, session=None):
    """
    Retrieve up to $bulk commands.

    :param bulk: Number of commands as an integer.
    :param cmd_type: Return only specified cmd_type.
    :param status: The status about the command
    :param source: The source where the command is from.
    :param session: The database session.

    :returns commands: List of dictionaries
    """
    return orm_commands.retrieve_commands(bulk_size=bulk_size, cmd_type=cmd_type,
                                          status=status, source=source, destination=destination,
                                          request_id=request_id, workload_id=workload_id,
                                          transform_id=transform_id, processing_id=processing_id,
                                          session=session)


@transactional_session
def delete_commands(commands, session=None):
    """
    Delete all commands with the given IDs.

    :param commands: The commands to delete as a list of dictionaries.
    """
    return orm_commands.delete_commands(commands=commands, session=session)


@transactional_session
def update_commands(commands, session=None):
    """
    Update all commands status with the given IDs.

    :param commands: The commands to be updated as a list of dictionaries.
    """
    return orm_commands.update_commands(commands=commands, session=session)


@transactional_session
def get_commands_by_status(status, locking=False, period=None, session=None):
    """
    Get commands

    :param status: Command status.
    :param locking: Whether only retrieves unlocked items.

    :param session: The database session in use.

    :returns: list of commands.
    """
    cmds = orm_commands.get_commands_by_status(status=status, locking=locking, period=period, session=session)
    if locking:
        parameters = []
        for cmd in cmds:
            param = {'cmd_id': cmd['cmd_id'],
                     'locking': CommandLocking.Locking}
            parameters.append(param)
        orm_commands.update_commands(parameters)
    return cmds
