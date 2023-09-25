#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023


"""
operations related to Events.
"""

from idds.orm.base.session import read_session, transactional_session
from idds.orm import events as orm_events


@transactional_session
def add_event(event, session=None):
    """
    Add an event to be submitted asynchronously to a command broker.

    :param event: The Event object.
    :param session: The database session.
    """
    return orm_events.add_event(event=event, session=session)


@read_session
def get_events(event_type, event_actual_id, status=None, session=None):
    """
    Get events

    :param event_type: event type.
    :param event_actual_id: event actual id.
    :param status: event status.
    """
    return orm_events.get_events(event_type=event_type, event_actual_id=event_actual_id,
                                 status=status, session=session)


@read_session
def get_event_priority(event_type, event_actual_id, session=None):
    """
    Get event priority

    :param event_type: event type.
    :param event_actual_id: event actual id.
    """
    return orm_events.get_event_priority(event_type=event_type, event_actual_id=event_actual_id, session=session)


@transactional_session
def update_event(event_id, status, session=None):
    """
    Update event

    :param event_id: event id.
    :param status: Event status.
    """
    return orm_events.update_event(evnet_id=event_id, status=status, session=session)


@transactional_session
def get_event_for_processing(event_type, num_events=1, session=None):
    """
    Get event for processing

    :param event_type: event type.
    """
    return orm_events.get_event_for_processing(event_type=event_type, num_events=num_events, session=session)


@transactional_session
def delete_event(event_id, session=None):
    """
    Delete event with the given id.

    :param event_id: The event id.
    """
    return orm_events.delete_event(event_id=event_id, session=session)


@transactional_session
def add_event_archive(event, session=None):
    """
    Add an event to the archive.

    :param event: The Event object.
    :param session: The database session.
    """
    return orm_events.add_event_archive(event=event, session=session)


@transactional_session
def clean_event(event, to_archive=True, session=None):
    return orm_events.clean_event(event=event, to_archive=to_archive, session=session)


@transactional_session
def fail_event(event, to_archive=True, session=None):
    return orm_events.fail_event(event=event, to_archive=to_archive, session=session)
