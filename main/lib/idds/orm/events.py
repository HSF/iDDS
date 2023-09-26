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

import re
import datetime

from sqlalchemy.exc import DatabaseError, IntegrityError, NoResultFound
from sqlalchemy.sql.expression import asc, desc

from idds.common import exceptions
from idds.common.event import EventStatus
from idds.orm.base import models
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_event(event, session=None):
    """
    Add an event to be submitted asynchronously to a command broker.

    :param event: The Event object.
    :param session: The database session.
    """

    try:
        old_events = get_events(event_type=event._event_type, event_actual_id=event.get_event_id(),
                                status=EventStatus.New, session=session)
        merge = False
        for old_event_db in old_events:
            old_event = old_event_db['content']['event']
            if old_event.able_to_merge(event):
                # discard current event
                old_event.merge(event)
                if old_event.changed():
                    old_event_db['content']['event'] = old_event
                    update_event(old_event.event_id, status=EventStatus.New, session=session)
                merge = True
                return None
        if not merge:
            priority = get_event_priority(event_type=event._event_type,
                                          event_actual_id=event.get_event_id(),
                                          session=session)
            event_db = models.Event(event_type=event._event_type,
                                    event_actual_id=event.get_event_id(),
                                    status=EventStatus.New,
                                    priority=priority,
                                    content={'event': event})
            event_db.save(session=session)
            return event_db.event_id
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))
    return None


@read_session
def get_events(event_type, event_actual_id, status=None, session=None):
    """
    Get events

    :param event_type: event type.
    :param event_actual_id: event actual id.
    :param status: event status.
    """
    try:
        if not isinstance(status, (list, tuple)):
            status = [status]
        if len(status) == 1:
            status = [status[0], status[0]]

        query = session.query(models.Event)
        if event_type:
            query = query.filter_by(event_type=event_type)
        if event_actual_id:
            query = query.filter_by(event_actual_id=event_actual_id)
        if status:
            query = query.filter(models.Event.status.in_(status))

        tmp = query.all()
        events = []
        if tmp:
            for t in tmp:
                events.append(t.to_dict())
        return events
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))
    return None


@transactional_session
def add_event_priority(event_type, event_actual_id, priority=10, session=None):
    """
    add event priority

    :param event_type: event type.
    :param event_actual_id: event actual id.
    """
    try:
        event_pr = models.EventPriority(event_type=event_type,
                                        event_actual_id=event_actual_id,
                                        priority=priority,
                                        last_processed_at=datetime.datetime.utcnow(),
                                        updated_at=datetime.datetime.utcnow())
        event_pr.save(session=session)
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))


@transactional_session
def update_event_priority(event_type, event_actual_id, priority=10, session=None):
    """
    Update event priority

    :param event_type: event type.
    :param event_actual_id: event actual id.
    """
    try:
        parameters = {'priority': priority,
                      'last_processed_at': datetime.datetime.utcnow(),
                      'updated_at': datetime.datetime.utcnow()}
        query = session.query(models.EventPriority)
        if event_type:
            query = query.filter_by(event_type=event_type)
        if event_actual_id:
            query = query.filter_by(event_actual_id=event_actual_id)
        row_count = query.update(parameters, synchronize_session=False)

        if row_count < 1:
            add_event_priority(event_type=event_type, event_actual_id=event_actual_id,
                               priority=priority, session=session)
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))
    return None


@read_session
def get_event_priority(event_type, event_actual_id, session=None):
    """
    Get event priority

    :param event_type: event type.
    :param event_actual_id: event actual id.
    """
    try:
        query = session.query(models.EventPriority)
        if event_type:
            query = query.filter_by(event_type=event_type)
        if event_actual_id:
            query = query.filter_by(event_actual_id=event_actual_id)

        tmp = query.first()
        if tmp:
            t = tmp.to_dict()
            time_diff = datetime.datetime.utcnow() - t['last_processed_at']
            priority = time_diff.total_seconds()
        else:
            priority = 3600 * 24 * 7
        return priority
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))
    return 10


@transactional_session
def update_event(event_id, status, session=None):
    """
    Update event

    :param event_id: event id.
    :param status: Event status.
    """
    try:
        parameters = {'status': status}
        if status == EventStatus.Processing:
            parameters['processing_at'] = datetime.datetime.utcnow()
        if status == EventStatus.Processed:
            parameters['processed_at'] = datetime.datetime.utcnow()
        session.query(models.Event).filter_by(event_id=event_id)\
               .update(parameters, synchronize_session=False)
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))


@transactional_session
def get_event_for_processing(event_type, num_events=1, session=None):
    """
    Get event for processing

    :param event_type: event type.
    """
    try:
        query = session.query(models.Event)
        if event_type:
            query = query.filter_by(event_type=event_type)
        status = [EventStatus.New, EventStatus.New]
        query = query.filter(models.Event.status.in_(status))
        query = query.order_by(desc(models.Event.priority))
        query = query.order_by(asc(models.Event.event_id))

        tmp = query.all()
        events = []
        if tmp:
            i = 0
            for event in tmp:
                # event = tmp.to_dict()
                # event = tmp
                session.expunge(event)
                update_event_priority(event.event_type, event.get_event_id(), session=session)
                update_event(event.event_id, status=EventStatus.Processing, session=session)
                events.append(event)
                i += 1
                if i >= num_events:
                    break
        return events
    except NoResultFound as _:     # noqa F841
        return []
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))
    return []


@transactional_session
def delete_event(event_id, session=None):
    """
    Delete event with the given id.

    :param event_id: The event id.
    """
    try:
        session.query(models.Event).filter_by(event_id=event_id).delete()
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@transactional_session
def add_event_archive(event, session=None):
    """
    Add an event to the archive.

    :param event: The Event object.
    :param session: The database session.
    """

    try:
        event_db = models.EventArchive(event_id=event.event_id,
                                       event_type=event.event_type,
                                       event_actual_id=event.event_actual_id,
                                       status=event.status,
                                       priority=event.priority,
                                       created_at=event.created_at,
                                       processing_at=event.processing_at,
                                       processed_at=event.processed_at,
                                       content=event.content)
        event_db.save(session=session)
        return event_db.event_id
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist event, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist event: %s' % str(e))
    return None


@transactional_session
def clean_event(event, to_archive=True, session=None):
    event.status = EventStatus.Processed
    event.processed_at = datetime.datetime.utcnow()
    delete_event(event.event_id, session=session)
    if to_archive:
        add_event_archive(event, session=session)


@transactional_session
def fail_event(event, to_archive=True, session=None):
    event.status = EventStatus.Failed
    event.processed_at = datetime.datetime.utcnow()
    delete_event(event.event_id, session=session)
    if to_archive:
        add_event_archive(event, session=session)
