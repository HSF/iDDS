#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


"""
operations related to Health.
"""

import datetime
import re

from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.orm.base import models
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_health_item(agent, hostname, pid, thread_id, thread_name, payload, session=None):
    """
    Add a health item.

    :param agent: The agent name.
    :param hostname: The hostname.
    :param pid: The pid.
    :param thread_id: The thread id.
    :param thread_name: The thread name.
    :param payload: The payload.
    :param session: The database session.
    """

    try:
        counts = session.query(models.Health)\
                        .filter(models.Health.agent == agent)\
                        .filter(models.Health.hostname == hostname)\
                        .filter(models.Health.pid == pid)\
                        .filter(models.Health.thread_id == thread_id)\
                        .update({'updated_at': datetime.datetime.utcnow(),
                                 'payload': payload})
        if not counts:
            new_h = models.Health(agent=agent, hostname=hostname, pid=pid,
                                  thread_id=thread_id, thread_name=thread_name,
                                  payload=payload)
            new_h.save(session=session)
    except IntegrityError as e:
        if re.match('.*ORA-00001.*', e.args[0]) or re.match('.*unique constraint.*', e.args[0]):
            print("unique constraintviolated: %s" % str(e))
    except DatabaseError as e:
        raise exceptions.DatabaseException('Could not persist message: %s' % str(e))


@read_session
def retrieve_health_items(session=None):
    """
    Retrieve health items.

    :param session: The database session.

    :returns healths: List of dictionaries
    """
    items = []
    try:
        query = session.query(models.Health)

        tmp = query.all()
        if tmp:
            for t in tmp:
                items.append(t.to_dict())
        return items
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@transactional_session
def clean_health(older_than=3600, hostname=None, pids=[], session=None):
    """
    Clearn items which is older than the time.

    :param older_than in seconds
    """

    query = session.query(models.Health)
    if older_than:
        query = query.filter(models.Health.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=older_than))
    if hostname:
        query = query.filter(models.Health.hostname == hostname)
    if pids:
        query = query.filter(models.Health.pid.in_(pids))
    query.delete()


@transactional_session
def update_health_item_status(item, status, session=None):
    session.query(models.Health)\
           .filter(models.Health.agent == item['agent'])\
           .filter(models.Health.hostname == item['hostname'])\
           .filter(models.Health.pid == item['pid'])\
           .filter(models.Health.thread_id == item['thread_id'])\
           .update({'status': status, 'updated_at': item['updated_at']})
