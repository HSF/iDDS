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
operations related to throttler.
"""

import re

from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.common.constants import ThrottlerStatus
from idds.orm.base import models
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_throttler(site, status=ThrottlerStatus.Active, num_requests=None, num_transforms=None, num_processings=None, new_contents=None,
                  queue_contents=None, others=None, session=None):
    """
    Add a throttler item

    :param site: The site name.
    :param session: The database session.
    """

    try:
        old_throttlers = get_throttlers(site=site, session=session)

        if old_throttlers:
            old_throttler = old_throttlers[0]
            parameters = {}
            if status is not None:
                parameters['status'] = status
            if num_requests is not None:
                parameters['num_requests'] = num_requests
            if num_transforms is not None:
                parameters['num_transforms'] = num_transforms
            if num_processings is not None:
                parameters['num_processings'] = num_processings
            if new_contents is not None:
                parameters['new_contents'] = new_contents
            if queue_contents is not None:
                parameters['queue_contents'] = queue_contents
            if others is not None:
                parameters['others'] = others
            update_throttler(throttler_id=old_throttler['throttler_id'], parameters=parameters, session=session)
            return old_throttler['throttler_id']
        else:
            throttler = models.Throttler(site=site,
                                         status=status,
                                         num_requests=num_requests,
                                         num_transforms=num_transforms,
                                         num_processings=num_processings,
                                         new_contents=new_contents,
                                         queue_contents=queue_contents,
                                         others=others)
            throttler.save(session=session)
            return throttler.throttler_id
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for content: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist throttler, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist throttler: %s' % str(e))
    return None


@read_session
def get_throttlers(site=None, status=None, session=None):
    """
    Get throttler

    :param site: site name.
    :param status: throttler status.
    """
    try:
        if status and not isinstance(status, (list, tuple)):
            status = [status]
        if status and len(status) == 1:
            status = [status[0], status[0]]

        query = session.query(models.Throttler)
        if site:
            query = query.filter_by(site=site)
        if status:
            query = query.filter(models.Throttler.status.in_(status))

        tmp = query.all()
        throttlers = []
        if tmp:
            for t in tmp:
                throttlers.append(t.to_dict())
        return throttlers
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist throttler, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist throttler: %s' % str(e))
    return None


@transactional_session
def update_throttler(throttler_id=None, site=None, parameters=None, session=None):
    """
    Update throttler

    :param throttler_id: throttler id.
    :param parameters: parameters in dict.
    """
    try:
        query = session.query(models.Throttler)
        if throttler_id is None and site is None:
            raise exceptions.DatabaseException("Could not update database with both throttler_id and site None")

        if throttler_id:
            query = query.filter_by(throttler_id=throttler_id)
        if site:
            query = query.filter_by(site=site)
        query.update(parameters, synchronize_session=False)
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist throttler, content too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist throttler: %s' % str(e))


@transactional_session
def delete_throttler(throttler_id, session=None):
    """
    Delete throttler with the given id.

    :param throttler_id: The throttler id.
    """
    try:
        session.query(models.Throttler).filter_by(throttler_id=throttler_id).delete()
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)
