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

from idds.common.constants import ThrottlerStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm import throttlers as orm_throttlers


@transactional_session
def add_throttler(site, status=ThrottlerStatus.Active, num_requests=None, num_transforms=None, num_processings=None, new_contents=None,
                  queue_contents=None, others=None, session=None):
    """
    Add a throttler item

    :param site: The site name.
    :param session: The database session.
    """
    return orm_throttlers.add_throttler(site=site, status=status, num_requests=num_requests, num_transforms=num_transforms,
                                        num_processings=num_processings, new_contents=new_contents, queue_contents=queue_contents,
                                        others=others, session=session)


@read_session
def get_throttlers(site=None, status=None, session=None):
    """
    Get throttler

    :param site: site name.
    :param status: throttler status.
    """
    return orm_throttlers.get_throttlers(site=site, status=status, session=session)


@transactional_session
def update_throttler(throttler_id=None, site=None, parameters=None, session=None):
    """
    Update throttler

    :param throttler_id: throttler id.
    :param parameters: parameters in dict.
    """
    return orm_throttlers.update_throttler(throttler_id=throttler_id, site=site, parameters=parameters, session=session)


@transactional_session
def delete_throttler(throttler_id, session=None):
    """
    Delete throttler with the given id.

    :param throttler_id: The throttler id.
    """
    return orm_throttlers.delete_throttler(throttler_id=throttler_id, session=session)
