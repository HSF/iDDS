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
from idds.common import exceptions
from idds.orm.base.session import read_session, transactional_session
from idds.orm import throttlers as orm_throttlers
from idds.core.rate_limit_manager import get_rate_limit_manager, RateLimitError



@transactional_session
def add_throttler(site, status=ThrottlerStatus.Active, num_requests=None, num_transforms=None, num_processings=None, new_contents=None,
                  queue_contents=None, max_requests_per_minute=100, max_requests_per_hour=10000, max_burst=50,
                  priority_level=5, vo_name=None, others=None, session=None):
    """
    Add a throttler item

    :param site: The site name.
    :param status: The throttler status.
    :param num_requests: Number of requests.
    :param num_transforms: Number of transforms.
    :param num_processings: Number of processings.
    :param new_contents: New contents count.
    :param queue_contents: Queue contents count.
    :param max_requests_per_minute: Maximum requests per minute for rate limiting.
    :param max_requests_per_hour: Maximum requests per hour for rate limiting.
    :param max_burst: Maximum burst capacity.
    :param priority_level: Priority level (1-10, higher = more priority).
    :param vo_name: Virtual Organization name.
    :param others: Other parameters.
    :param session: The database session.
    """
    return orm_throttlers.add_throttler(site=site, status=status, num_requests=num_requests, num_transforms=num_transforms,
                                        num_processings=num_processings, new_contents=new_contents, queue_contents=queue_contents,
                                        max_requests_per_minute=max_requests_per_minute, max_requests_per_hour=max_requests_per_hour,
                                        max_burst=max_burst, priority_level=priority_level, vo_name=vo_name,
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


def check_rate_limit(site=None, user_id=None, vo_id=None, tokens=1):
    """
    Check if a request is within rate limits.
    
    :param site: Site name for throttler lookup
    :param user_id: User identifier (for per-user limiting)
    :param vo_id: VO identifier (for per-VO limiting)
    :param tokens: Number of tokens needed
    :return: (allowed: bool, headers: dict, error_message: str or None)
    
    :raises RateLimitError: If rate limit exceeded
    """
    manager = get_rate_limit_manager()
    
    try:
        headers = manager.check_request(tokens=tokens, user_id=user_id, vo_id=vo_id)
        return (True, headers, None)
    except RateLimitError as e:
        return (False, {}, str(e))


def get_rate_limit_status(site=None, user_id=None, vo_id=None):
    """
    Get rate limit statistics for a site/user/VO.
    
    :param site: Site name
    :param user_id: User identifier
    :param vo_id: VO identifier
    :return: Dictionary with rate limit statistics
    """
    manager = get_rate_limit_manager()
    return manager.get_stats(user_id=user_id, vo_id=vo_id)
