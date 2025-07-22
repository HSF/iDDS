#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025


"""
operations related to Health.
"""

import datetime
import re

from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.common.constants import RequestGroupType, RequestGroupStatus, RequestGroupLocking
from idds.orm.base import models
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_request_group(campaign=None, campaign_scope=None, campaign_group=None, campaign_tag=None, requester=None,
                      username=None, userdn=None, priority=0, group_type=RequestGroupType.Workflow,
                      status=RequestGroupStatus.New, locking=RequestGroupLocking.Idle, lifetime=None,
                      new_retries=0, update_retries=0, max_new_retries=3, max_update_retries=0,
                      new_poll_period=1, update_poll_period=10, group_metadata=None, processing_metadata=None,
                      max_processing_requests=-1, session=None):
    """
    Add a request group.

    :param campaign: The campaign name.
    :param campaign_scope: The campaign scope.
    :param campaign_group: The campaign group.
    :param campaign_tag: The campaign tag.
    :param requester: The requester name.
    :param username: The user name.
    :param userdn: The user dn.
    :param priority: The priority.
    :param group_type: The group type.
    :param status: The request status as integer.
    :param group_metadata: The metadata as json.
    :param processing_metadata: The metadata as json.
    :param session: The database session.

    :returns group id.
    """

    try:
        if lifetime:
            expired_at = datetime.datetime.utcnow() + datetime.timedelta(days=lifetime)
        else:
            expired_at = None

        if new_poll_period:
            new_poll_period = datetime.timedelta(seconds=new_poll_period)
        if update_poll_period:
            update_poll_period = datetime.timedelta(seconds=update_poll_period)

        new_group = models.RequestGroup(campaign=campaign, campaign_scope=campaign_scope, campaign_group=campaign_group,
                                        campaign_tag=campaign_tag, requester=requester, username=username, userdn=userdn,
                                        priority=priority, group_type=group_type, status=status, locking=locking,
                                        expired_at=expired_at, new_retries=new_retries, update_retries=update_retries,
                                        max_new_retries=max_new_retries, max_update_retries=max_update_retries,
                                        new_poll_period=new_poll_period, update_poll_period=update_poll_period,
                                        group_metadata=group_metadata, processing_metadata=processing_metadata,
                                        max_processing_requests=max_processing_requests)

        new_group.save(session=session)
        group_id = new_group.group_id
        return group_id
    except IntegrityError as e:
        if re.match('.*ORA-00001.*', e.args[0]) or re.match('.*unique constraint.*', e.args[0]):
            print("unique constraintviolated: %s" % str(e))
    except DatabaseError as e:
        raise exceptions.DatabaseException('Could not persist request group info: %s' % str(e))


@read_session
def get_request_groups(campaign=None, campaign_scope=None, campaign_group=None, campaign_tag=None, group_id=None, session=None):
    """
    Retrieve request groups.

    :param campaign: The campaign name.
    :param campaign_scope: The campaign scope.
    :param campaign_group: The campaign group.
    :param campaign_tag: The campaign tag.
    :param group_id: The group id.
    :param session: The database session.

    :returns request groups: list of request groups
    """
    try:
        query = session.query(models.RequestGroup)

        if group_id is not None:
            query = query.filter(models.RequestGroup.group_id == group_id)

        if campaign:
            query = query.filter(models.RequestGroup.campaign == campaign)
        if campaign_scope:
            query = query.filter(models.RequestGroup.campaign_scope == campaign_scope)
        if campaign_group:
            query = query.filter(models.RequestGroup.campaign_group == campaign_group)
        if campaign_tag:
            query = query.filter(models.RequestGroup.campaign_tag == campaign_tag)

        tmp = query.all()
        rets = []
        if tmp:
            for t in tmp:
                rets.append(t.to_dict())
        return rets
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)
