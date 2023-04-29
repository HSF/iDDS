#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2023


"""
operations related to Health.
"""

import datetime

from idds.common.constants import HealthStatus
from idds.orm import health as orm_health
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
    """
    return orm_health.add_health_item(agent=agent, hostname=hostname, pid=pid,
                                      thread_id=thread_id,
                                      thread_name=thread_name,
                                      payload=payload,
                                      session=session)


@read_session
def retrieve_health_items(session=None):
    """
    Retrieve health items.

    :returns healths: List of dictionaries
    """
    return orm_health.retrieve_health_items(session=session)


@transactional_session
def clean_health(older_than=3600, hostname=None, pids=[], session=None):
    """
    Clearn items which is older than the time.

    :param older_than in seconds
    """
    orm_health.clean_health(older_than=older_than, hostname=hostname, pids=pids, session=session)


@transactional_session
def select_agent(name, newer_than=3600, session=None):
    """
    Select one active receiver.

    :param older_than in seconds to be cleaned.
    """
    orm_health.clean_health(older_than=newer_than, session=session)
    health_items = orm_health.retrieve_health_items(session=session)
    selected_agent = None
    selected_agent_diff = None
    utc_now = datetime.datetime.utcnow()
    for health_item in health_items:
        if health_item['agent'] != name:
            continue

        updated_at = health_item['updated_at']
        time_diff = utc_now - updated_at
        if time_diff.total_seconds() > newer_than:
            continue

        if health_item['status'] == HealthStatus.Active:
            selected_agent = health_item
            break

        if selected_agent is None:
            selected_agent = health_item
            selected_agent_diff = time_diff
        else:
            if time_diff < selected_agent_diff:
                selected_agent = health_item
                selected_agent_diff = time_diff

    if selected_agent:
        if selected_agent['status'] != HealthStatus.Active:
            orm_health.update_health_item_status(selected_agent, status=HealthStatus.Active, session=session)
            selected_agent['status'] = HealthStatus.Active
    for health_item in health_items:
        if health_item['agent'] == name and health_item['status'] == HealthStatus.Active and health_item['health_id'] != selected_agent['health_id']:
            orm_health.update_health_item_status(selected_agent, status=HealthStatus.Default, session=session)
    return selected_agent
