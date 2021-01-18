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


from idds.orm import health as orm_health


def add_health_item(agent, hostname, pid, thread_id, thread_name, payload):
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
                                      payload=payload)


def retrieve_health_items():
    """
    Retrieve health items.

    :returns healths: List of dictionaries
    """
    return orm_health.retrieve_health_items()


def clean_health(older_than=3600):
    """
    Clearn items which is older than the time.

    :param older_than in seconds
    """
    orm_health.clean_health(older_than=older_than)
