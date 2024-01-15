#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


"""
operations related to Meta info.
"""

from idds.common.constants import MetaStatus
from idds.orm import meta as orm_meta
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_meta_item(name, status=MetaStatus.Active, description=None, meta_info=None, session=None):
    """
    Add a meta item.

    :param name: The meta name.
    :param status: The meta status.
    :param description: The meta description.
    :param meta_info: The metadata.
    :param session: The database session.
    """
    return orm_meta.add_meta_item(name=name, status=status, description=description,
                                  meta_info=meta_info, session=session)


@read_session
def get_meta_item(name, session=None):
    """
    Retrieve meta item.

    :param name: The meta name.
    :param session: The database session.

    :returns metainfo: dictionary of meta info
    """
    orm_meta.get_meta_item(name=name, session=session)


@read_session
def get_meta_items(session=None):
    """
    Retrieve meta items.

    :param session: The database session.

    :returns metainfo: List of dictionaries
    """
    orm_meta.get_meta_items(session=session)
