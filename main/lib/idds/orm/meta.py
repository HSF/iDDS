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
operations related to Health.
"""

import datetime
import re

from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.common.constants import MetaStatus
from idds.orm.base import models
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

    try:
        to_update = {'updated_at': datetime.datetime.utcnow()}
        if status:
            to_update['status'] = status
        if description:
            to_update['description'] = description
        if meta_info:
            to_update['meta_info'] = meta_info
        counts = session.query(models.MetaInfo)\
                        .filter(models.MetaInfo.name == name)\
                        .update(to_update)
        if not counts:
            new_item = models.MetaInfo(name=name, status=status, description=description, meta_info=meta_info)
            new_item.save(session=session)
    except IntegrityError as e:
        if re.match('.*ORA-00001.*', e.args[0]) or re.match('.*unique constraint.*', e.args[0]):
            print("unique constraintviolated: %s" % str(e))
    except DatabaseError as e:
        raise exceptions.DatabaseException('Could not persist meta info: %s' % str(e))


@read_session
def get_meta_item(name, session=None):
    """
    Retrieve meta item.

    :param name: The meta name.
    :param session: The database session.

    :returns metainfo: dictionary of meta info
    """
    try:
        query = session.query(models.MetaInfo)
        query = query.filter(models.MetaInfo.name == name)

        ret = query.first()
        if not ret:
            return None
        else:
            return ret.to_dict()
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@read_session
def get_meta_items(session=None):
    """
    Retrieve meta items.

    :param session: The database session.

    :returns metainfo: List of dictionaries
    """
    items = []
    try:
        query = session.query(models.MetaInfo)

        tmp = query.all()
        if tmp:
            for t in tmp:
                items.append(t.to_dict())
        return items
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)
