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
operations related to Conditions.
"""

import re
import datetime

from sqlalchemy.exc import DatabaseError, IntegrityError

from idds.common import exceptions
from idds.common.constants import ConditionStatus
from idds.orm.base import models
from idds.orm.base.session import read_session, transactional_session


@transactional_session
def add_condition(request_id, internal_id, status=ConditionStatus.WaitForTrigger,
                  substatus=None, is_loop=False, loop_index=None, cloned_from=None,
                  evaluate_result=None, previous_transforms=None, following_transforms=None,
                  condition=None, session=None):
    """
    Add a condition.

    :param request_id: The request id.
    :param intenal_id: The internal id.
    :param status: The status about the condition.
    :param substatus: The substatus about the condition.
    :param is_loop: Whether it's a loop condition.
    :param loop_index: The loop index if it's a loop.
    :param cloned_from: The original condition if it's a loop.
    :param evaluate_result: The condition's evaluated result.
    :param previous_transforms: The previous transforms which can trigger this condition.
    :param following_transorms: The following transforms which will be triggered.
    :param condition: The condition function.
    :param session: The database session.
    """

    try:
        cond = models.Condition(request_id=request_id, internal_id=internal_id,
                                status=status, substatus=substatus, is_loop=is_loop,
                                loop_index=loop_index, cloned_from=cloned_from,
                                evaluate_result=evaluate_result,
                                previous_transforms=previous_transforms,
                                following_transforms=following_transforms,
                                condition=condition)

        cond.save(session=session)
        cond_id = cond.condition_id
        return cond_id
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for condition: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist condition, condition too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist condition: %s' % str(e))


@transactional_session
def update_condition(condition_id, parameters, session=None):
    """
    Update condition.

    :param condition_id: The condition id.
    :param parameters: Parameters as a dict.
    :param session: The database session.
    """

    try:
        parameters['updated_at'] = datetime.datetime.utcnow()
        session.query(models.Condition).filter_by(condition_id=condition_id)\
               .update(parameters, synchronize_session=False)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for condition: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist condition, condition too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist condition: %s' % str(e))


@transactional_session
def update_conditions(conditions, session=None):
    """
    Update conditions.

    :param conditions: Condtions as a list of dict.
    :param session: The database session.
    """

    try:
        session.bulk_update_mappings(models.Conidition, conditions)
    except TypeError as e:
        raise exceptions.DatabaseException('Invalid JSON for condition: %s' % str(e))
    except DatabaseError as e:
        if re.match('.*ORA-12899.*', e.args[0]) \
           or re.match('.*1406.*', e.args[0]):
            raise exceptions.DatabaseException('Could not persist condition, condition too large: %s' % str(e))
        else:
            raise exceptions.DatabaseException('Could not persist condition: %s' % str(e))


@read_session
def retrieve_conditions(request_id, internal_id=None, status=None, session=None):
    """
    Retrieve conditions

    :param request_id: The request id.
    :param intenal_id: The internal id.
    :param status: The status about the condition.
    :param session: The database session.

    :returns command: List of conditions
    """
    conditions = []
    try:
        query = session.query(models.Condition)

        if request_id is not None:
            query = query.filter_by(request_id=request_id)
        if internal_id is not None:
            query = query.filter_by(internal_id=internal_id)
        if status is not None:
            query = query.filter_by(status=status)

        tmp = query.all()
        if tmp:
            for t in tmp:
                conditions.append(t.to_dict())
        return conditions
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)


@transactional_session
def delete_conditions(request_id=None, internal_id=None, session=None):
    """
    Delete all conditions with the given IDs.

    :param request_id: The request id.
    :param intenal_id: The internal id.
    :param session: The database session.
    """
    try:
        query = session.query(models.Condition)

        if request_id is not None:
            query = query.filter_by(request_id=request_id)
        if internal_id is not None:
            query = query.filter_by(internal_id=internal_id)

        query.delete(synchronize_session=False)
    except IntegrityError as e:
        raise exceptions.DatabaseException(e.args)
