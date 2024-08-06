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


from idds.common.constants import ConditionStatus
from idds.orm.base.session import read_session, transactional_session
from idds.orm import conditions as orm_conditions


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

    cond_id = orm_conditions.add_condition(request_id=request_id, internal_id=internal_id,
                                           status=status, substatus=substatus, is_loop=is_loop,
                                           loop_index=loop_index, cloned_from=cloned_from,
                                           evaluate_result=evaluate_result,
                                           previous_transforms=previous_transforms,
                                           following_transforms=following_transforms,
                                           condition=condition,
                                           session=session)
    return cond_id


@transactional_session
def update_condition(condition_id, parameters, session=None):
    """
    Update condition.

    :param condition_id: The condition id.
    :param parameters: Parameters as a dict.
    :param session: The database session.
    """
    orm_conditions.update_condition(condition_id=condition_id, parameters=parameters, session=session)


@transactional_session
def update_conditions(conditions, session=None):
    """
    Update conditions.

    :param conditions: Condtions as a list of dict.
    :param session: The database session.
    """
    orm_conditions.update_conditions(conditions=conditions, session=session)


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
    conds = orm_conditions.retrieve_conditions(request_id=request_id, internal_id=internal_id,
                                               status=status, session=session)
    return conds


@transactional_session
def delete_conditions(request_id=None, internal_id=None, session=None):
    """
    Delete all conditions with the given IDs.

    :param request_id: The request id.
    :param intenal_id: The internal id.
    :param session: The database session.
    """
    orm_conditions.delete_condtions(request_id=request_id, internal_id=internal_id, session=session)
