Conditions: Examples
=============================

iDDS provides composite conditions to support complicated workflows.

conditions
~~~~~~~~~~~~~~~~~~~~~~~~

1. condtions

Here are conditions iDDS supports

.. code-block:: python

    from idds.workflow.work import Work, WorkStatus
    from idds.workflow.workflow import (CompositeCondition, AndCondition, OrCondition, Condition,
                                        ConditionOperator, ConditionTrigger, Workflow)

    com_cond = CompositeCondition(operator=ConditionOperator.And, conditions=[], true_works=[], false_works=[])

    and_cond = AndCondition(conditions=[], true_works=[], false_works=[])
             = CompositeCondition(operator=ConditionOperator.And, conditions=[], true_works=[], false_works=[])

    or_cond = OrCondition(conditions=[], true_works=[], false_works=[])
            = CompositeCondition(operator=ConditionOperator.Or, conditions=[], true_works=[], false_works=[])

    # To support old conditions
    cond = Condition(cond=<cond>, true_work=<true_work>, false_work=<false_work>)
         = CompositeCondition(operator=ConditionOperator.And, conditions=[cond], true_works=[true_work], false_works=[false_work])


    # combine multiple condtions into one condition
    and_cond = AndCondition(conditions=[work5.is_terminated], true_works=[work6])
    or_cond = OrCondition(conditions=[work7.is_terminated, work8.is_terminated], true_works=[work9])
    conds = AndCondition(conditions=[work1.is_finished, work2.is_started],
                         true_works=[work3, or_cond], false_works=[work4, and_cond])
    # for combined conditions, only need to add the top(top tree) condition to the workflow.
    # Since every add_condition will create an evaluation entrypoint in iDDS.
    #  If a branch of a combined condition is added to a workflow with add_condition, this branch will be evaluated as a separate condition tree.
    workflow.add_condition(conds)


2. condition trigger

(This part is for iDDS developers. Users normally should not use this function.)
Condition trigger is an option for iDDS to process whether to remember whether a condition work is already triggered. It's used to avoid duplicated triggering some processes(For example, when using work1.is_started to trigger work2. If the condition is not recorded, work2 can be triggered many times every time when the condition is evaluated).

.. code-block:: python

   class ConditionTrigger(IDDSEnum):
        NotTriggered = 0
        ToTrigger = 1
        Triggered = 2

    # ToTrigger will return untriggered works based on the conditions and mark the work as triggered.
    # exception: if the work.is_template is true, even the condition work is marked as triggered, the work will still be triggered. So for cases such as work.is_started should not be used as a condition for works with is_template=True.
    cond.get_next_works(trigger=ConditionTrigger.ToTrigger)

    # Will only return untriggered works based on conditions. It will not update the trigger status.
    cond.get_next_works(trigger=ConditionTrigger.NotTriggered)

    # Will only return triggered works based on conditions. It will not update the trigger status.
    cond.get_next_works(trigger=ConditionTrigger.Triggered)
