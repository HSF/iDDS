Workflow: Examples
=============================

iDDS examples for subworkflow and loopworkflow.
(example: main/lib/idds/tests/test_workflow_condition_v2.py)

Loop workflow
~~~~~~~~~~~~~~~~~~~~~~~~

Here is a simple example of loop workflow.

.. code-block:: python

    from idds.workflowv2.work import Work, WorkStatus
    from idds.workflowv2.workflow import (CompositeCondition, AndCondition, OrCondition,
                                          Condition, ConditionTrigger, Workflow, ParameterLink)

    work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
    work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

    workflow = Workflow()
    workflow.add_work(work1, initial=False)
    workflow.add_work(work2, initial=False)

    cond = Condition(cond=work2.is_finished)
    workflow.add_loop_condition(cond)

    # custom_condition
    # iDDS will check to_contiue status.
    # With ATLASLocalPanDAWork, the output will be parsed. If 'to_continue' is in the output, it will be used.
    work2.add_custom_condition(key='to_continue', value=True)
    cond = Condition(work2.get_custom_condition_status)
    workflow.add_loop_condition(cond1)

    # multiple custom_condition
    # to_continue and to_continue1
    work2.add_custom_condition(key='to_continue', value=True, op='and')
    work2.add_custom_condition(key='to_continue1', value=True, op='and')
    cond = Condition(work2.get_custom_condition_status)
    workflow.add_loop_condition(cond1)

    # multiple custom_condition
    # (to_continue and to_continue1) or to_exit or to_exit1
    work2.add_custom_condition(key='to_continue', value=True, op='and')
    work2.add_custom_condition(key='to_continue1', value=True, op='and')
    work2.add_custom_condition(key='to_exit', value=False, op='or')
    work2.add_custom_condition(key='to_exit1', value=False, op='or')
    cond = Condition(work2.get_custom_condition_status)
    workflow.add_loop_condition(cond1)

Sub workflow
~~~~~~~~~~~~~~~~~~~~~~~

Here is a simple example of sub workflow.

.. code-block:: python

    from idds.workflowv2.work import Work, WorkStatus
    from idds.workflowv2.workflow import (CompositeCondition, AndCondition, OrCondition,
                                          Condition, ConditionTrigger, Workflow, ParameterLink)

    work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
    work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

    workflow1 = Workflow()
    workflow1.add_work(work1, initial=False)
    workflow1.add_work(work2, initial=False)

    work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)

    workflow = Workflow()
    workflow.add_work(work3, initial=False)
    workflow.add_work(workflow1, initial=False)

Sub loop workflow with ParameterLinks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is a simple example of sub loop workflow with parameter links.

.. code-block:: python

    from idds.workflowv2.work import Work, WorkStatus
    from idds.workflowv2.workflow import (CompositeCondition, AndCondition, OrCondition,
                                          Condition, ConditionTrigger, Workflow, ParameterLink)

    work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1,
                 primary_input_collection={'scope': 'test_scop', 'name': 'input_test_work_1'},
                 primary_output_collection={'scope': 'test_scop', 'name': 'output_test_work_1'})
    work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2,
                 primary_input_collection={'scope': 'test_scop', 'name': 'input_test_work_2'},
                 primary_output_collection={'scope': 'test_scop', 'name': 'output_test_work_2'})

    workflow1 = Workflow()
    workflow1.add_work(work1, initial=False)
    workflow1.add_work(work2, initial=False)

    cond1 = Condition(cond=work1.is_finished, true_work=work2)
    workflow1.add_condition(cond1)

    p_link = ParameterLink(parameters=[{'source': 'primary_output_collection',
                                        'destination': 'primary_input_collection'}])
    workflow1.add_parameter_link(work1, work2, p_link)

    cond = Condition(cond=work2.is_finished)
    workflow1.add_loop_condition(cond)

    work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3,
                 primary_input_collection={'scope': 'test_scop', 'name': 'input_test_work_3'},
                 primary_output_collection={'scope': 'test_scop', 'name': 'output_test_work_3'})
    cond2 = Condition(cond=work3.is_finished, true_work=workflow1)
    p_link1 = ParameterLink(parameters=[{'source': 'primary_output_collection',
                                         'destination': 'primary_input_collection'}])

    workflow = Workflow()
    workflow.add_work(work3, initial=False)
    workflow.add_work(workflow1, initial=False)
    workflow.add_condition(cond2)
    workflow.add_parameter_link(work3, work1, p_link1)


Workflow with global parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is a simple example of workflow with global parameters. When a work starts, the work will use the global parameters and call 'setattr' to set the attributes for this work. When a work terminates, idds will call getattr to get the values for global parameters and store them in the global parameters.
However, to avoid the global parameters overwrite the work's private attributes, currently only parameters start with 'user_' will be accepted as global parameters. Other parameters will be ignored with a warning logging messages.

.. code-block:: python

    from idds.workflowv2.work import Work, WorkStatus
    from idds.workflowv2.workflow import (CompositeCondition, AndCondition, OrCondition,
                                          Condition, ConditionTrigger, Workflow, ParameterLink)

    work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1,
                 primary_input_collection={'scope': 'test_scop', 'name': 'input_test_work_1'},
                 primary_output_collection={'scope': 'test_scop', 'name': 'output_test_work_1'})
    work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2,
                 primary_input_collection={'scope': 'test_scop', 'name': 'input_test_work_2'},
                 primary_output_collection={'scope': 'test_scop', 'name': 'output_test_work_2'})

    workflow1 = Workflow()
    workflow1.add_work(work1, initial=False)
    workflow1.add_work(work2, initial=False)

    # global paraeters
    workflow1.set_global_parameters({'user_attr1': 1, 'user_attr2': 2})

    # sliced global parameters
    workflow1.set_global_parameters({'user_attr': [1, 2, 3]})
    workflow1.set_sliced_global_parameters(source='user_attr', index=0)
    workflow1.set_sliced_global_parameters(source='user_attr', index=1, name='user_myattr')


