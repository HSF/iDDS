#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021 - 2022


"""
Test workflow condtions.
"""

import unittest2 as unittest
# from nose.tools import assert_equal
from idds.common.utils import setup_logging

from idds.common.utils import json_dumps, json_loads

from idds.workflowv2.work import Work, WorkStatus
from idds.workflowv2.workflow import (CompositeCondition, AndCondition, OrCondition,
                                      Condition, ConditionTrigger, Workflow, ParameterLink)


setup_logging(__name__)


class TestWorkflowCondtion(unittest.TestCase):

    def test_work_custom_condition(self):
        # init_p = Parameter({'input_dataset': 'data17:data17.test.raw.1'})
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work1.add_custom_condition('to_exit', True)
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit = False
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit = 'False'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit = True
        assert(work1.get_custom_condition_status() is True)

        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work1.add_custom_condition('to_exit', 'true')
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit = False
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit = 'False'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit = True
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit = 'true'
        assert(work1.get_custom_condition_status() is True)

        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work1.add_custom_condition('to_exit1', True)
        work1.add_custom_condition('to_exit2', True)
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = False
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = 'False'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = True
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit2 = 'true'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit2 = True
        assert(work1.get_custom_condition_status() is True)

        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work1.add_custom_condition('to_exit1', True, op='or')
        work1.add_custom_condition('to_exit2', True, op='or')
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = False
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = 'False'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = True
        assert(work1.get_custom_condition_status() is True)
        work1.to_exit1 = False
        work1.to_exit2 = 'true'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit2 = True
        assert(work1.get_custom_condition_status() is True)

        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        # to_exit or (to_exit1 or to_exit2)
        work1.add_custom_condition('to_exit', True, op='and')
        work1.add_custom_condition('to_exit1', True, op='or')
        work1.add_custom_condition('to_exit2', True, op='or')
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = False
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = 'False'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit1 = True
        assert(work1.get_custom_condition_status() is True)
        work1.to_exit1 = False
        work1.to_exit2 = 'true'
        assert(work1.get_custom_condition_status() is False)
        work1.to_exit2 = True
        assert(work1.get_custom_condition_status() is True)
        work1.to_exit1 = False
        work1.to_exit2 = False
        work1.to_exit = True
        assert(work1.get_custom_condition_status() is True)
        assert(work1.get_not_custom_condition_status() is False)

    def test_condition(self):
        # init_p = Parameter({'input_dataset': 'data17:data17.test.raw.1'})
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)
        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        work4 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=4)
        work5 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=5)
        work6 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=6)
        work7 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=7,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.raw.1'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work2'}])
        work8 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=8,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.work2'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work3'}])

        workflow = Workflow()
        workflow.add_work(work1, initial=True)
        workflow.add_work(work2, initial=True)
        workflow.add_work(work3, initial=False)
        workflow.add_work(work8, initial=False)

        # CompositeCondition
        cond1 = CompositeCondition(conditions=work1.is_finished, true_works=work2, false_works=work3)
        works = cond1.all_works()
        assert(works == [work1, work2, work3])
        works = cond1.all_pre_works()
        assert(works == [work1])
        works = cond1.all_next_works()
        assert(works == [work2, work3])
        cond_status = cond1.get_condition_status()
        assert(cond_status is False)

        work1.status = WorkStatus.Finished
        cond_status = cond1.get_condition_status()
        assert(cond_status is True)
        work1.status = WorkStatus.New

        works = cond1.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work3])
        work1.status = WorkStatus.Finished
        works = cond1.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work2])
        work1.status = WorkStatus.New

        works = cond1.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work3])
        works = cond1.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond1.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work2])
        works = cond1.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.New

        works = cond1.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work3])
        work1.status = WorkStatus.Finished
        works = cond1.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work2])
        work1.status = WorkStatus.New

        # CompositeCondition
        cond2 = CompositeCondition(conditions=[work1.is_finished, work2.is_finished, work3.is_finished], true_works=[work4, work5], false_works=[work6, work7])

        works = cond2.all_works()
        assert(works == [work1, work2, work3, work4, work5, work6, work7])
        works = cond2.all_pre_works()
        assert(works == [work1, work2, work3])
        works = cond2.all_next_works()
        assert(works == [work4, work5, work6, work7])
        cond_status = cond2.get_condition_status()
        assert(cond_status is False)

        work1.status = WorkStatus.Finished
        cond_status = cond2.get_condition_status()
        assert(cond_status is False)
        work2.status = WorkStatus.Finished
        cond_status = cond2.get_condition_status()
        assert(cond_status is False)
        work3.status = WorkStatus.Finished
        cond_status = cond2.get_condition_status()
        assert(cond_status is True)
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond2.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work6, work7])
        work1.status = WorkStatus.Finished
        work2.status = WorkStatus.Finished
        work3.status = WorkStatus.Finished
        works = cond2.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work4, work5])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond2.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work6, work7])
        works = cond2.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        work2.status = WorkStatus.Finished
        work3.status = WorkStatus.Finished
        works = cond2.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work4, work5])
        works = cond2.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond2.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work6, work7])
        work1.status = WorkStatus.Finished
        work2.status = WorkStatus.Finished
        work3.status = WorkStatus.Finished
        works = cond2.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work4, work5])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        # AndCondition
        cond3 = AndCondition(conditions=[work1.is_finished, work2.is_finished, work3.is_finished], true_works=[work4, work5], false_works=[work6, work7])

        works = cond3.all_works()
        assert(works == [work1, work2, work3, work4, work5, work6, work7])
        works = cond3.all_pre_works()
        assert(works == [work1, work2, work3])
        works = cond3.all_next_works()
        assert(works == [work4, work5, work6, work7])
        cond_status = cond3.get_condition_status()
        assert(cond_status is False)

        work1.status = WorkStatus.Finished
        cond_status = cond3.get_condition_status()
        assert(cond_status is False)
        work2.status = WorkStatus.Finished
        cond_status = cond3.get_condition_status()
        assert(cond_status is False)
        work3.status = WorkStatus.Finished
        cond_status = cond3.get_condition_status()
        assert(cond_status is True)
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond3.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work6, work7])
        work1.status = WorkStatus.Finished
        work2.status = WorkStatus.Finished
        work3.status = WorkStatus.Finished
        works = cond3.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work4, work5])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond3.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work6, work7])
        works = cond3.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        work2.status = WorkStatus.Finished
        work3.status = WorkStatus.Finished
        works = cond3.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work4, work5])
        works = cond3.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond3.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work6, work7])
        work1.status = WorkStatus.Finished
        work2.status = WorkStatus.Finished
        work3.status = WorkStatus.Finished
        works = cond3.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work4, work5])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        # OrCondtion
        cond4 = OrCondition(conditions=[work1.is_finished, work2.is_finished, work3.is_finished], true_works=[work4, work5], false_works=[work6, work7])

        works = cond4.all_works()
        assert(works == [work1, work2, work3, work4, work5, work6, work7])
        works = cond4.all_pre_works()
        assert(works == [work1, work2, work3])
        works = cond4.all_next_works()
        assert(works == [work4, work5, work6, work7])
        cond_status = cond4.get_condition_status()
        assert(cond_status is False)

        work1.status = WorkStatus.Finished
        cond_status = cond4.get_condition_status()
        assert(cond_status is True)
        work1.status = WorkStatus.New
        work2.status = WorkStatus.Finished
        cond_status = cond4.get_condition_status()
        assert(cond_status is True)
        work2.status = WorkStatus.New
        work3.status = WorkStatus.Finished
        cond_status = cond4.get_condition_status()
        assert(cond_status is True)
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond4.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work6, work7])
        work1.status = WorkStatus.Finished
        # work2.status = WorkStatus.Finished
        # work3.status = WorkStatus.Finished
        works = cond4.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work4, work5])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond4.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work6, work7])
        works = cond4.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        # work2.status = WorkStatus.Finished
        # work3.status = WorkStatus.Finished
        works = cond4.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work4, work5])
        works = cond4.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        works = cond4.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work6, work7])
        work1.status = WorkStatus.Finished
        # work2.status = WorkStatus.Finished
        # work3.status = WorkStatus.Finished
        works = cond4.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work4, work5])
        work1.status = WorkStatus.New
        work2.status = WorkStatus.New
        work3.status = WorkStatus.New

        # Condition
        cond5 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)

        works = cond5.all_works()
        assert(works == [work1, work2, work3])
        works = cond5.all_pre_works()
        assert(works == [work1])
        works = cond5.all_next_works()
        assert(works == [work2, work3])
        cond_status = cond5.get_condition_status()
        assert(cond_status is False)

        work1.status = WorkStatus.Finished
        cond_status = cond5.get_condition_status()
        assert(cond_status is True)
        work1.status = WorkStatus.New

        works = cond5.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work3])
        work1.status = WorkStatus.Finished
        works = cond5.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work2])
        work1.status = WorkStatus.New

        works = cond5.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work3])
        works = cond5.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond5.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work2])
        works = cond5.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.New

        works = cond5.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work3])
        work1.status = WorkStatus.Finished
        works = cond5.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work2])
        work1.status = WorkStatus.New

        # multiple conditions
        cond6 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)
        cond7 = CompositeCondition(conditions=[work4.is_finished, work5.is_finished], true_works=[work6, cond6], false_works=work7)

        works = cond7.all_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2, work3, work4, work5, work6, work7])
        works = cond7.all_pre_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work4, work5])
        works = cond7.all_next_works()
        works.sort(key=lambda x: x.work_id)
        # print([w.work_id for w in works])
        assert(works == [work2, work3, work6, work7])
        cond_status = cond7.get_condition_status()
        assert(cond_status is False)

        work4.status = WorkStatus.Finished
        cond_status = cond7.get_condition_status()
        assert(cond_status is False)
        work5.status = WorkStatus.Finished
        cond_status = cond7.get_condition_status()
        assert(cond_status is True)
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New

        works = cond7.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2, work6])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work7])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2, work6])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        # multiple conditions
        # cond8 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)
        cond8 = Condition(cond=work1.is_finished)
        cond9 = CompositeCondition(conditions=[work4.is_finished, cond8.is_condition_true], true_works=[work6], false_works=work7)

        works = cond9.all_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work4, work6, work7])
        works = cond9.all_pre_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work4])
        works = cond9.all_next_works()
        works.sort(key=lambda x: x.work_id)
        # print([w.work_id for w in works])
        assert(works == [work6, work7])
        cond_status = cond9.get_condition_status()
        assert(cond_status is False)

        work4.status = WorkStatus.Finished
        cond_status = cond9.get_condition_status()
        assert(cond_status is False)
        work1.status = WorkStatus.Finished
        cond_status = cond9.get_condition_status()
        assert(cond_status is True)
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond9.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work7])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        return workflow

    def print_workflow(self, workflow):
        print('print workflow')
        print(workflow.conditions)
        for cond_id in workflow.conditions:
            print(cond_id)
            cond = workflow.conditions[cond_id]
            print(cond)
            print(cond.conditions)
            print(cond.true_works)
            print(cond.false_works)
            for w in cond.true_works:
                print(w)
                if isinstance(w, CompositeCondition):
                    print(w.conditions)
                    print(w.true_works)
                    print(w.false_works)

    def test_workflow(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)
        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        work4 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=4)
        work5 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=5)
        work6 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=6)
        work7 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=7,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.raw.1'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work2'}])
        work8 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=8,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.work2'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work3'}])

        workflow = Workflow()
        workflow.add_work(work1, initial=False)
        workflow.add_work(work2, initial=False)
        workflow.add_work(work3, initial=False)
        workflow.add_work(work4, initial=False)
        workflow.add_work(work5, initial=False)
        workflow.add_work(work6, initial=False)
        workflow.add_work(work7, initial=False)
        workflow.add_work(work8, initial=False)

        # multiple conditions
        cond6 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)
        cond7 = CompositeCondition(conditions=[work4.is_finished, work5.is_finished], true_works=[work6, cond6], false_works=work7)

        # multiple conditions
        # cond8 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)
        cond8 = Condition(cond=work1.is_finished)
        cond9 = CompositeCondition(conditions=[work4.is_finished, cond8.is_condition_true], true_works=[work6], false_works=work7)

        workflow.add_condition(cond7)
        workflow.add_condition(cond9)
        id_works = workflow.independent_works
        # print(id_works)
        id_works.sort()
        id_works_1 = [work1, work4, work5, work8]
        id_works_1 = [w.get_template_id() for w in id_works_1]
        id_works_1.sort()
        # id_works.sort(key=lambda x: x.work_id)
        assert(id_works == id_works_1)

        workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        workflow1 = json_loads(workflow_str)
        # print('before load_metadata')
        # self.print_workflow(workflow1)
        workflow1.load_metadata()
        # print('after load_metadata')
        # self.print_workflow(workflow1)
        workflow_str1 = json_dumps(workflow1, sort_keys=True, indent=4)
        assert(workflow_str == workflow_str1)

        works = cond7.all_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2, work3, work4, work5, work6, work7])
        works = cond7.all_pre_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work4, work5])
        works = cond7.all_next_works()
        works.sort(key=lambda x: x.work_id)
        # print([w.work_id for w in works])
        assert(works == [work2, work3, work6, work7])
        cond_status = cond7.get_condition_status()
        assert(cond_status is False)

        work4.status = WorkStatus.Finished
        cond_status = cond7.get_condition_status()
        assert(cond_status is False)
        work5.status = WorkStatus.Finished
        cond_status = cond7.get_condition_status()
        assert(cond_status is True)
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New

        works = cond7.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2, work6])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work7])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2, work6])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        # multiple conditions
        # cond8 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)
        # cond8 = Condition(cond=work1.is_finished)
        # cond9 = CompositeCondition(conditions=[work4.is_finished, cond8.is_condition_true], true_works=[work6], false_works=work7)

        works = cond9.all_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work4, work6, work7])
        works = cond9.all_pre_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work4])
        works = cond9.all_next_works()
        works.sort(key=lambda x: x.work_id)
        # print([w.work_id for w in works])
        assert(works == [work6, work7])
        cond_status = cond9.get_condition_status()
        assert(cond_status is False)

        work4.status = WorkStatus.Finished
        cond_status = cond9.get_condition_status()
        assert(cond_status is False)
        work1.status = WorkStatus.Finished
        cond_status = cond9.get_condition_status()
        assert(cond_status is True)
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond9.get_next_works(trigger=ConditionTrigger.NotTriggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.NotTriggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work7])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        return workflow

    def test_workflow_condition_reload(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)
        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        work4 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=4)
        work5 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=5)
        work6 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=6)
        work7 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=7,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.raw.1'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work2'}])
        work8 = Work(executable='echo',
                     arguments='--in=IN_DATASET --out=OUT_DATASET',
                     sandbox=None,
                     work_id=8,
                     primary_input_collection={'scope': 'data17', 'name': 'data17.test.work2'},
                     output_collections=[{'scope': 'data17', 'name': 'data17.test.work3'}])

        workflow = Workflow()
        workflow.add_work(work1, initial=False)
        workflow.add_work(work2, initial=False)
        workflow.add_work(work3, initial=False)
        workflow.add_work(work4, initial=False)
        workflow.add_work(work5, initial=False)
        workflow.add_work(work6, initial=False)
        workflow.add_work(work7, initial=False)
        workflow.add_work(work8, initial=False)

        # multiple conditions
        cond6 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)
        cond7 = CompositeCondition(conditions=[work4.is_finished, work5.is_finished], true_works=[work6, cond6], false_works=work7)

        # multiple conditions
        # cond8 = Condition(cond=work1.is_finished, true_work=work2, false_work=work3)
        cond8 = Condition(cond=work1.is_finished)
        cond9 = CompositeCondition(conditions=[work4.is_finished, cond8.is_condition_true], true_works=[work6], false_works=work7)

        workflow.add_condition(cond7)
        workflow.add_condition(cond9)

        workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        workflow1 = json_loads(workflow_str)
        # print('before load_metadata')
        # self.print_workflow(workflow1)
        workflow1.load_metadata()
        # print('after load_metadata')
        # self.print_workflow(workflow1)
        workflow_str1 = json_dumps(workflow1, sort_keys=True, indent=4)
        assert(workflow_str == workflow_str1)

        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work7])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        works = cond7.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work5.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3, work6])
        work1.status = WorkStatus.Finished
        works = cond7.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2, work6])
        work4.status = WorkStatus.New
        work5.status = WorkStatus.New
        work1.status = WorkStatus.New

        # cond9
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [work7])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        assert(works == [])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        works = cond9.get_next_works(trigger=ConditionTrigger.ToTrigger)
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        assert(works == [work7])
        work4.status = WorkStatus.Finished
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work1.status = WorkStatus.Finished
        works = cond9.get_next_works(trigger=ConditionTrigger.Triggered)
        works.sort(key=lambda x: x.work_id)
        assert(works == [work6])
        work4.status = WorkStatus.New
        work1.status = WorkStatus.New

        return workflow

    def test_workflow_loop(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow = Workflow()
        workflow.add_work(work1, initial=False)
        workflow.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow.add_loop_condition(cond)

        workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        workflow1 = json_loads(workflow_str)
        # print('before load_metadata')
        # self.print_workflow(workflow1)
        workflow1.load_metadata()
        # print('after load_metadata')
        # self.print_workflow(workflow1)
        workflow_str1 = json_dumps(workflow1, sort_keys=True, indent=4)
        assert(workflow_str == workflow_str1)

    def test_workflow_loop1(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow = Workflow()
        workflow.add_work(work1, initial=False)
        workflow.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow.add_loop_condition(cond)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        assert(workflow.num_run == 1)

        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        return workflow

    def test_workflow_loop2(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow = Workflow()
        workflow.add_work(work1, initial=False)
        workflow.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow.add_loop_condition(cond)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        assert(workflow.num_run == 1)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Finished
        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        assert(workflow.num_run == 2)
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Finished
        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        assert(workflow.num_run == 3)
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Failed
        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.num_run == 3)
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)

        return workflow

    def test_workflow_subworkflow(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)

        works = workflow.get_new_works()
        # print(json_dumps(workflow, sort_keys=True, indent=4))
        # print(json_dumps(works, sort_keys=True, indent=4))
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2, work3])
        # assert(workflow1.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed
        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        assert(workflow.is_terminated() is True)

    def test_workflow_subworkflow1(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        cond = Condition(cond=work3.is_finished, true_work=workflow1)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)
        workflow.add_condition(cond)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3])
        # assert(workflow.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.is_terminated() is True)

    def test_workflow_subworkflow2(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        cond = Condition(cond=work3.is_finished, true_work=workflow1)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)
        workflow.add_condition(cond)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3])
        # assert(workflow.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        assert(workflow.is_terminated() is False)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.is_terminated() is True)

    def test_workflow_subloopworkflow(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow1.add_loop_condition(cond)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2, work3])
        # assert(workflow1.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed
        assert(workflow.is_terminated() is True)

    def test_workflow_subloopworkflow1(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow1.add_loop_condition(cond)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2, work3])
        # assert(workflow1.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished
        assert(workflow.is_terminated() is False)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        # assert(workflow1.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed
        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        assert(workflow.is_terminated() is True)

    def test_workflow_subloopworkflow2(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow1.add_loop_condition(cond)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        cond1 = Condition(cond=work3.is_finished, true_work=workflow1)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)
        workflow.add_condition(cond1)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3])
        # assert(workflow.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.is_terminated() is True)

    def test_workflow_subloopworkflow3(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow1.add_loop_condition(cond)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        cond1 = Condition(cond=work3.is_finished, true_work=workflow1)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)
        workflow.add_condition(cond1)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3])
        # assert(workflow.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        assert(workflow.is_terminated() is False)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        assert(works == [work1, work2])
        assert(workflow.is_terminated() is False)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        assert(works == [work1, work2])
        assert(workflow.is_terminated() is False)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Failed

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.is_terminated() is True)

    def test_custom_condition(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work1.add_custom_condition(key="to_continue", value=True)
        assert(work1.get_custom_condition_status() is False)
        # output_data will be set based on the outputs of jobs.
        work1.output_data = {'to_continue': True}
        assert(work1.get_custom_condition_status() is True)

    def test_workflow_subloopworkflow4(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        work2.add_custom_condition(key="to_continue", value=True)
        cond = Condition(cond=work2.get_custom_condition_status)
        workflow1.add_loop_condition(cond)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        cond1 = Condition(cond=work3.is_finished, true_work=workflow1)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)
        workflow.add_condition(cond1)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3])
        # assert(workflow.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.is_terminated() is True)

    def test_workflow_subloopworkflow_reload(self):
        work1 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=1)
        work2 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=2)

        workflow1 = Workflow()
        workflow1.add_work(work1, initial=False)
        workflow1.add_work(work2, initial=False)

        cond = Condition(cond=work2.is_finished)
        workflow1.add_loop_condition(cond)

        work3 = Work(executable='/bin/hostname', arguments=None, sandbox=None, work_id=3)
        cond1 = Condition(cond=work3.is_finished, true_work=workflow1)

        workflow = Workflow()
        workflow.add_work(work3, initial=False)
        workflow.add_work(workflow1, initial=False)
        workflow.add_condition(cond1)

        # reload
        workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        workflow = json_loads(workflow_str)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3])
        # assert(workflow.num_run == 1)

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        # reload
        workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        workflow = json_loads(workflow_str)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1, work2])
        assert(workflow.is_terminated() is False)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Finished

        # reload
        workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        workflow = json_loads(workflow_str)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        assert(works == [work1, work2])
        assert(workflow.is_terminated() is False)

        for work in works:
            work.transforming = True
            work.status = WorkStatus.Failed

        # reload
        workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)
        workflow = json_loads(workflow_str)

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.is_terminated() is True)

    def test_workflow_subloopworkflow_parameter_link(self):
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

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1])
        # assert(workflow.num_run == 1)
        work1_1 = works[0]
        assert(work1_1.primary_input_collection.name == 'input_test_work_1')
        assert(work1_1.primary_output_collection.name == 'output_test_work_1')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        assert(workflow1.is_terminated() is False)
        work2_1 = works[0]
        # workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
        # print(workflow_str)

        assert(work2_1.primary_input_collection.name == 'output_test_work_1')
        assert(work2_1.primary_output_collection.name == 'output_test_work_2')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow1.is_terminated() is True)

    def test_workflow_subloopworkflow_parameter_link1(self):
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

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1])
        # assert(workflow.num_run == 1)
        work1_1 = works[0]
        assert(work1_1.primary_input_collection.name == 'input_test_work_1')
        assert(work1_1.primary_output_collection.name == 'output_test_work_1')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        assert(workflow1.is_terminated() is False)
        work2_1 = works[0]
        # workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
        # print(workflow_str)

        assert(work2_1.primary_input_collection.name == 'output_test_work_1')
        assert(work2_1.primary_output_collection.name == 'output_test_work_2')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1])
        assert(workflow1.is_terminated() is False)
        # workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
        # print(workflow_str)
        work1_2 = works[0]
        assert(work1_2.primary_input_collection.name == 'input_test_work_1')
        assert(work1_2.primary_output_collection.name == 'output_test_work_1.2')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        assert(workflow1.is_terminated() is False)
        work2_2 = works[0]
        # workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
        # print(workflow_str)

        assert(work2_2.primary_input_collection.name == 'output_test_work_1.2')
        assert(work2_2.primary_output_collection.name == 'output_test_work_2.2')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed

        works = workflow1.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow1.is_terminated() is True)

    def test_workflow_subloopworkflow_parameter_link2(self):
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

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work3])
        # assert(workflow.num_run == 1)
        work3_1 = works[0]
        assert(work3_1.primary_input_collection.name == 'input_test_work_3')
        assert(work3_1.primary_output_collection.name == 'output_test_work_3')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1])
        assert(workflow.is_terminated() is False)
        work1_1 = works[0]
        # workflow_str = json_dumps(workflow, sort_keys=True, indent=4)
        # print(workflow_str)

        assert(work1_1.primary_input_collection.name == 'output_test_work_3')
        assert(work1_1.primary_output_collection.name == 'output_test_work_1')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        assert(workflow1.is_terminated() is False)
        # workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
        # print(workflow_str)
        work2_1 = works[0]
        assert(work2_1.primary_input_collection.name == 'output_test_work_1')
        assert(work2_1.primary_output_collection.name == 'output_test_work_2')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work1])
        assert(workflow1.is_terminated() is False)
        work1_2 = works[0]
        # workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
        # print(workflow_str)

        assert(work1_2.primary_input_collection.name == 'output_test_work_3')
        assert(work1_2.primary_output_collection.name == 'output_test_work_1.2')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Finished

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [work2])
        assert(workflow1.is_terminated() is False)
        work2_2 = works[0]
        # workflow_str = json_dumps(workflow1, sort_keys=True, indent=4)
        # print(workflow_str)

        assert(work2_2.primary_input_collection.name == 'output_test_work_1.2')
        assert(work2_2.primary_output_collection.name == 'output_test_work_2.2')

        for work in works:
            # if work.work_id == 3:
            work.transforming = True
            work.status = WorkStatus.Failed

        works = workflow.get_new_works()
        works.sort(key=lambda x: x.work_id)
        assert(works == [])
        assert(workflow.is_terminated() is True)
