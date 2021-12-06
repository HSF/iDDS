#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2021

import copy
import datetime
import logging
import inspect
import random
import time
import uuid


from idds.common import exceptions
from idds.common.constants import IDDSEnum
from idds.common.utils import json_dumps, setup_logging, get_proxy
from idds.common.utils import str_to_date
from .base import Base
from .work import Work


setup_logging(__name__)


class ConditionOperator(IDDSEnum):
    And = 0
    Or = 1


class ConditionTrigger(IDDSEnum):
    NotTriggered = 0
    ToTrigger = 1
    Triggered = 2


class CompositeCondition(Base):
    def __init__(self, operator=ConditionOperator.And, conditions=[], true_works=None, false_works=None, logger=None):
        self._conditions = []
        self._true_works = []
        self._false_works = []

        super(CompositeCondition, self).__init__()

        self.internal_id = str(uuid.uuid4())[:8]
        self.template_id = self.internal_id
        # self.template_id = str(uuid.uuid4())[:8]

        self.logger = logger
        if self.logger is None:
            self.setup_logger()

        if conditions is None:
            conditions = []
        if true_works is None:
            true_works = []
        if false_works is None:
            false_works = []
        if conditions and type(conditions) not in [tuple, list]:
            conditions = [conditions]
        if true_works and type(true_works) not in [tuple, list]:
            true_works = [true_works]
        if false_works and type(false_works) not in [tuple, list]:
            false_works = [false_works]
        self.validate_conditions(conditions)

        self.operator = operator
        self.conditions = []
        self.true_works = []
        self.false_works = []

        self.conditions = conditions
        self.true_works = true_works
        self.false_works = false_works

    def get_class_name(self):
        return self.__class__.__name__

    def get_internal_id(self):
        return self.internal_id

    def get_template_id(self):
        return self.template_id

    @property
    def conditions(self):
        # return self.get_metadata_item('true_works', [])
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        self._conditions = value
        new_value = []
        for cond in value:
            if cond is None:
                continue
            if inspect.ismethod(cond):
                new_cond = {'idds_method': cond.__name__,
                            'idds_method_internal_id': cond.__self__.get_internal_id(),
                            'idds_method_class_id': cond.__self__.get_template_id()}
            else:
                new_cond = cond
            new_value.append(new_cond)
        self.add_metadata_item('conditions', new_value)

    @property
    def true_works(self):
        # return self.get_metadata_item('true_works', [])
        return self._true_works

    @true_works.setter
    def true_works(self, value):
        self._true_works = value
        true_work_meta = self.get_metadata_item('true_works', {})
        for work in value:
            if work is None:
                continue
            if isinstance(work, Work):
                if work.get_template_id() not in true_work_meta:
                    true_work_meta[work.get_internal_id()] = {'triggered': False,
                                                              'template_id': work.get_template_id()}
            elif isinstance(work, CompositeCondition):
                if work.get_template_id() not in true_work_meta:
                    true_work_meta[work.get_internal_id()] = {'triggered': False,
                                                              'template_id': work.get_template_id(),
                                                              'metadata': work.metadata}
            elif isinstance(work, Workflow):
                if work.get_template_id() not in true_work_meta:
                    true_work_meta[work.get_internal_id()] = {'triggered': False,
                                                              'template_id': work.get_template_id(),
                                                              'metadata': work.metadata}
        self.add_metadata_item('true_works', true_work_meta)

    @property
    def false_works(self):
        # return self.get_metadata_item('false_works', [])
        return self._false_works

    @false_works.setter
    def false_works(self, value):
        self._false_works = value
        false_work_meta = self.get_metadata_item('false_works', {})
        for work in value:
            if work is None:
                continue
            if isinstance(work, Work):
                if work.get_template_id() not in false_work_meta:
                    false_work_meta[work.get_internal_id()] = {'triggered': False,
                                                               'template_id': work.get_template_id()}
            elif isinstance(work, CompositeCondition):
                if work.get_template_id() not in false_work_meta:
                    false_work_meta[work.get_internal_id()] = {'triggered': False,
                                                               'template_id': work.get_template_id(),
                                                               'metadata': work.metadata}
            elif isinstance(work, Workflow):
                if work.get_template_id() not in false_work_meta:
                    false_work_meta[work.get_internal_id()] = {'triggered': False,
                                                               'template_id': work.get_template_id(),
                                                               'metadata': work.metadata}
        self.add_metadata_item('false_works', false_work_meta)

    def validate_conditions(self, conditions):
        if type(conditions) not in [tuple, list]:
            raise exceptions.IDDSException("conditions must be list")
        for cond in conditions:
            assert(inspect.ismethod(cond))
            assert(isinstance(cond.__self__, Work))
            if (cond.__self__.is_template):
                raise exceptions.IDDSException("Work class for CompositeCondition must not be a template")

    def add_condition(self, cond):
        assert(inspect.ismethod(cond))
        assert(isinstance(cond.__self__, Work))
        if (cond.__self__.is_template):
            raise exceptions.IDDSException("Work class for CompositeCondition must not be a template")

        # self.conditions.append({'condition': cond, 'current_work': cond.__self__})

        self._conditions.append(cond)
        new_value = self.get_metadata_item('conditions', [])
        if inspect.ismethod(cond):
            new_cond = {'idds_method': cond.__name__,
                        'idds_method_internal_id': cond.__self__.get_internal_id(),
                        'idds_method_class_id': cond.__self__.get_template_id()}
        else:
            new_cond = cond
        new_value.append(new_cond)
        self.add_metadata_item('conditions', new_value)

    def load_metadata(self):
        # conditions = self.get_metadata_item('conditions', [])
        true_works_meta = self.get_metadata_item('true_works', {})
        false_works_meta = self.get_metadata_item('false_works', {})

        for work in self.true_works:
            if isinstance(work, CompositeCondition) or isinstance(work, Workflow):
                if work.get_internal_id() in true_works_meta:
                    work.metadata = true_works_meta[work.get_internal_id()]['metadata']
        for work in self.false_works:
            if isinstance(work, CompositeCondition) or isinstance(work, Workflow):
                if work.get_internal_id() in false_works_meta:
                    work.metadata = false_works_meta[work.get_internal_id()]['metadata']

    def to_dict(self):
        # print('to_dict')
        ret = {'class': self.__class__.__name__,
               'module': self.__class__.__module__,
               'attributes': {}}
        for key, value in self.__dict__.items():
            # print(key)
            # print(value)
            # if not key.startswith('__') and not key.startswith('_'):
            if not key.startswith('__'):
                if key == 'logger':
                    value = None
                elif key == '_conditions':
                    new_value = []
                    for cond in value:
                        if inspect.ismethod(cond):
                            new_cond = {'idds_method': cond.__name__,
                                        'idds_method_internal_id': cond.__self__.get_internal_id(),
                                        'idds_method_class_id': cond.__self__.get_template_id()}
                        else:
                            new_cond = cond
                        new_value.append(new_cond)
                    value = new_value
                elif key in ['_true_works', '_false_works']:
                    new_value = []
                    for w in value:
                        if isinstance(w, Work):
                            new_w = w.get_template_id()
                        elif isinstance(w, CompositeCondition):
                            new_w = w.to_dict()
                        elif isinstance(w, Workflow):
                            new_w = w.to_dict()
                        else:
                            new_w = w
                        new_value.append(new_w)
                    value = new_value
                else:
                    value = self.to_dict_l(value)
                ret['attributes'][key] = value
        return ret

    def get_work_from_id(self, work_id, class_id, works, works_template):
        for w_id in works:
            if works[w_id].get_internal_id() == work_id:
                return works[w_id]
        for w_id in works_template:
            if works_template[w_id].get_template_id() == class_id:
                return works_template[w_id]
        return None

    def load_conditions(self, works, works_template):
        new_conditions = []
        for cond in self.conditions:
            if 'idds_method' in cond and 'idds_method_class_id' in cond:
                internal_id = cond['idds_method_internal_id']
                class_id = cond['idds_method_class_id']
                work = self.get_work_from_id(internal_id, class_id, works, works_template)
                if work is not None:
                    new_cond = getattr(work, cond['idds_method'])
                else:
                    self.logger.error("Work cannot be found for %s:%s" % (internal_id, class_id))
                    new_cond = cond
            else:
                new_cond = cond
            new_conditions.append(new_cond)
        self.conditions = new_conditions

        new_true_works = []
        for w in self.true_works:
            class_id = self.true_works[w]['template_id']
            if isinstance(w, CompositeCondition):
                # work = w.load_conditions(works, works_template)
                w.load_conditions(works, works_template)
                work = w
            elif type(w) in [str]:
                work = self.get_work_from_id(w, class_id, works, works_template)
                if work is None:
                    self.logger.error("Work cannot be found for %s" % str(w))
                    work = w
            else:
                self.logger.error("Work cannot be found for %s" % str(w))
                work = w
            new_true_works.append(work)
        self.true_works = new_true_works

        new_false_works = []
        for w in self.false_works:
            class_id = self.true_works[w]['template_id']
            if isinstance(w, CompositeCondition):
                # work = w.load_condtions(works, works_template)
                w.load_conditions(works, works_template)
                work = w
            elif type(w) in [str]:
                work = self.get_work_from_id(w, class_id, works, works_template)
                if work is None:
                    self.logger.error("Work cannot be found for %s" % str(w))
                    work = w
            else:
                self.logger.error("Work cannot be found for %s" % str(w))
                work = w
            new_false_works.append(work)
        self.false_works = new_false_works

    def all_works(self):
        works = []
        works = works + self.all_pre_works()
        works = works + self.all_next_works()
        return works

    def all_condition_ids(self):
        works = []
        for cond in self.conditions:
            if inspect.ismethod(cond):
                works.append(cond.__self__.get_internal_id())
            else:
                self.logger.error("cond cannot be recognized: %s" % str(cond))
                works.append(cond)
        for work in self.true_works + self.false_works:
            if isinstance(work, CompositeCondition):
                works = works + work.all_condition_ids()
        return works

    def all_pre_works(self):
        works = []
        for cond in self.conditions:
            if inspect.ismethod(cond):
                works.append(cond.__self__)
            else:
                self.logger.error("cond cannot be recognized: %s" % str(cond))
                works.append(cond)
        for work in self.true_works + self.false_works:
            if isinstance(work, CompositeCondition):
                works = works + work.all_pre_works()
        return works

    def all_next_works(self):
        works = []
        for work in self.true_works + self.false_works:
            if isinstance(work, CompositeCondition):
                works = works + work.all_next_works()
            else:
                works.append(work)
        return works

    def get_current_cond_status(self, cond):
        if callable(cond):
            if cond():
                return True
            else:
                return False
        else:
            if cond:
                return True
            else:
                return False

    def get_cond_status(self):
        if self.operator == ConditionOperator.And:
            for cond in self.conditions:
                if not self.get_current_cond_status(cond):
                    return False
            return True
        else:
            for cond in self.conditions:
                if self.get_current_cond_status(cond):
                    return True
            return False

    def get_condition_status(self):
        return self.get_cond_status()

    def is_condition_true(self):
        if self.get_cond_status():
            return True
        return False

    def is_condition_false(self):
        if not self.get_cond_status():
            return True
        return False

    def get_next_works(self, trigger=ConditionTrigger.NotTriggered):
        works = []
        if self.get_cond_status():
            true_work_meta = self.get_metadata_item('true_works', {})
            for work in self.true_works:
                if isinstance(work, CompositeCondition) or isinstance(work, Workflow):
                    works = works + work.get_next_works(trigger=trigger)
                else:
                    if work.get_internal_id() not in true_work_meta:
                        true_work_meta[work.get_internal_id()] = {'triggered': False,
                                                                  'template_id': work.get_template_id()}
                    if trigger == ConditionTrigger.ToTrigger:
                        if not true_work_meta[work.get_internal_id()]['triggered']:
                            true_work_meta[work.get_internal_id()]['triggered'] = True
                            works.append(work)
                        elif work.get_is_template():
                            # A template can be triggered many times.
                            works.append(work)
                    elif trigger == ConditionTrigger.NotTriggered:
                        if not true_work_meta[work.get_internal_id()]['triggered']:
                            works.append(work)
                    elif trigger == ConditionTrigger.Triggered:
                        if true_work_meta[work.get_internal_id()]['triggered']:
                            works.append(work)
            self.add_metadata_item('true_works', true_work_meta)
        else:
            false_work_meta = self.get_metadata_item('false_works', {})
            for work in self.false_works:
                if isinstance(work, CompositeCondition) or isinstance(work, Workflow):
                    works = works + work.get_next_works(trigger=trigger)
                else:
                    if work.get_internal_id() not in false_work_meta:
                        false_work_meta[work.get_internal_id()] = {'triggered': False,
                                                                   'template_id': work.get_template_id()}
                    if trigger == ConditionTrigger.ToTrigger:
                        if not false_work_meta[work.get_internal_id()]['triggered']:
                            false_work_meta[work.get_internal_id()]['triggered'] = True
                            works.append(work)
                        elif work.get_is_template():
                            # A template can be triggered many times.
                            works.append(work)
                    elif trigger == ConditionTrigger.NotTriggered:
                        if not false_work_meta[work.get_internal_id()]['triggered']:
                            works.append(work)
                    elif trigger == ConditionTrigger.Triggered:
                        if false_work_meta[work.get_internal_id()]['triggered']:
                            works.append(work)
            self.add_metadata_item('false_works', false_work_meta)
        return works

    def generate_condition_from_template(self):
        logger = self.logger
        self.logger = None
        new_cond = copy.deepcopy(self)
        self.logger = logger
        new_cond.logger = logger
        # new_work.template_work_id = self.get_internal_id()
        new_cond.internal_id = str(uuid.uuid4())[:8]
        return new_cond


class AndCondition(CompositeCondition):
    def __init__(self, conditions=[], true_works=None, false_works=None, logger=None):
        super(AndCondition, self).__init__(operator=ConditionOperator.And,
                                           conditions=conditions,
                                           true_works=true_works,
                                           false_works=false_works,
                                           logger=logger)


class OrCondition(CompositeCondition):
    def __init__(self, conditions=[], true_works=None, false_works=None, logger=None):
        super(OrCondition, self).__init__(operator=ConditionOperator.Or,
                                          conditions=conditions,
                                          true_works=true_works,
                                          false_works=false_works,
                                          logger=logger)


class Condition(CompositeCondition):
    def __init__(self, cond=None, current_work=None, true_work=None, false_work=None, logger=None):
        super(Condition, self).__init__(operator=ConditionOperator.And,
                                        conditions=[cond] if cond else [],
                                        true_works=[true_work] if true_work else [],
                                        false_works=[false_work] if false_work else [],
                                        logger=logger)

    # to support load from old conditions
    @property
    def cond(self):
        # return self.get_metadata_item('true_works', [])
        return self.conditions[0] if len(self.conditions) >= 1 else None

    @cond.setter
    def cond(self, value):
        self.conditions = [value]

    @property
    def true_work(self):
        # return self.get_metadata_item('true_works', [])
        return self.true_works if len(self.true_works) >= 1 else None

    @true_work.setter
    def true_work(self, value):
        self.true_works = [value]

    @property
    def false_work(self):
        # return self.get_metadata_item('true_works', [])
        return self.false_works if len(self.false_works) >= 1 else None

    @false_work.setter
    def false_work(self, value):
        self.false_works = [value]


class TemplateCondition(CompositeCondition):
    def __init__(self, cond=None, current_work=None, true_work=None, false_work=None, logger=None):
        if true_work is not None and not isinstance(true_work, Work):
            raise exceptions.IDDSException("true_work can only be set with Work class")
        if false_work is not None and not isinstance(false_work, Work):
            raise exceptions.IDDSException("false_work can only be set with Work class")

        super(TemplateCondition, self).__init__(operator=ConditionOperator.And,
                                                conditions=[cond] if cond else [],
                                                true_works=[true_work] if true_work else [],
                                                false_works=[false_work] if false_work else [],
                                                logger=logger)

    def validate_conditions(self, conditions):
        if type(conditions) not in [tuple, list]:
            raise exceptions.IDDSException("conditions must be list")
        if len(conditions) > 1:
            raise exceptions.IDDSException("Condition class can only support one condition. To support multiple condition, please use CompositeCondition.")
        for cond in conditions:
            assert(inspect.ismethod(cond))
            assert(isinstance(cond.__self__, Work))

    def add_condition(self, cond):
        raise exceptions.IDDSException("Condition class doesn't support add_condition. To support multiple condition, please use CompositeCondition.")


class ParameterLink(Base):
    def __init__(self, parameters):
        self.parameters = parameters

    def get_parameter_value(self, work, p):
        p_f = getattr(work, p, 'None')
        if p_f:
            if callable(p_f):
                return p_f()
            else:
                return p_f
        else:
            return None

    def set_parameters(self, work):
        p_values = {}
        for p in self.parameters:
            p_values[p] = self.get_parameter_value(work, p)
        self.add_metadata_item('parameters', p_values)

    def get_parameters(self):
        return self.get_metadata_item('parameters', {})


class Workflow(Base):

    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        """
        Init a workflow.
        """
        self._conditions = {}
        self._conditions_temp = {}
        self._work_conds = {}
        self.conditions_template = {}
        self.work_conds_template = {}

        self._parameter_links = {}

        super(Workflow, self).__init__()

        self.internal_id = str(uuid.uuid4())[:8]
        self.template_work_id = self.internal_id
        # self.template_work_id = str(uuid.uuid4())[:8]
        self.lifetime = lifetime
        self.pending_time = pending_time

        if name:
            self._name = name + "." + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))
        else:
            self._name = 'idds.workflow.' + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))

        if workload_id is None:
            workload_id = int(time.time())
        self.workload_id = workload_id

        self.logger = logger
        if self.logger is None:
            self.setup_logger()

        self.works_template = {}
        self._works = {}
        self.works = {}
        self.work_sequence = {}  # order list

        self.terminated_works = []
        self.initial_works = []
        # if the primary initial_work is not set, it's the first initial work.
        self.primary_initial_work = None
        self.independent_works = []

        self.first_initial = False
        self.new_to_run_works = []
        self.current_running_works = []

        self.num_subfinished_works = 0
        self.num_finished_works = 0
        self.num_failed_works = 0
        self.num_cancelled_works = 0
        self.num_suspended_works = 0
        self.num_expired_works = 0
        self.num_total_works = 0

        self.last_work = None

        self.last_updated_at = datetime.datetime.utcnow()
        self.expired = False

        self.to_update_transforms = {}

        # user defined Condition class
        self.user_defined_conditions = {}

        self.username = None
        self.userdn = None
        self.proxy = None

        self.loop_condition_template = {}
        self.loop_condition = None

        self.parameter_links_template = {}
        self.parameter_links = {}

        """
        self._running_data_names = []
        for name in ['internal_id', 'template_work_id', 'workload_id', 'work_sequence', 'terminated_works',
                     'first_initial', 'new_to_run_works', 'current_running_works',
                     'num_subfinished_works', 'num_finished_works', 'num_failed_works', 'num_cancelled_works', 'num_suspended_works',
                     'num_expired_works', 'num_total_works', 'last_work']:
            self._running_data_names.append(name)
        for name in ['works']:
            self._running_data_names.append(name)
        """

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def internal_id(self):
        return self.get_metadata_item('internal_id')

    @internal_id.setter
    def internal_id(self, value):
        self.add_metadata_item('internal_id', value)

    @property
    def template_work_id(self):
        return self.get_metadata_item('template_work_id')

    @template_work_id.setter
    def template_work_id(self, value):
        self.add_metadata_item('template_work_id', value)

    def get_template_work_id(self):
        return self.template_work_id

    def get_template_id(self):
        return self.template_work_id

    @property
    def workload_id(self):
        return self.get_metadata_item('workload_id')

    @workload_id.setter
    def workload_id(self, value):
        self.add_metadata_item('workload_id', value)

    @property
    def lifetime(self):
        # return self.get_metadata_item('lifetime', None)
        return getattr(self, '_lifetime', None)

    @lifetime.setter
    def lifetime(self, value):
        # self.add_metadata_item('lifetime', value)
        self._lifetime = value

    @property
    def pending_time(self):
        # return self.get_metadata_item('pending_time', None)
        return getattr(self, '_pending_time', None)

    @pending_time.setter
    def pending_time(self, value):
        # self.add_metadata_item('pending_time', value)
        self._pending_time = value

    @property
    def last_updated_at(self):
        last_updated_at = self.get_metadata_item('last_updated_at', None)
        if last_updated_at and type(last_updated_at) in [str]:
            last_updated_at = str_to_date(last_updated_at)
        return last_updated_at

    @last_updated_at.setter
    def last_updated_at(self, value):
        self.add_metadata_item('last_updated_at', value)

    def has_new_updates(self):
        self.last_updated_at = datetime.datetime.utcnow()

    @property
    def expired(self):
        t = self.get_metadata_item('expired', False)
        if type(t) in [bool]:
            return t
        elif type(t) in [str] and t.lower() in ['true']:
            return True
        else:
            return False

    @expired.setter
    def expired(self, value):
        self.add_metadata_item('expired', value)

    @property
    def works(self):
        return self._works

    @works.setter
    def works(self, value):
        self._works = value
        work_metadata = {}
        if self._works:
            for k in self._works:
                work = self._works[k]
                if isinstance(work, Workflow):
                    work_metadata[k] = {'internal_id': work.internal_id,
                                        'template_id': work.get_template_id(),
                                        'type': 'workflow',
                                        'metadata': work.metadata}
                else:
                    work_metadata[k] = {'internal_id': work.internal_id,
                                        'template_id': work.get_template_id(),
                                        'type': 'work',
                                        'work_id': work.work_id,
                                        'status': work.status,
                                        'substatus': work.substatus,
                                        'transforming': work.transforming}
        self.add_metadata_item('works', work_metadata)

    def refresh_works(self):
        work_metadata = {}
        if self._works:
            for k in self._works:
                work = self._works[k]
                if isinstance(work, Workflow):
                    work.refresh_works()
                    work_metadata[k] = {'internal_id': work.internal_id,
                                        'template_id': work.get_template_id(),
                                        'type': 'workflow',
                                        'metadata': work.metadata}
                else:
                    work_metadata[k] = {'internal_id': work.internal_id,
                                        'template_id': work.get_template_id(),
                                        'type': 'work',
                                        'work_id': work.work_id,
                                        'status': work.status,
                                        'substatus': work.substatus,
                                        'transforming': work.transforming}
                if work.last_updated_at and (not self.last_updated_at or work.last_updated_at > self.last_updated_at):
                    self.last_updated_at = work.last_updated_at
        self.add_metadata_item('works', work_metadata)

    def load_works(self):
        work_metadata = self.get_metadata_item('works', {})
        for k in work_metadata:
            if 'type' not in work_metadata[k]:
                work_metadata[k]['type'] = 'work'

            if work_metadata[k]['type'] == 'work':
                if k not in self._works:
                    template_id = work_metadata[k]['template_id']
                    work_template = self.works_template[template_id]
                    new_work = work_template.generate_work_from_template()
                    new_work.work_id = work_metadata[k]['work_id']
                    new_work.internal_id = work_metadata[k]['internal_id']
                    self._works[k] = new_work
                self._works[k].work_id = work_metadata[k]['work_id']
                self._works[k].transforming = work_metadata[k]['transforming']
                if 'status' in work_metadata[k]:
                    self._works[k].status = work_metadata[k]['status']
                    self._works[k].substatus = work_metadata[k]['substatus']
            elif work_metadata[k]['type'] == 'workflow':
                if k not in self._works:
                    template_id = work_metadata[k]['template_id']
                    workflow_template = self.works_template[template_id]
                    new_workflow = workflow_template.generate_work_from_template()
                    new_workflow.metadata = work_metadata[k]['metadata']
                    # new_workflow.load_works()
                    new_workflow.internal_id = work_metadata[k]['internal_id']
                    self._works[k] = new_workflow

            work = self._works[k]
            if work.last_updated_at and (not self.last_updated_at or work.last_updated_at > self.last_updated_at):
                self.last_updated_at = work.last_updated_at

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        self._conditions = value
        conditions_metadata = {}
        if self._conditions:
            for k in self._conditions:
                conditions_metadata[k] = {'template_id': self._conditions[k].get_template_id(),
                                          'metadata': self._conditions[k].metadata}
        self.add_metadata_item('conditions', conditions_metadata)

    @property
    def conditions_temp(self):
        return self._conditions_temp

    @conditions_temp.setter
    def conditions_temp(self, value):
        self._conditions_temp = value
        conditions_metadata = {}
        if self._conditions_temp:
            conditions_metadata = self._conditions_temp
        self.add_metadata_item('conditions_temp', conditions_metadata)

    @property
    def work_conds(self):
        return self._work_conds

    @work_conds.setter
    def work_conds(self, value):
        self._work_conds = value
        self.add_metadata_item('work_conds', value)

    def load_work_conditions(self):
        conditions_metadata = self.get_metadata_item('conditions', {})
        for cond_internal_id in conditions_metadata:
            template_id = conditions_metadata[cond_internal_id]['template_id']
            cond_template = self.conditions_template[template_id]
            cond = cond_template.generate_condition_from_template()
            cond.metadata = conditions_metadata[cond_internal_id]['metadata']
            self.conditions[cond_internal_id] = cond
            self.conditions[cond_internal_id].load_conditions(self.works, self.get_works_template())

        work_conds = self.get_metadata_item('work_conds', {})
        self._work_conds = work_conds

    @property
    def loop_condition(self):
        return self._loop_condition

    @loop_condition.setter
    def loop_condition(self, value):
        self._loop_condition = value
        self.add_metadata_item('loop_condition', self._loop_condition.get_condition_status())

    @property
    def work_sequence(self):
        return self.get_metadata_item('work_sequence', {})

    @work_sequence.setter
    def work_sequence(self, value):
        self.add_metadata_item('work_sequence', value)

    @property
    def terminated_works(self):
        return self.get_metadata_item('terminated_works', [])

    @terminated_works.setter
    def terminated_works(self, value):
        self.add_metadata_item('terminated_works', value)

    @property
    def first_initial(self):
        return self.get_metadata_item('first_initial', False)

    @first_initial.setter
    def first_initial(self, value):
        self.add_metadata_item('first_initial', value)

    @property
    def new_to_run_works(self):
        return self.get_metadata_item('new_to_run_works', [])

    @new_to_run_works.setter
    def new_to_run_works(self, value):
        self.add_metadata_item('new_to_run_works', value)

    @property
    def current_running_works(self):
        return self.get_metadata_item('current_running_works', [])

    @current_running_works.setter
    def current_running_works(self, value):
        self.add_metadata_item('current_running_works', value)

    @property
    def num_subfinished_works(self):
        return self.get_metadata_item('num_subfinished_works', 0)

    @num_subfinished_works.setter
    def num_subfinished_works(self, value):
        self.add_metadata_item('num_subfinished_works', value)

    @property
    def num_finished_works(self):
        return self.get_metadata_item('num_finished_works', 0)

    @num_finished_works.setter
    def num_finished_works(self, value):
        self.add_metadata_item('num_finished_works', value)

    @property
    def num_failed_works(self):
        return self.get_metadata_item('num_failed_works', 0)

    @num_failed_works.setter
    def num_failed_works(self, value):
        self.add_metadata_item('num_failed_works', value)

    @property
    def num_cancelled_works(self):
        return self.get_metadata_item('num_cancelled_works', 0)

    @num_cancelled_works.setter
    def num_cancelled_works(self, value):
        self.add_metadata_item('num_cancelled_works', value)

    @property
    def num_suspended_works(self):
        return self.get_metadata_item('num_suspended_works', 0)

    @num_suspended_works.setter
    def num_suspended_works(self, value):
        self.add_metadata_item('num_suspended_works', value)

    @property
    def num_expired_works(self):
        return self.get_metadata_item('num_expired_works', 0)

    @num_expired_works.setter
    def num_expired_works(self, value):
        self.add_metadata_item('num_expired_works', value)

    @property
    def num_total_works(self):
        return self.get_metadata_item('num_total_works', 0)

    @num_total_works.setter
    def num_total_works(self, value):
        self.add_metadata_item('num_total_works', value)

    @property
    def last_work(self):
        return self.get_metadata_item('last_work', None)

    @last_work.setter
    def last_work(self, value):
        self.add_metadata_item('last_work', value)

    @property
    def to_update_transforms(self):
        return self.get_metadata_item('to_update_transforms', {})

    @to_update_transforms.setter
    def to_update_transforms(self, value):
        self.add_metadata_item('to_update_transforms', value)

    def load_metadata(self):
        self.load_works()
        self.load_work_conditions()

    def get_class_name(self):
        return self.__class__.__name__

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())

    def log_info(self, info):
        if self.logger is None:
            self.setup_logger()
        self.logger.info(info)

    def log_debug(self, info):
        if self.logger is None:
            self.setup_logger()
        self.logger.debug(info)

    def get_internal_id(self):
        return self.internal_id

    def copy(self):
        new_wf = copy.deepcopy(self)
        return new_wf

    def __deepcopy__(self, memo):
        logger = self.logger
        self.logger = None

        cls = self.__class__
        result = cls.__new__(cls)

        memo[id(self)] = result

        # Deep copy all other attributes
        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memo))

        self.logger = logger
        result.logger = logger
        return result

    def get_works_template(self):
        return self.works_template

    def generate_work_from_template(self):
        logger = self.logger
        self.logger = None
        new_workflow = copy.deepcopy(self)
        self.logger = logger
        new_workflow.logger = logger
        # new_work.template_work_id = self.get_internal_id()
        new_workflow.internal_id = str(uuid.uuid4())[:8]
        return new_workflow

    def add_work_template(self, work):
        self.works_template[work.get_template_id()] = work

    def get_new_work_from_template(self, work_id, new_parameters=None):
        # 1. initialize works
        # template_id = work.get_template_id()
        template_id = work_id
        work = self.works_template[template_id]
        new_work = work.generate_work_from_template()
        if new_parameters:
            new_work.set_parameters(new_parameters)
        new_work.sequence_id = self.num_total_works
        if isinstance(new_work, Workflow):
            pass
        else:
            new_work.initialize_work()
        works = self.works
        works[new_work.get_internal_id()] = new_work
        self.works = works
        # self.work_sequence.append(new_work.get_internal_id())
        self.work_sequence[str(self.num_total_works)] = new_work.get_internal_id()
        if isinstance(new_work, Workflow):
            self.num_total_works += 1
        else:
            self.num_total_works += 1
        self.new_to_run_works.append(new_work.get_internal_id())
        self.last_work = new_work.get_internal_id()

        # 2. initialize conditions related to this work
        if template_id in self.work_conds_template:
            conds = self.work_conds_template[template_id]
            self.work_conds[new_work.get_internal_id()] = []
            for cond_template_id in conds:
                cond_template = self.conditions_template[cond_template_id]
                if not cond_template.has_multiple_pre_works():
                    cond = cond_template.generate_new_cond_from_template()
                    self.conditions[cond.get_internal_id()] = cond
                    self.work_conds[new_work.get_internal_id()].append(cond.get_internal_id())
                    cond.attach_pre_work(new_work)
                else:
                    if cond_template_id in self.conditions_temp:
                        # condition is already created, for example, for AndCondition or OrCondition
                        # cond_temp will be cleaned when a new loop is created in LoopWorkflow
                        cond_internal_id = self.conditions_temp[cond_template_id]
                        self.work_conds[new_work.get_internal_id()].append(cond_internal_id)
                        cond = self.conditions[cond_internal_id]
                        cond.attach_pre_work(new_work)
                    else:
                        cond = cond_template.generate_new_cond_from_template()
                        self.conditions[cond.get_internal_id()] = cond
                        self.work_conds[new_work.get_internal_id()].append(cond.get_internal_id())
                        self.conditions_temp[cond_template_id] = cond.get_internal_id()
                        cond.attach_pre_work(new_work)
        return new_work

    def register_user_defined_condition(self, condition):
        cond_src = inspect.getsource(condition)
        self.user_defined_conditions[condition.__name__] = cond_src

    def load_user_defined_condition(self):
        # try:
        #     Condition()
        # except NameError:
        #     global Condition
        #     import Condition

        for cond_src_name in self.user_defined_conditions:
            # global cond_src_name
            exec(self.user_defined_conditions[cond_src_name])

    def set_workload_id(self, workload_id):
        self.workload_id = workload_id

    def get_workload_id(self):
        return self.workload_id

    def add_work(self, work, initial=False, primary=False):
        self.first_initial = False
        self.add_work_template(work)
        if initial:
            if primary:
                self.primary_initial_work = work.get_template_id()
            self.add_initial_works(work)

        self.independent_works.append(work.get_template_id())

    def add_condition(self, cond):
        self.first_initial = False
        cond_works = cond.all_works()
        for cond_work in cond_works:
            assert(cond_work.get_template_id() in self.get_works_template())

        if cond.get_template_id() not in self.conditions_template:
            conditions = self.conditions_template
            conditions[cond.get_template_id()] = cond
            self.conditions_template = conditions

        # if cond.current_work not in self.work_conds:
        #     self.work_conds[cond.current_work] = []
        # self.work_conds[cond.current_work].append(cond)
        work_conds = self.work_conds_template
        for work in cond.all_pre_works():
            if work.get_template_id() not in work_conds:
                work_conds[work.get_template_id()] = []
            work_conds[work.get_template_id()].append(cond.get_template_id())
        self.work_conds_template = work_conds

        # if a work is a true_work or false_work of a condition,
        # should remove it from independent_works
        cond_next_works = cond.all_next_works()
        for next_work in cond_next_works:
            if next_work.get_template_id() in self.independent_works:
                self.independent_works.remove(next_work.get_template_id())

    def add_initial_works(self, work):
        assert(work.get_template_id() in self.get_works_template())
        self.initial_works.append(work.get_template_id())
        if self.primary_initial_work is None:
            self.primary_initial_work = work.get_template_id()

    def enable_next_works(self, work, cond):
        self.log_debug("Checking Work %s condition: %s" % (work.get_internal_id(),
                                                           json_dumps(cond, sort_keys=True, indent=4)))
        # load_conditions should cover it.
        # if cond and self.is_class_method(cond.cond):
        #     # cond_work_id = self.works[cond.cond['idds_method_class_id']]
        #     cond.cond = getattr(work, cond.cond['idds_method'])

        self.log_info("Work %s condition: %s" % (work.get_internal_id(), cond.conditions))
        next_works = cond.get_next_works(trigger=ConditionTrigger.ToTrigger)
        self.log_info("Work %s condition status %s" % (work.get_internal_id(), cond.get_cond_status()))
        self.log_info("Work %s next works %s" % (work.get_internal_id(), str(next_works)))
        new_next_works = []
        if next_works is not None:
            for next_work in next_works:
                new_parameters = work.get_parameters_for_next_task()
                new_next_work = self.get_new_work_from_template(next_work.get_template_id(), new_parameters)
                work.add_next_work(new_next_work.get_internal_id())
                # cond.add_condition_work(new_next_work)   ####### TODO:
                new_next_works.append(new_next_work)
            return new_next_works

    def add_loop_condition(self, condition, position='end'):
        self.loop_condition_template = {'position': position,
                                        'condition': condition}

    def has_loop_condition(self):
        if self.loop_condition_template and 'condition' in self.loop_condition_template:
            return True
        return False

    def get_loop_condition_status(self):
        if self.has_loop_condition():
            cond_template = self.loop_condition_template['condition']
            loop_condition = cond_template.generate_condition_from_template()
            loop_condition.load_conditions(self.works, self.get_works_template())
            self.loop_condition = loop_condition
            return self.loop_condition.get_condition_status()
        return False

    def __str__(self):
        return str(json_dumps(self))

    def get_new_works(self):
        """
        *** Function called by Marshaller agent.

        new works to be ready to start
        """
        self.sync_works()
        return [self.works[k] for k in self.new_to_run_works]

    def get_current_works(self):
        """
        *** Function called by Marshaller agent.

        Current running works
        """
        self.sync_works()
        return [self.works[k] for k in self.current_running_works]

    def get_all_works(self):
        """
        *** Function called by Marshaller agent.

        Current running works
        """
        self.sync_works()
        return [self.works[k] for k in self.works]

    def get_primary_initial_collection(self):
        """
        *** Function called by Clerk agent.
        """

        if self.primary_initial_work:
            return self.get_works_template()[self.primary_initial_work].get_primary_input_collection()
        elif self.initial_works:
            return self.get_works_template()[self.initial_works[0]].get_primary_input_collection()
        elif self.independent_works:
            return self.get_works_template()[self.independent_works[0]].get_primary_input_collection()
        else:
            keys = self.get_works_template().keys()
            return self.get_works_template()[keys[0]].get_primary_input_collection()
        return None

    def get_dependency_works(self, work_id, depth, max_depth):
        if depth > max_depth:
            return []

        deps = []
        for dep_work_id in self.work_dependencies[work_id]:
            deps.append(dep_work_id)
            l_deps = self.get_dependency_works(dep_work_id, depth + 1, max_depth)
            deps += l_deps
        deps = list(dict.fromkeys(deps))
        return deps

    def order_independent_works(self):
        ind_work_ids = self.independent_works
        self.independent_works = []
        self.work_dependencies = {}
        for ind_work_id in ind_work_ids:
            work = self.works_template[ind_work_id]
            self.work_dependencies[ind_work_id] = []
            for ind_work_id1 in ind_work_ids:
                if ind_work_id == ind_work_id1:
                    continue
                work1 = self.works_template[ind_work_id1]
                if work.depend_on(work1):
                    self.work_dependencies[ind_work_id].append(ind_work_id1)
        self.log_debug('work dependencies 1: %s' % str(self.work_dependencies))

        max_depth = len(ind_work_ids) + 1
        work_dependencies = copy.deepcopy(self.work_dependencies)
        for work_id in work_dependencies:
            deps = self.get_dependency_works(work_id, 0, max_depth)
            self.work_dependencies[work_id] = deps
        self.log_debug('work dependencies 2: %s' % str(self.work_dependencies))

        while True:
            for work_id in self.work_dependencies:
                if work_id not in self.independent_works and len(self.work_dependencies[work_id]) == 0:
                    self.independent_works.append(work_id)
            for work_id in self.independent_works:
                if work_id in self.work_dependencies:
                    del self.work_dependencies[work_id]
            for work_id in self.work_dependencies:
                for in_work_id in self.independent_works:
                    if in_work_id in self.work_dependencies[work_id]:
                        self.work_dependencies[work_id].remove(in_work_id)
            if not self.work_dependencies:
                break
        self.log_debug('independent_works: %s' % str(self.independent_works))

    def first_initialize(self):
        # set new_to_run works
        if not self.first_initial:
            self.first_initial = True
            self.order_independent_works()
            if self.initial_works:
                tostart_works = self.initial_works
            elif self.independent_works:
                tostart_works = self.independent_works
            else:
                tostart_works = list(self.get_works_template().keys())
                tostart_works = [tostart_works[0]]

            for work_id in tostart_works:
                self.get_new_work_from_template(work_id)

    def sync_works(self):
        self.first_initialize()

        self.refresh_works()

        for k in self.works:
            work = self.works[k]
            self.log_debug("work %s is_terminated(%s:%s)" % (work.get_internal_id(), work.is_terminated(), work.get_status()))

        for work in [self.works[k] for k in self.new_to_run_works]:
            if work.transforming:
                self.new_to_run_works.remove(work.get_internal_id())
                self.current_running_works.append(work.get_internal_id())

        for work in [self.works[k] for k in self.current_running_works]:
            if isinstance(work, Workflow):
                work.sync_works()
            if work.get_internal_id() in self.work_conds:
                self.log_debug("Work %s has condition dependencies %s" % (work.get_internal_id(),
                                                                          json_dumps(self.work_conds[work.get_internal_id()], sort_keys=True, indent=4)))
                for cond_id in self.work_conds[work.get_internal_id()]:
                    cond = self.conditions[cond_id]
                    self.log_debug("Work %s has condition dependencie %s" % (work.get_internal_id(),
                                                                             json_dumps(cond, sort_keys=True, indent=4)))
                    self.enable_next_works(work, cond)

            if work.is_terminated():
                self.log_info("Work %s is terminated(%s)" % (work.get_internal_id(), work.get_status()))
                self.log_debug("Work conditions: %s" % json_dumps(self.work_conds, sort_keys=True, indent=4))
                if work.get_template_id() not in self.work_conds:
                    # has no next work
                    self.log_info("Work %s has no condition dependencies" % work.get_internal_id())
                    self.terminated_works.append(work.get_internal_id())
                    self.current_running_works.remove(work.get_internal_id())
                else:
                    # self.log_debug("Work %s has condition dependencies %s" % (work.get_internal_id(),
                    #                                                           json_dumps(self.work_conds[work.get_template_id()], sort_keys=True, indent=4)))
                    # for cond in self.work_conds[work.get_template_id()]:
                    #     self.enable_next_works(work, cond)
                    self.terminated_works.append(work.get_internal_id())
                    self.current_running_works.remove(work.get_internal_id())

                if work.is_finished():
                    self.num_finished_works += 1
                elif work.is_subfinished():
                    self.num_subfinished_works += 1
                elif work.is_failed():
                    self.num_failed_works += 1
                elif work.is_expired():
                    self.num_expired_works += 1
                elif work.is_cancelled():
                    self.num_cancelled_works += 1
                elif work.is_suspended():
                    self.num_suspended_works += 1
        log_str = "num_total_works: %s" % self.num_total_works
        log_str += ", num_finished_works: %s" % self.num_finished_works
        log_str += ", num_subfinished_works: %s" % self.num_subfinished_works
        log_str += ", num_failed_works: %s" % self.num_failed_works
        log_str += ", num_expired_works: %s" % self.num_expired_works
        log_str += ", num_cancelled_works: %s" % self.num_cancelled_works
        log_str += ", num_suspended_works: %s" % self.num_suspended_works
        self.log_debug(log_str)

    def resume_works(self):
        self.num_subfinished_works = 0
        self.num_finished_works = 0
        self.num_failed_works = 0
        self.num_cancelled_works = 0
        self.num_suspended_works = 0
        self.num_expired_works = 0

        self.last_updated_at = datetime.datetime.utcnow()

        t_works = self.terminated_works
        self.terminated_works = []
        self.current_running_works = self.current_running_works + t_works
        for work in [self.works[k] for k in self.current_running_works]:
            if isinstance(work, Workflow):
                work.resume_works()
            else:
                work.resume_work()

    def clean_works(self):
        self.num_subfinished_works = 0
        self.num_finished_works = 0
        self.num_failed_works = 0
        self.num_cancelled_works = 0
        self.num_suspended_works = 0
        self.num_expired_works = 0
        self.num_total_works = 0

        self.last_updated_at = datetime.datetime.utcnow()

        self.terminated_works = []
        self.current_running_works = []
        self.works = {}
        self.work_sequence = {}  # order list

        self.first_initial = False
        self.new_to_run_works = []

    def get_exact_workflows(self):
        """
        *** Function called by Clerk agent.

        TODO: The primary dataset for the initial work is a dataset with '*'.
        workflow.primary_initial_collection = 'some datasets with *'
        collections = get_collection(workflow.primary_initial_collection)
        wfs = []
        for coll in collections:
            wf = self.copy()
            wf.name = self.name + "_" + number
            wf.primary_initial_collection = coll
            wfs.append(wf)
        return wfs
        """
        return [self]

    def is_terminated(self):
        """
        *** Function called by Marshaller agent.
        """
        self.sync_works()
        if len(self.new_to_run_works) == 0 and len(self.current_running_works) == 0:
            return True
        return False

    def is_finished(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and self.num_finished_works == self.num_total_works

    def is_subfinished(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_finished_works + self.num_subfinished_works > 0 and self.num_finished_works + self.num_subfinished_works <= self.num_total_works)

    def is_failed(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_failed_works > 0) and (self.num_cancelled_works == 0) and (self.num_suspended_works == 0) and (self.num_expired_works == 0)

    def is_to_expire(self, expired_at=None, pending_time=None, request_id=None):
        if self.expired:
            # it's already expired. avoid sending duplicated messages again and again.
            return False
        if expired_at:
            if type(expired_at) in [str]:
                expired_at = str_to_date(expired_at)
            if expired_at < datetime.datetime.utcnow():
                self.logger.info("Request(%s) expired_at(%s) is smaller than utc now(%s), expiring" % (request_id,
                                                                                                       expired_at,
                                                                                                       datetime.datetime.utcnow()))
                return True

        act_pending_time = None
        if self.pending_time:
            # in days
            act_pending_time = float(self.pending_time)
        else:
            if pending_time:
                act_pending_time = float(pending_time)
        if act_pending_time:
            act_pending_seconds = int(86400 * act_pending_time)
            if self.last_updated_at + datetime.timedelta(seconds=act_pending_seconds) < datetime.datetime.utcnow():
                log_str = "Request(%s) last updated at(%s) + pending seconds(%s)" % (request_id,
                                                                                     self.last_updated_at,
                                                                                     act_pending_seconds)
                log_str += " is smaller than utc now(%s), expiring" % (datetime.datetime.utcnow())
                self.logger.info(log_str)
                return True

        return False

    def is_expired(self):
        """
        *** Function called by Marshaller agent.
        """
        # return self.is_terminated() and (self.num_expired_works > 0)
        return self.is_terminated() and self.expired

    def is_cancelled(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_cancelled_works > 0)

    def is_suspended(self):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_suspended_works > 0)

    def get_terminated_msg(self):
        """
        *** Function called by Marshaller agent.
        """
        if self.last_work:
            return self.works[self.last_work].get_terminated_msg()
        return None

    def add_proxy(self):
        self.proxy = get_proxy()
        if not self.proxy:
            raise Exception("Cannot get local proxy")

    def get_proxy(self):
        return self.proxy


class LoopWorkflow(Base):
    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        # super(Workflow, self).__init__(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)
        self.logger = logger
        if self.logger is None:
            self.setup_logger()

        self.template = Workflow(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)
        self.num_run = 0
        self.runs = {}
        self.loop_condition_position = 'end'

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())

    @property
    def metadata(self):
        run_metadata = {'num_run': self.num_run,
                        'runs': {}}
        for run_id in self.runs:
            run_metadata['runs'][run_id] = self.runs[run_id].metadata
        return run_metadata

    @metadata.setter
    def metadata(self, value):
        run_metadata = value
        self.num_run = run_metadata['num_run']
        runs = run_metadata['runs']
        for run_id in runs:
            self.runs[run_id] = self.template.copy()
            self.runs[run_id].metadata = runs[run_id]

    def set_workload_id(self, workload_id):
        self.template.workload_id = workload_id
        # self.dynamic.workload_id = workload_id

    def get_workload_id(self):
        return self.template.workload_id

    def add_work(self, work, initial=False, primary=False):
        self.template.add_work(work, initial, primary)

    def add_condition(self, cond):
        self.template.add_condition(cond)

    def get_new_works(self):
        self.sync_works()
        self.runs[str(self.num_run)].get_new_works()

    def get_current_works(self):
        self.sync_works()
        self.runs[str(self.num_run)].get_current_works()

    def get_all_works(self):
        self.sync_works()
        self.runs[str(self.num_run)].get_all_works()

    def get_primary_initial_collection(self):
        self.runs[str(self.num_run)].get_primary_initial_collection()

    def resume_works(self):
        self.runs[str(self.num_run)].resume_works()

    def clean_works(self):
        self.runs[str(self.num_run)].clean_works()

    def is_terminated(self):
        self.runs[str(self.num_run)].is_terminated()

    def is_finished(self):
        self.runs[str(self.num_run)].is_finished()

    def is_failed(self):
        self.runs[str(self.num_run)].is_failed()

    def is_expired(self):
        self.runs[str(self.num_run)].is_expired()

    def is_cancelled(self):
        self.runs[str(self.num_run)].is_cancelled()

    def is_suspended(self):
        self.runs[str(self.num_run)].is_suspended()

    def get_terminated_msg(self):
        self.runs[str(self.num_run)].get_terminated_msg()

    def add_proxy(self):
        self.template.add_proxy()

    def get_proxy(self):
        self.template.get_proxy()

    def add_loop_condition(self, condition, position='end'):
        if not position or position != 'begin':
            position = 'end'
        position = 'end'    # force position to end currently. position = 'begin' is not supported now.
        self.template.add_loop_condition(condition, position=position)
        self.loop_condition_position = position

    def sync_works(self):
        # position is end.
        if str(self.num_run) not in self.runs:
            self.runs[str(self.num_run)] = self.template.copy()

        self.runs[str(self.num_run)].sync_works()
        if self.runs[str(self.num_run)].is_terminated():
            if self.runs[str(self.num_run)].has_loop_condition():
                if self.runs[str(self.num_run)].get_loop_condition_status():
                    self.num_run += 1
                    self.runs[str(self.num_run)] = self.template.copy()


class SubWorkflow(LoopWorkflow):
    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        """
        Init a workflow.
        """
        super(SubWorkflow, self).__init__(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)


class CompositeWorkflow(LoopWorkflow):
    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        """
        Init a workflow.
        """
        super(CompositeWorkflow, self).__init__(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)
