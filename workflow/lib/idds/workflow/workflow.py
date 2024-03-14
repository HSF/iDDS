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
import uuid


from idds.common import exceptions
from idds.common.constants import IDDSEnum, WorkStatus
from idds.common.utils import json_dumps, setup_logging, get_proxy
from idds.common.utils import str_to_date
from .base import Base
from .work import Work, Collection


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

    def copy(self):
        new_cond = copy.deepcopy(self)
        return new_cond

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

    @property
    def conditions(self):
        # return self.get_metadata_item('true_works', [])
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        self._conditions = value

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
                if work.get_internal_id() not in true_work_meta:
                    true_work_meta[work.get_internal_id()] = {'triggered': False}
            elif isinstance(work, CompositeCondition):
                if work.get_internal_id() not in true_work_meta:
                    true_work_meta[work.get_internal_id()] = {'triggered': False}
            elif isinstance(work, Workflow):
                if work.get_internal_id() not in true_work_meta:
                    true_work_meta[work.get_internal_id()] = {'triggered': False}
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
                if work.get_internal_id() not in false_work_meta:
                    false_work_meta[work.get_internal_id()] = {'triggered': False}
            elif isinstance(work, CompositeCondition):
                if work.get_internal_id() not in false_work_meta:
                    false_work_meta[work.get_internal_id()] = {'triggered': False}
            elif isinstance(work, Workflow):
                if work.get_internal_id() not in false_work_meta:
                    false_work_meta[work.get_internal_id()] = {'triggered': False}
        self.add_metadata_item('false_works', false_work_meta)

    def validate_conditions(self, conditions):
        if type(conditions) not in [tuple, list]:
            raise exceptions.IDDSException("conditions must be list")
        for cond in conditions:
            assert(inspect.ismethod(cond))

    def add_condition(self, cond):
        assert(inspect.ismethod(cond))
        assert(isinstance(cond.__self__, Work))

        # self.conditions.append({'condition': cond, 'current_work': cond.__self__})

        self._conditions.append(cond)

    def load_metadata(self):
        # conditions = self.get_metadata_item('conditions', [])
        # true_works_meta = self.get_metadata_item('true_works', {})
        # false_works_meta = self.get_metadata_item('false_works', {})
        pass

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
                            if isinstance(cond.__self__, Work):
                                new_cond = {'idds_method': cond.__name__,
                                            'idds_method_internal_id': cond.__self__.get_internal_id()}
                            elif isinstance(cond.__self__, CompositeCondition):
                                new_cond = {'idds_method': cond.__name__,
                                            'idds_method_condition': cond.__self__.to_dict()}
                            elif isinstance(cond.__self__, Workflow):
                                new_cond = {'idds_method': cond.__name__,
                                            'idds_method_internal_id': cond.__self__.get_internal_id()}
                            else:
                                new_cond = {'idds_method': cond.__name__,
                                            'idds_method_internal_id': cond.__self__.get_internal_id()}
                        else:
                            if hasattr(cond, '__self__'):
                                new_cond = {'idds_attribute': cond.__name__,
                                            'idds_method_internal_id': cond.__self__.get_internal_id()}
                            else:
                                new_cond = cond
                        new_value.append(new_cond)
                    value = new_value
                elif key in ['_true_works', '_false_works']:
                    new_value = []
                    for w in value:
                        if isinstance(w, Work):
                            new_w = w.get_internal_id()
                        elif isinstance(w, CompositeCondition):
                            new_w = w.to_dict()
                        elif isinstance(w, Workflow):
                            # new_w = w.to_dict()
                            new_w = w.get_internal_id()
                        else:
                            new_w = w
                        new_value.append(new_w)
                    value = new_value
                else:
                    value = self.to_dict_l(value)
                ret['attributes'][key] = value
        return ret

    def get_work_from_id(self, work_id, works):
        return works[work_id]

    def load_conditions(self, works):
        new_conditions = []
        for cond in self.conditions:
            if callable(cond):
                new_conditions.append(cond)
            else:
                if 'idds_method' in cond and 'idds_method_internal_id' in cond:
                    self.logger.debug("idds_method_internal_id: %s" % cond['idds_method_internal_id'])
                    self.logger.debug("idds_method: %s" % cond['idds_method'])

                    internal_id = cond['idds_method_internal_id']
                    work = self.get_work_from_id(internal_id, works)

                    self.logger.debug("get_work_from_id: %s: [%s]" % (internal_id, [work]))

                    if work is not None:
                        new_cond = getattr(work, cond['idds_method'])
                    else:
                        self.logger.error("Condition method work cannot be found for %s" % (internal_id))
                        new_cond = cond
                elif 'idds_attribute' in cond and 'idds_method_internal_id' in cond:
                    internal_id = cond['idds_method_internal_id']
                    work = self.get_work_from_id(internal_id, works)
                    if work is not None:
                        new_cond = getattr(work, cond['idds_attribute'])
                    else:
                        self.logger.error("Condition attribute work cannot be found for %s" % (internal_id))
                        new_cond = cond
                elif 'idds_method' in cond and 'idds_method_condition' in cond:
                    new_cond = cond['idds_method_condition']
                    new_cond = getattr(new_cond, cond['idds_method'])
                else:
                    new_cond = cond
                new_conditions.append(new_cond)
        self.conditions = new_conditions

        new_true_works = []
        self.logger.debug("true_works: %s" % str(self.true_works))

        for w in self.true_works:
            # self.logger.debug("true_work: %s" % str(w))
            if isinstance(w, CompositeCondition):
                # work = w.load_conditions(works, works_template)
                w.load_conditions(works)
                work = w
            elif isinstance(w, Workflow):
                work = w
            elif isinstance(w, Work):
                work = w
            elif type(w) in [str]:
                work = self.get_work_from_id(w, works)
                if work is None:
                    self.logger.error("True work cannot be found for %s" % str(w))
                    work = w
            else:
                self.logger.error("True work cannot be found for type(%s): %s" % (type(w), str(w)))
                work = w
            new_true_works.append(work)
        self.true_works = new_true_works

        new_false_works = []
        for w in self.false_works:
            if isinstance(w, CompositeCondition):
                # work = w.load_condtions(works, works_template)
                w.load_conditions(works)
                work = w
            elif isinstance(w, Workflow):
                work = w
            elif isinstance(w, Work):
                work = w
            elif type(w) in [str]:
                work = self.get_work_from_id(w, works)
                if work is None:
                    self.logger.error("False work cannot be found for type(%s): %s" % (type(w), str(w)))
                    work = w
            else:
                self.logger.error("False work cannot be found for %s" % str(w))
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
                if isinstance(cond.__self__, Work) or isinstance(cond.__self__, Workflow):
                    works.append(cond.__self__.get_internal_id())
                elif isinstance(cond.__self__, CompositeCondition):
                    works = works + cond.__self__.all_condition_ids()
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
                if isinstance(cond.__self__, Work) or isinstance(cond.__self__, Workflow):
                    works.append(cond.__self__)
                elif isinstance(cond.__self__, CompositeCondition):
                    works = works + cond.__self__.all_pre_works()
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
                if isinstance(work, CompositeCondition):
                    works = works + work.get_next_works(trigger=trigger)
                else:
                    if work.get_internal_id() not in true_work_meta:
                        true_work_meta[work.get_internal_id()] = {'triggered': False}
                    if trigger == ConditionTrigger.ToTrigger:
                        if not true_work_meta[work.get_internal_id()]['triggered']:
                            true_work_meta[work.get_internal_id()]['triggered'] = True
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
                if isinstance(work, CompositeCondition):
                    works = works + work.get_next_works(trigger=trigger)
                else:
                    if work.get_internal_id() not in false_work_meta:
                        false_work_meta[work.get_internal_id()] = {'triggered': False}
                    if trigger == ConditionTrigger.ToTrigger:
                        if not false_work_meta[work.get_internal_id()]['triggered']:
                            false_work_meta[work.get_internal_id()]['triggered'] = True
                            works.append(work)
                    elif trigger == ConditionTrigger.NotTriggered:
                        if not false_work_meta[work.get_internal_id()]['triggered']:
                            works.append(work)
                    elif trigger == ConditionTrigger.Triggered:
                        if false_work_meta[work.get_internal_id()]['triggered']:
                            works.append(work)
            self.add_metadata_item('false_works', false_work_meta)
        return works


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
        super(ParameterLink, self).__init__()
        self.parameters = {}
        self.num_parameters = 0
        if parameters:
            if type(parameters) not in [list, tuple]:
                parameters = [parameters]
            for p in parameters:
                if p:
                    if type(p) in [str]:
                        self.parameters[str(self.num_parameters)] = {'source': p, 'destination': p}
                        self.num_parameters += 1
                    elif type(p) in [dict] and 'source' in p and 'destination' in p:
                        self.parameters[str(self.num_parameters)] = {'source': p['source'], 'destination': p['destination']}
                        self.num_parameters += 1
                    else:
                        raise Exception("Cannot parse the parameters format. Accepted format: list of string or dict{'source': <>, 'destination': <>}")

        self.internal_id = str(uuid.uuid4())[:8]
        self.template_id = self.internal_id

    def get_internal_id(self):
        return self.internal_id

    def get_parameter_value(self, work, p):
        ret = None
        p_f = getattr(work, p, 'None')
        if p_f:
            if callable(p_f):
                ret = p_f()
            else:
                ret = p_f
        else:
            ret = None
        if ret and type(ret) in [Collection] and hasattr(ret, 'to_origin_dict'):
            ret = ret.to_origin_dict()
        return ret

    def set_parameters(self, work):
        p_values = {}
        for p in self.parameters:
            p_values[p] = self.get_parameter_value(work, self.parameters[p]['source'])
        self.add_metadata_item('parameters', p_values)

    def get_parameters(self):
        p_values = self.get_metadata_item('parameters', {})
        ret = {}
        for p in self.parameters:
            if p in p_values:
                ret[self.parameters[p]['destination']] = p_values[p]
        return ret


class WorkflowBase(Base):

    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        """
        Init a workflow.
        """
        self._works = {}
        self._conditions = {}
        self._work_conds = {}

        self.parameter_links = {}
        self.parameter_links_source = {}
        self.parameter_links_destination = {}

        self._global_parameters = {}

        super(WorkflowBase, self).__init__()

        self.internal_id = str(uuid.uuid4())[:8]
        self.template_work_id = self.internal_id
        # self.template_work_id = str(uuid.uuid4())[:8]
        self.lifetime = lifetime
        self.pending_time = pending_time

        if name:
            # self._name = name + "." + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))
            self._name = name
        else:
            self._name = 'idds.workflow.' + datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f") + str(random.randint(1, 1000))

        if workload_id is None:
            # workload_id = int(time.time())
            pass
        self.workload_id = workload_id

        self.logger = logger
        if self.logger is None:
            self.setup_logger()

        self._works = {}
        self.works = {}
        self.work_sequence = {}  # order list

        self.next_works = []

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

        self._loop_condition_position = 'end'
        self.loop_condition = None

        self.num_run = None

        self.global_parameters = {}

        self.to_cancel = False
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
                    work_metadata[k] = {'type': 'workflow',
                                        'metadata': work.metadata}
                else:
                    work_metadata[k] = {'type': 'work',
                                        'work_id': work.work_id,
                                        'workload_id': work.workload_id,
                                        'external_id': work.external_id,
                                        'status': work.status.value if work.status else work.status,
                                        'substatus': work.substatus.value if work.substatus else work.substatus,
                                        'next_works': work.next_works,
                                        'transforming': work.transforming}
        self.add_metadata_item('works', work_metadata)

    def refresh_works(self):
        work_metadata = {}
        if self._works:
            for k in self._works:
                work = self._works[k]
                if isinstance(work, Workflow):
                    work.refresh_works()
                    work_metadata[k] = {'type': 'workflow',
                                        'metadata': work.metadata}
                else:
                    work_metadata[k] = {'type': 'work',
                                        'work_id': work.work_id,
                                        'workload_id': work.workload_id,
                                        'external_id': work.external_id,
                                        'status': work.status.value if work.status else work.status,
                                        'substatus': work.substatus.value if work.substatus else work.substatus,
                                        'next_works': work.next_works,
                                        'transforming': work.transforming}
                if work.last_updated_at and (not self.last_updated_at or work.last_updated_at > self.last_updated_at):
                    self.last_updated_at = work.last_updated_at
        self.add_metadata_item('works', work_metadata)

    def load_works(self):
        work_metadata = self.get_metadata_item('works', {})
        for k in self._works:
            if k in work_metadata:
                if work_metadata[k]['type'] == 'work':
                    self._works[k].work_id = work_metadata[k]['work_id']
                    self._works[k].workload_id = work_metadata[k]['workload_id'] if 'workload_id' in work_metadata[k] else None
                    self._works[k].external_id = work_metadata[k]['external_id'] if 'external_id' in work_metadata[k] else None
                    self._works[k].transforming = work_metadata[k]['transforming']
                    self._works[k].status = WorkStatus(work_metadata[k]['status']) if work_metadata[k]['status'] else work_metadata[k]['status']
                    self._works[k].substatus = WorkStatus(work_metadata[k]['substatus']) if work_metadata[k]['substatus'] else work_metadata[k]['substatus']
                    self._works[k].next_works = work_metadata[k]['next_works'] if 'next_works' in work_metadata[k] else []
                elif work_metadata[k]['type'] == 'workflow':
                    self._works[k].metadata = work_metadata[k]['metadata']

            work = self._works[k]
            if work.last_updated_at and (not self.last_updated_at or work.last_updated_at > self.last_updated_at):
                self.last_updated_at = work.last_updated_at

    @property
    def next_works(self):
        return self.get_metadata_item('next_works', [])

    @next_works.setter
    def next_works(self, value):
        self.add_metadata_item('next_works', value)

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        self._conditions = value
        conditions_metadata = {}
        if self._conditions:
            for k in self._conditions:
                conditions_metadata[k] = self._conditions[k].metadata
        self.add_metadata_item('conditions', conditions_metadata)

    @property
    def work_conds(self):
        return self._work_conds

    @work_conds.setter
    def work_conds(self, value):
        self._work_conds = value
        # self.add_metadata_item('work_conds', value)

    def load_work_conditions(self):
        conditions_metadata = self.get_metadata_item('conditions', {})
        for cond_internal_id in self._conditions:
            if cond_internal_id in conditions_metadata:
                self.conditions[cond_internal_id].metadata = conditions_metadata[cond_internal_id]
            if isinstance(self.conditions[cond_internal_id], Workflow):
                # self.conditions[cond_internal_id].load_conditions(self.works)
                pass
            elif isinstance(self.conditions[cond_internal_id], Work):
                # self.conditions[cond_internal_id].load_conditions(self.works)
                pass
            elif isinstance(self.conditions[cond_internal_id], CompositeCondition):
                self.conditions[cond_internal_id].load_conditions(self.works)
                pass
        # work_conds = self.get_metadata_item('work_conds', {})
        # self._work_conds = work_conds

    @property
    def global_parameters(self):
        self._global_parameters = self.get_metadata_item('gp', {})
        return self._global_parameters

    @global_parameters.setter
    def global_parameters(self, value):
        self._global_parameters = value
        gp_metadata = {}
        if self._global_parameters:
            for key in self._global_parameters:
                if key.startswith("user_"):
                    gp_metadata[key] = self._global_parameters[key]
                else:
                    self.logger.warn("Only parameters start with 'user_' can be set as global parameters. The parameter '%s' will be ignored." % (key))
        self.add_metadata_item('gp', gp_metadata)

    def set_global_parameters(self, value):
        self.global_parameters = value

    @property
    def sliced_global_parameters(self):
        self._sliced_global_parameters = self.get_metadata_item('sliced_gp', {})
        return self._sliced_global_parameters

    @sliced_global_parameters.setter
    def sliced_global_parameters(self, value):
        self._sliced_global_parameters = value
        gp_metadata = {}
        if self._sliced_global_parameters:
            for key in self._sliced_global_parameters:
                if key.startswith("user_"):
                    gp_metadata[key] = self._sliced_global_parameters[key]
                else:
                    self.logger.warn("Only parameters start with 'user_' can be set as global parameters. The parameter '%s' will be ignored." % (key))
        self.add_metadata_item('sliced_gp', gp_metadata)

    def set_sliced_global_parameters(self, source, name=None, index=0):
        sliced_global_parameters = self.sliced_global_parameters
        sliced_global_parameters[source] = {'name': name, 'index': index}
        # to trigger the setter function
        self.sliced_global_parameters = self.sliced_global_parameters

    def sync_global_parameters_from_work(self, work):
        self.log_debug("work %s is_terminated, global_parameters: %s" % (work.get_internal_id(), str(self.global_parameters)))
        if self.global_parameters:
            for key in self.global_parameters:
                status, value = work.get_global_parameter_from_output_data(key)
                self.log_debug("work %s get_global_parameter_from_output_data(key: %s) results(%s:%s)" % (work.get_internal_id(), key, status, value))
                if status:
                    self.global_parameters[key] = value
                elif hasattr(work, key):
                    self.global_parameters[key] = getattr(work, key)
        self.set_global_parameters(self.global_parameters)

    @property
    def loop_condition(self):
        return self._loop_condition

    @loop_condition.setter
    def loop_condition(self, value):
        # self._loop_condition_position = position
        self._loop_condition = value
        if self._loop_condition:
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
    def to_start_works(self):
        return self.get_metadata_item('to_start_works', [])

    @to_start_works.setter
    def to_start_works(self, value):
        self.add_metadata_item('to_start_works', value)

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
    def init_works(self):
        return self.get_metadata_item('init_works', [])

    @init_works.setter
    def init_works(self, value):
        self.add_metadata_item('init_works', value)

    @property
    def to_update_transforms(self):
        return self.get_metadata_item('to_update_transforms', {})

    @to_update_transforms.setter
    def to_update_transforms(self, value):
        self.add_metadata_item('to_update_transforms', value)

    @property
    def num_run(self):
        return self.get_metadata_item('num_run', 0)

    @num_run.setter
    def num_run(self, value):
        self.add_metadata_item('num_run', value)

    @property
    def to_cancel(self):
        return self.get_metadata_item('to_cancel', False)

    @to_cancel.setter
    def to_cancel(self, value):
        self.add_metadata_item('to_cancel', value)

    def load_metadata(self):
        self.load_works()
        self.load_work_conditions()
        self.load_parameter_links()

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

    def get_works(self):
        return self.works

    def get_new_work_to_run(self, work_id, new_parameters=None):
        # 1. initialize works
        # template_id = work.get_template_id()
        work = self.works[work_id]
        work.workload_id = None

        if isinstance(work, Workflow):
            work.parent_num_run = self.num_run
            work.sync_works(to_cancel=self.to_cancel)

            work.sequence_id = self.num_total_works

            works = self.works
            self.works = works
            # self.work_sequence.append(new_work.get_internal_id())
            self.work_sequence[str(self.num_total_works)] = work.get_internal_id()
            self.num_total_works += 1
            self.new_to_run_works.append(work.get_internal_id())
            self.last_work = work.get_internal_id()
        else:
            new_parameters = self.get_destination_parameters(work_id)
            if new_parameters:
                work.set_parameters(new_parameters)
            work.sequence_id = self.num_total_works

            work.num_run = self.num_run
            work.initialize_work()
            work.sync_global_parameters(self.global_parameters, self.sliced_global_parameters)
            work.renew_parameters_from_attributes()
            if work.parent_workload_id is None and self.num_total_works > 0:
                last_work_id = self.work_sequence[str(self.num_total_works - 1)]
                last_work = self.works[last_work_id]
                work.parent_workload_id = last_work.workload_id
                last_work.add_next_work(work.get_internal_id())
            works = self.works
            self.works = works
            # self.work_sequence.append(new_work.get_internal_id())
            self.work_sequence[str(self.num_total_works)] = work.get_internal_id()
            self.num_total_works += 1
            self.new_to_run_works.append(work.get_internal_id())
            self.last_work = work.get_internal_id()

        return work

    def get_new_parameters_for_work(self, work):
        new_parameters = self.get_destination_parameters(work.get_internal_id())
        if new_parameters:
            work.set_parameters(new_parameters)
        work.sequence_id = self.num_total_works

        work.initialize_work()
        work.sync_global_parameters(self.global_parameters, self.sliced_global_parameters)
        work.renew_parameters_from_attributes()
        works = self.works
        self.works = works
        return work

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

    def get_site(self):
        try:
            work_id = self.primary_initial_work
            if not work_id:
                work_id = list(self.works.keys())[0]
            work = self.works[work_id]
            return work.get_site()
        except Exception:
            pass
        return None

    def add_initial_works(self, work):
        self.initial_works.append(work.get_internal_id())
        if self.primary_initial_work is None:
            self.primary_initial_work = work.get_internal_id()

    def add_work(self, work, initial=False, primary=False):
        self.first_initial = False
        self.works[work.get_internal_id()] = work
        if initial:
            if primary:
                self.primary_initial_work = work.get_internal_id()
            self.add_initial_works(work)

        self.independent_works.append(work.get_internal_id())

    def has_to_build_work(self):
        return False

    def add_condition(self, cond):
        self.first_initial = False
        cond_works = cond.all_works()
        for cond_work in cond_works:
            assert(cond_work.get_internal_id() in self.get_works())

        conditions = self.conditions
        conditions[cond.get_internal_id()] = cond
        self.conditions = conditions

        # if cond.current_work not in self.work_conds:
        #     self.work_conds[cond.current_work] = []
        # self.work_conds[cond.current_work].append(cond)
        work_conds = self.work_conds
        for work in cond.all_pre_works():
            if work.get_internal_id() not in work_conds:
                work_conds[work.get_internal_id()] = []
            work_conds[work.get_internal_id()].append(cond.get_internal_id())
        self.work_conds = work_conds

        # if a work is a true_work or false_work of a condition,
        # should remove it from independent_works
        cond_next_works = cond.all_next_works()
        for next_work in cond_next_works:
            if next_work.get_internal_id() in self.independent_works:
                self.independent_works.remove(next_work.get_internal_id())

    def find_workflow_from_work(self, work):
        if work.get_internal_id() in self._works:
            return self
        else:
            for k in self._works:
                wk = self._works[k]
                if isinstance(wk, Workflow):
                    wf = wk.find_workflow_from_work(work)
                    if wf:
                        return wf
        return None

    def add_parameter_link(self, work_source, work_destinations, parameter_link):
        wf_s = self.find_workflow_from_work(work_source)
        if not wf_s:
            raise Exception("Cannot find work %s in the workflow." % work_source.get_internal_id())
        if work_source.get_internal_id() not in wf_s.parameter_links_source:
            wf_s.parameter_links_source[work_source.get_internal_id()] = []
        wf_s.parameter_links_source[work_source.get_internal_id()].append(parameter_link.get_internal_id())

        if type(work_destinations) not in [list, tuple]:
            work_destinations = [work_destinations]
        for work_destination in work_destinations:
            wf = self.find_workflow_from_work(work_destination)
            if not wf:
                raise Exception("Cannot find work %s in the workflow." % work_destination.get_internal_id())
            if parameter_link.get_internal_id() not in wf.parameter_links:
                wf.parameter_links[parameter_link.get_internal_id()] = parameter_link
                if work_destination.get_internal_id() not in wf.parameter_links_destination:
                    wf.parameter_links_destination[work_destination.get_internal_id()] = []
                wf.parameter_links_destination[work_destination.get_internal_id()].append(parameter_link.get_internal_id())

    def find_parameter_links_from_id(self, internal_id):
        rets = []
        if internal_id in self.parameter_links:
            rets.append((self, self.parameter_links[internal_id]))
        for k in self._works:
            wk = self._works[k]
            if isinstance(wk, Workflow):
                links = wk.find_parameter_links_from_id(internal_id)
                rets = rets + links
        return rets

    def refresh_parameter_links(self):
        p_metadata = {}
        for internal_id in self.parameter_links:
            p_metadata[internal_id] = self.parameter_links[internal_id].metadata
        self.add_metadata_item('parameter_links', p_metadata)

    def get_parameter_links_metadata(self):
        p_metadata = {}
        for internal_id in self.parameter_links:
            p_metadata[internal_id] = self.parameter_links[internal_id].metadata
        self.add_metadata_item('parameter_links', p_metadata)
        return p_metadata

    def set_parameter_links_metadata(self, p_links):
        for internal_id in self.parameter_links:
            if internal_id in p_links:
                p_metadata = p_links[internal_id]
                self.parameter_links[internal_id].metadata = p_metadata

    def set_source_parameters(self, internal_id):
        work = self.works[internal_id]
        # if type(work) in [Work]:
        #     print(work.work_id)
        #     print(internal_id)
        #     print(self.parameter_links_source)
        if internal_id in self.parameter_links_source:
            for p_id in self.parameter_links_source[internal_id]:
                # print(p_id)
                p_links = self.find_parameter_links_from_id(p_id)
                # print(p_links)
                for wf, p_link in p_links:
                    p_link.set_parameters(work)
                    wf.refresh_parameter_links()

    def get_destination_parameters(self, internal_id):
        # work = self.works[internal_id]
        parameters = {}
        if internal_id in self.parameter_links_destination:
            for p_id in self.parameter_links_destination[internal_id]:
                p_link = self.parameter_links[p_id]
                parameters.update(p_link.get_parameters())
        return parameters

    def load_parameter_links(self):
        p_metadata = self.get_metadata_item('parameter_links', {})
        for p_id in self.parameter_links:
            if p_id in p_metadata:
                self.parameter_links[p_id].metadata = p_metadata[p_id]

    def add_next_work(self, work_id):
        next_works = self.next_works
        next_works.append(work_id)
        self.next_works = next_works

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
                # parameters = self.get_destination_parameters(next_work.get_internal_id())
                new_next_work = self.get_new_work_to_run(next_work.get_internal_id())
                work.add_next_work(new_next_work.get_internal_id())
                new_next_work.parent_workload_id = work.workload_id
                # cond.add_condition_work(new_next_work)   ####### TODO:
                new_next_works.append(new_next_work)
            return new_next_works

    def add_loop_condition(self, condition, position='end'):
        self.loop_condition_position = position
        self.loop_condition = condition

    def has_loop_condition(self):
        if self.loop_condition:
            return True
        return False

    def get_loop_condition_status(self):
        if self.has_loop_condition():
            self.loop_condition.load_conditions(self.works)
            # self.logger.debug("Loop condition %s" % (json_dumps(self.loop_condition, sort_keys=True, indent=4)))
            return self.loop_condition.get_condition_status()
        return False

    def __str__(self):
        return str(json_dumps(self))

    def get_new_works(self):
        """
        *** Function called by Marshaller agent.

        new works to be ready to start
        """
        if self.to_cancel:
            return []

        self.sync_works(to_cancel=self.to_cancel)
        works = []

        if self.to_start_works:
            init_works = self.init_works
            to_start_works = self.to_start_works
            work_id = to_start_works.pop(0)
            self.to_start_works = to_start_works
            self.get_new_work_to_run(work_id)
            if not init_works:
                init_works.append(work_id)
                self.init_works = init_works

        for k in self.new_to_run_works:
            if isinstance(self.works[k], Work):
                self.works[k] = self.get_new_parameters_for_work(self.works[k])
                works.append(self.works[k])
            if isinstance(self.works[k], Workflow):
                works = works + self.works[k].get_new_works()
        for k in self.current_running_works:
            if isinstance(self.works[k], Workflow):
                works = works + self.works[k].get_new_works()
        return works

    def get_current_works(self):
        """
        *** Function called by Marshaller agent.

        Current running works
        """
        self.sync_works(to_cancel=self.to_cancel)
        works = []
        for k in self.current_running_works:
            if isinstance(self.works[k], Work):
                works.append(self.works[k])
            if isinstance(self.works[k], Workflow):
                works = works + self.works[k].get_current_works()
        return works

    def get_all_works(self):
        """
        *** Function called by Marshaller agent.

        Current running works
        """
        self.sync_works(to_cancel=self.to_cancel)

        works = []
        for k in self.works:
            if isinstance(self.works[k], Work):
                works.append(self.works[k])
            if isinstance(self.works[k], Workflow):
                works = works + self.works[k].get_all_works()
        return works

    def get_primary_initial_collection(self):
        """
        *** Function called by Clerk agent.
        """

        if self.primary_initial_work:
            if isinstance(self.get_works()[self.primary_initial_work], Workflow):
                return self.get_works()[self.primary_initial_work].get_primary_initial_collection()
            else:
                return self.get_works()[self.primary_initial_work].get_primary_input_collection()
        elif self.initial_works:
            if isinstance(self.get_works()[self.initial_works[0]], Workflow):
                return self.get_works()[self.initial_works[0]].get_primary_initial_collection()
            else:
                return self.get_works()[self.initial_works[0]].get_primary_input_collection()
        elif self.independent_works:
            if isinstance(self.get_works()[self.independent_works[0]], Workflow):
                return self.get_works()[self.independent_works[0]].get_primary_initial_collection()
            else:
                return self.get_works()[self.independent_works[0]].get_primary_input_collection()
        else:
            keys = self.get_works().keys()
            if isinstance(self.get_works()[keys[0]], Workflow):
                return self.get_works()[keys[0]].get_primary_initial_collection()
            else:
                return self.get_works()[keys[0]].get_primary_input_collection()
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
        self.log_debug("ordering independent works")
        ind_work_ids = self.independent_works
        self.log_debug("independent works: %s" % (str(ind_work_ids)))
        self.independent_works = []
        self.work_dependencies = {}
        for ind_work_id in ind_work_ids:
            work = self.works[ind_work_id]
            self.work_dependencies[ind_work_id] = []
            for ind_work_id1 in ind_work_ids:
                if ind_work_id == ind_work_id1:
                    continue
                work1 = self.works[ind_work_id1]
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
            # self.log_debug('independent_works N: %s' % str(self.independent_works))
            # self.log_debug('work dependencies N: %s' % str(self.work_dependencies))
            has_changes = False
            for work_id in self.work_dependencies:
                if work_id not in self.independent_works and len(self.work_dependencies[work_id]) == 0:
                    self.independent_works.append(work_id)
                    has_changes = True
            for work_id in self.independent_works:
                if work_id in self.work_dependencies:
                    del self.work_dependencies[work_id]
                    has_changes = True
            for work_id in self.work_dependencies:
                for in_work_id in self.independent_works:
                    if in_work_id in self.work_dependencies[work_id]:
                        self.work_dependencies[work_id].remove(in_work_id)
                        has_changes = True
            if not self.work_dependencies:
                break
            if not has_changes:
                self.log_debug("There are loop dependencies between works.")
                self.log_debug('independent_works N: %s' % str(self.independent_works))
                self.log_debug('work dependencies N: %s' % str(self.work_dependencies))
                for work_id in self.work_dependencies:
                    if work_id not in self.independent_works:
                        self.independent_works.append(work_id)
                break
        self.log_debug('independent_works: %s' % str(self.independent_works))
        self.log_debug("ordered independent works")

    def first_initialize(self):
        # set new_to_run works
        if not self.first_initial:
            self.log_debug("first initializing")
            self.first_initial = True
            self.order_independent_works()
            if self.initial_works:
                tostart_works = self.initial_works
            elif self.independent_works:
                tostart_works = self.independent_works
            else:
                tostart_works = list(self.get_works().keys())
                tostart_works = [tostart_works[0]]

            to_start_works = self.to_start_works
            for work_id in tostart_works:
                to_start_works.append(work_id)
            self.to_start_works = to_start_works
            self.log_debug("first initialized")

    def sync_works(self, to_cancel=False):
        if to_cancel:
            self.to_cancel = to_cancel
        self.log_debug("synchroning works")
        self.first_initialize()

        self.refresh_works()

        for k in self.works:
            work = self.works[k]
            self.log_debug("work %s is_terminated(%s:%s)" % (work.get_internal_id(), work.is_terminated(), work.get_status()))

            if work.get_internal_id() not in self.current_running_works and work.get_status() in [WorkStatus.Transforming]:
                self.current_running_works.append(work.get_internal_id())

        for work in [self.works[k] for k in self.new_to_run_works]:
            if work.transforming:
                self.new_to_run_works.remove(work.get_internal_id())
                if work.get_internal_id() not in self.current_running_works:
                    self.current_running_works.append(work.get_internal_id())

        for work in [self.works[k] for k in self.current_running_works]:
            if isinstance(work, Workflow):
                work.sync_works(to_cancel=self.to_cancel)

            if work.is_terminated():
                self.log_debug("work %s is_terminated, sync_global_parameters_from_work" % (work.get_internal_id()))
                self.set_source_parameters(work.get_internal_id())
                self.sync_global_parameters_from_work(work)

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
                if work.get_internal_id() not in self.work_conds:
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

        self.num_finished_works = 0
        self.num_subfinished_works = 0
        self.num_failed_works = 0
        self.num_expired_works = 0
        self.num_cancelled_works = 0
        self.num_suspended_works = 0

        for k in self.works:
            work = self.works[k]
            if work.is_terminated():
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

            # if work.is_terminated():
            #    # if it's a loop workflow, to generate new loop
            #    if isinstance(work, Workflow):
            #        work.sync_works()
        log_str = "num_total_works: %s" % self.num_total_works
        log_str += ", num_finished_works: %s" % self.num_finished_works
        log_str += ", num_subfinished_works: %s" % self.num_subfinished_works
        log_str += ", num_failed_works: %s" % self.num_failed_works
        log_str += ", num_expired_works: %s" % self.num_expired_works
        log_str += ", num_cancelled_works: %s" % self.num_cancelled_works
        log_str += ", num_suspended_works: %s" % self.num_suspended_works
        self.log_debug(log_str)

        self.refresh_works()
        self.log_debug("synchronized works")

    def resume_works(self):
        self.to_cancel = False
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

    def get_relation_data(self, work):
        ret = {'work': {'workload_id': work.workload_id,
                        'external_id': work.external_id,
                        'work_name': work.get_work_name()}}
        if hasattr(work, 'get_ancestry_works'):
            ret['work']['ancestry_works'] = work.get_ancestry_works()

        next_works = work.next_works
        if next_works:
            next_works_data = []
            for next_id in next_works:
                next_work = self.works[next_id]
                if isinstance(next_work, Workflow):
                    next_work_data = next_work.get_relation_map()
                else:
                    next_work_data = self.get_relation_data(next_work)
                next_works_data.append(next_work_data)
            ret['next_works'] = next_works_data
        return ret

    def organzie_based_on_ancestry_works(self, works):
        new_ret = []

        ordered_items = {}
        left_items = []
        for item in works:
            if type(item) in [dict]:
                if 'ancestry_works' not in item['work'] or not item['work']['ancestry_works']:
                    new_ret.append(item)
                    ordered_items[item['work']['work_name']] = item
                else:
                    # ancestry_works = item['work']['ancestry_works']
                    left_items.append(item)
            elif type(item) in [list]:
                # subworkflow
                # work_names, ancestry_works = self.get_workflow_ancestry_works(item)
                # if not ancestry_works:
                #     new_ret.append(item)
                # currently now support to use dependency_map to depend_on a workflow.
                # depending on a workflow should use Condition. It's already processed.
                new_ret.append(item)
        while True:
            new_left_items = left_items
            left_items = []
            has_updates = False
            for item in new_left_items:
                ancestry_works = item['work']['ancestry_works']
                all_ancestry_ready = True
                for work_name in ancestry_works:
                    if work_name not in ordered_items and work_name != item['work']['work_name']:
                        all_ancestry_ready = False
                if all_ancestry_ready:
                    for work_name in ancestry_works:
                        if work_name != item['work']['work_name']:
                            if 'next_works' not in ordered_items[work_name]:
                                ordered_items[work_name]['next_works'] = [item]
                            else:
                                ordered_items[work_name]['next_works'].append(item)
                            has_updates = True
                            ordered_items[item['work']['work_name']] = item
                else:
                    left_items.append(item)
            if not has_updates or not left_items:
                break
        for item in left_items:
            new_ret.append(item)
        return new_ret

    def get_relation_map(self):
        ret = []
        init_works = self.init_works
        for internal_id in init_works:
            if isinstance(self.works[internal_id], Workflow):
                work_data = self.works[internal_id].get_relation_map()
            else:
                work_data = self.get_relation_data(self.works[internal_id])
            ret.append(work_data)
        ret = self.organzie_based_on_ancestry_works(ret)
        return ret

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
        # self.works = {}
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

    def is_terminated(self, synchronize=True):
        """
        *** Function called by Marshaller agent.
        """
        self.sync_works(to_cancel=self.to_cancel)
        if (self.to_cancel or len(self.new_to_run_works) == 0) and len(self.current_running_works) == 0:
            return True
        return False

    def is_finished(self, synchronize=True):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and self.num_finished_works == self.num_total_works

    def is_subfinished(self, synchronize=True):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_finished_works + self.num_subfinished_works > 0 and self.num_finished_works + self.num_subfinished_works <= self.num_total_works)

    def is_processed(self, synchronize=True):
        """
        *** Function called by Transformer agent.
        """
        return self.is_terminated(synchronize=synchronize) and (self.num_finished_works + self.num_subfinished_works > 0 and self.num_finished_works + self.num_subfinished_works <= self.num_total_works)

    def is_failed(self, synchronize=True):
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

    def is_expired(self, synchronize=True):
        """
        *** Function called by Marshaller agent.
        """
        # return self.is_terminated() and (self.num_expired_works > 0)
        return self.is_terminated() and self.expired

    def is_cancelled(self, synchronize=True):
        """
        *** Function called by Marshaller agent.
        """
        return self.is_terminated() and (self.num_cancelled_works > 0)

    def is_suspended(self, synchronize=True):
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

    def get_status(self):
        if self.is_terminated():
            if self.is_finished():
                return WorkStatus.Finished
            elif self.is_subfinished():
                return WorkStatus.SubFinished
            elif self.is_failed():
                return WorkStatus.Failed
            elif self.is_expired():
                return WorkStatus.Expired
            elif self.is_cancelled():
                return WorkStatus.Cancelled
            elif self.is_suspended():
                return WorkStatus.Suspended
        return WorkStatus.Transforming

    def depend_on(self, work):
        return False

    def add_proxy(self):
        self.proxy = get_proxy()
        if not self.proxy:
            raise Exception("Cannot get local proxy")

    def get_proxy(self):
        return self.proxy


class Workflow(Base):
    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        # super(Workflow, self).__init__(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)
        self.logger = logger
        if self.logger is None:
            self.setup_logger()

        self.template = WorkflowBase(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)
        self.parent_num_run = None
        self._num_run = 0
        self.runs = {}
        self.loop_condition_position = 'end'
        self.origin_metadata = None

        # for old idds version
        t_works = self.template.works
        if not t_works and hasattr(self, 'works_template'):
            self.template.works = self.works_template

    def setup_logger(self):
        # Setup logger
        self.logger = logging.getLogger(self.get_class_name())

    def log_info(self, info):
        if self.logger is None:
            self.setup_logger()
        self.logger.info(info)

    def log_debug(self, info):
        if self.logger is None:
            self.setup_logger()
        self.logger.debug(info)

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

    def get_template_id(self):
        return self.template.get_template_id()

    @property
    def metadata(self):
        run_metadata = {'parent_num_run': self.parent_num_run,
                        'num_run': self._num_run,
                        'runs': {}}
        for run_id in self.runs:
            run_metadata['runs'][run_id] = self.runs[run_id].metadata
        if not self.runs:
            run_metadata['parameter_links'] = self.template.get_parameter_links_metadata()
        return run_metadata

    @metadata.setter
    def metadata(self, value):
        self.template.load_metadata()
        self.origin_metadata = value

        run_metadata = value
        self.parent_num_run = run_metadata['parent_num_run']
        self._num_run = run_metadata['num_run']
        runs = run_metadata['runs']
        if not runs and 'parameter_links' in run_metadata:
            parameter_links = run_metadata['parameter_links']
            self.template.set_parameter_links_metadata(parameter_links)
        for run_id in runs:
            self.runs[run_id] = self.template.copy()
            self.runs[run_id].metadata = runs[run_id]
        # self.add_metadata_item('runs', )

    @property
    def independent_works(self):
        if self.runs:
            return self.runs[str(self.num_run)].independent_works
        return self.template.independent_works

    @independent_works.setter
    def independent_works(self, value):
        if self.runs:
            self.runs[str(self.num_run)].independent_works = value
        self.template.independent_works = value

    def add_next_work(self, work_id):
        if self.runs:
            self.runs[str(self.num_run)].add_next_work(work_id)
        else:
            raise Exception("There are no runs. It should not have next work")

    @property
    def last_updated_at(self):
        if self.runs:
            return self.runs[str(self.num_run)].last_updated_at
        return None

    @last_updated_at.setter
    def last_updated_at(self, value):
        if self.runs:
            self.runs[str(self.num_run)].last_updated_at = value

    @property
    def name(self):
        return self.template.name

    @name.setter
    def name(self, value):
        self.template.name = value

    @property
    def username(self):
        return self.template.username

    @username.setter
    def username(self, value):
        self.template.username = value

    @property
    def userdn(self):
        return self.template.userdn

    @userdn.setter
    def userdn(self, value):
        self.template.userdn = value

    @property
    def lifetime(self):
        return self.template.lifetime

    @lifetime.setter
    def lifetime(self, value):
        self.template.lifetime = value

    @property
    def to_cancel(self):
        return self.template.to_cancel

    @to_cancel.setter
    def to_cancel(self, value):
        if self.runs:
            self.runs[str(self.num_run)].to_cancel = value
        self.template.to_cancel = value

    @property
    def num_run(self):
        if self.parent_num_run:
            # return self.parent_num_run * 100 + self._num_run
            pass
        return self._num_run

    @num_run.setter
    def num_run(self, value):
        if self.parent_num_run:
            # self._num_run = value - self.parent_num_run * 100
            self._num_run = value
        else:
            self._num_run = value

    @property
    def transforming(self):
        if self.runs and str(self.num_run) in self.runs:
            return True
        return False

    @transforming.setter
    def transforming(self, value):
        if self._num_run < 1:
            self._num_run = 1
        if str(self.num_run) not in self.runs:
            self.runs[str(self.num_run)] = self.template.copy()
            self.runs[str(self.num_run)].num_run = self.num_run
            if self.runs[str(self.num_run)].has_loop_condition():
                self.runs[str(self.num_run)].num_run = self.num_run
                if self._num_run > 1:
                    p_metadata = self.runs[str(self.num_run - 1)].get_metadata_item('parameter_links')
                    self.runs[str(self.num_run)].add_metadata_item('parameter_links', p_metadata)

    def set_workload_id(self, workload_id):
        if self.runs:
            self.runs[str(self.num_run)].workload_id = workload_id
        else:
            self.template.workload_id = workload_id
        # self.dynamic.workload_id = workload_id

    def get_internal_id(self):
        if self.runs:
            return self.runs[str(self.num_run)].get_internal_id()
        return self.template.get_internal_id()

    def get_workload_id(self):
        if self.runs:
            return self.runs[str(self.num_run)].workload_id
        return self.template.workload_id

    def get_site(self):
        return self.template.get_site()

    def add_work(self, work, initial=False, primary=False):
        self.template.add_work(work, initial, primary)

    def has_to_build_work(self):
        return False

    def add_condition(self, cond):
        self.template.add_condition(cond)

    def add_parameter_link(self, work_source, work_destinations, parameter_link):
        self.template.add_parameter_link(work_source, work_destinations, parameter_link)

    def find_workflow_from_work(self, work):
        return self.template.find_workflow_from_work(work)

    def find_parameter_links_from_id(self, internal_id):
        if self.runs:
            return self.runs[str(self.num_run)].find_parameter_links_from_id(internal_id)
        return self.template.find_parameter_links_from_id(internal_id)

    def refresh_parameter_links(self):
        if self.runs:
            self.runs[str(self.num_run)].refresh_parameter_links()

    def set_global_parameters(self, value):
        self.template.set_global_parameters(value)

    def set_sliced_global_parameters(self, source, index=0):
        self.template.set_sliced_global_parameters(source, index)

    def sync_global_parameters_from_work(self, work):
        if self.runs:
            return self.runs[str(self.num_run)].sync_global_parameters_from_work(work)
        return self.template.sync_global_parameters_from_work(work)

    def get_new_works(self):
        self.log_debug("synchronizing works")
        self.sync_works(to_cancel=self.to_cancel)
        self.log_debug("synchronized works")
        if self.runs:
            return self.runs[str(self.num_run)].get_new_works()
        return []

    def get_current_works(self):
        self.sync_works(to_cancel=self.to_cancel)
        if self.runs:
            return self.runs[str(self.num_run)].get_current_works()
        return []

    def get_all_works(self):
        self.sync_works(to_cancel=self.to_cancel)
        if self.runs:
            return self.runs[str(self.num_run)].get_all_works()
        return []

    def get_primary_initial_collection(self):
        if self.runs:
            return self.runs[str(self.num_run)].get_primary_initial_collection()
        return self.template.get_primary_initial_collection()

    def resume_works(self):
        if self.runs:
            self.runs[str(self.num_run)].resume_works()
        self.template.to_cancel = False

    def clean_works(self):
        # if self.runs:
        #     self.runs[str(self.num_run)].clean_works()
        self.parent_num_run = None
        self._num_run = 0
        self.runs = {}

    def is_to_expire(self, expired_at=None, pending_time=None, request_id=None):
        if self.runs:
            return self.runs[str(self.num_run)].is_to_expire(expired_at=expired_at, pending_time=pending_time, request_id=request_id)
        return False

    def is_terminated(self, synchronize=True):
        if self.runs:
            if self.runs[str(self.num_run)].is_terminated():
                if not self.runs[str(self.num_run)].has_loop_condition() or not self.runs[str(self.num_run)].get_loop_condition_status():
                    return True
        return False

    def is_finished(self, synchronize=True):
        if self.is_terminated():
            return self.runs[str(self.num_run)].is_finished()
        return False

    def is_subfinished(self, synchronize=True):
        if self.is_terminated():
            return self.runs[str(self.num_run)].is_subfinished()
        return False

    def is_processed(self, synchronize=True):
        if self.is_terminated():
            return self.runs[str(self.num_run)].is_processed()
        return False

    def is_failed(self, synchronize=True):
        if self.is_terminated():
            return self.runs[str(self.num_run)].is_failed()
        return False

    def is_expired(self, synchronize=True):
        if self.is_terminated():
            return self.runs[str(self.num_run)].is_expired()
        return False

    def is_cancelled(self, synchronize=True):
        if self.is_terminated():
            return self.runs[str(self.num_run)].is_cancelled()
        return False

    def is_suspended(self, synchronize=True):
        if self.is_terminated():
            return self.runs[str(self.num_run)].is_suspended()
        return False

    def get_terminated_msg(self):
        if self.is_terminated():
            return self.runs[str(self.num_run)].get_terminated_msg()
        return None

    def get_status(self):
        if not self.runs:
            return WorkStatus.New
        if not self.is_terminated():
            return WorkStatus.Transforming
        return self.runs[str(self.num_run)].get_status()

    def depend_on(self, work):
        return self.template.depend_on(work)

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

    def refresh_works(self):
        if self.runs:
            self.runs[str(self.num_run)].refresh_works()

    def sync_works(self, to_cancel=False):
        if to_cancel:
            self.to_cancel = to_cancel

        origin_metadata = self.origin_metadata
        t_works = self.template.works
        if not t_works and hasattr(self, 'works_template'):
            self.template.works = self.works_template
            if origin_metadata:
                self.metadata = origin_metadata

        # position is end.
        if self.num_run < 1:
            self.num_run = 1
        if str(self.num_run) not in self.runs:
            self.runs[str(self.num_run)] = self.template.copy()

            t_works = self.runs[str(self.num_run)].works
            if not t_works and hasattr(self, 'works_template'):
                self.runs[str(self.num_run)].works = self.works_template
                if origin_metadata:
                    self.metadata = origin_metadata

            self.runs[str(self.num_run)].num_run = self.num_run
            if self.runs[str(self.num_run)].has_loop_condition():
                self.runs[str(self.num_run)].num_run = self.num_run
                if self.num_run > 1:
                    p_metadata = self.runs[str(self.num_run - 1)].get_metadata_item('parameter_links')
                    self.runs[str(self.num_run)].add_metadata_item('parameter_links', p_metadata)

        t_works = self.runs[str(self.num_run)].works
        if not t_works and hasattr(self, 'works_template'):
            self.runs[str(self.num_run)].works = self.works_template
            if origin_metadata:
                self.metadata = origin_metadata

        self.runs[str(self.num_run)].sync_works(to_cancel=to_cancel)

        if self.runs[str(self.num_run)].is_terminated():
            if to_cancel:
                self.logger.info("num_run %s, to cancel" % self.num_run)
            else:
                if self.runs[str(self.num_run)].has_loop_condition():
                    if self.runs[str(self.num_run)].get_loop_condition_status():
                        self.logger.info("num_run %s get_loop_condition_status %s, start next run" % (self.num_run, self.runs[str(self.num_run)].get_loop_condition_status()))
                        self._num_run += 1
                        self.runs[str(self.num_run)] = self.template.copy()

                        self.runs[str(self.num_run)].num_run = self.num_run
                        p_metadata = self.runs[str(self.num_run - 1)].get_metadata_item('parameter_links')
                        self.runs[str(self.num_run)].add_metadata_item('parameter_links', p_metadata)

                        self.runs[str(self.num_run)].global_parameters = self.runs[str(self.num_run - 1)].global_parameters
                    else:
                        self.logger.info("num_run %s get_loop_condition_status %s, terminated loop" % (self.num_run, self.runs[str(self.num_run)].get_loop_condition_status()))

    def get_relation_map(self):
        if not self.runs:
            return []
        if self.template.has_loop_condition():
            rets = {}
            for run in self.runs:
                rets[run] = self.runs[run].get_relation_map()
            return [rets]
        else:
            return self.runs[str(self.num_run)].get_relation_map()


class SubWorkflow(Workflow):
    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        # Init a workflow.
        super(SubWorkflow, self).__init__(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)


class LoopWorkflow(Workflow):
    def __init__(self, name=None, workload_id=None, lifetime=None, pending_time=None, logger=None):
        # Init a workflow.
        super(LoopWorkflow, self).__init__(name=name, workload_id=workload_id, lifetime=lifetime, pending_time=pending_time, logger=logger)
