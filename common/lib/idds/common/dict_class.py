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
Dict class.
"""

import inspect
from enum import Enum


class DictClass(object):
    def to_dict_l(self, d):
        # print('to_dict_l')
        # print(d)
        if not d:
            return d

        if hasattr(d, 'to_dict'):
            return d.to_dict()
        elif isinstance(d, dict):
            for k, v in d.items():
                d[k] = self.to_dict_l(v)
        elif isinstance(d, list):
            new_d = []
            for k in d:
                new_d.append(self.to_dict_l(k))
            return new_d
        elif inspect.ismethod(d):
            return {'idds_method': d.__name__, 'idds_method_class_id': d.__self__.get_internal_id()}
        return d

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
                else:
                    value = self.to_dict_l(value)
                ret['attributes'][key] = value
        return ret

    @staticmethod
    def is_class(d):
        if d and isinstance(d, dict) and 'class' in d and 'module' in d and 'attributes' in d:
            return True
        return False

    @staticmethod
    def is_class_method(d):
        if d and isinstance(d, dict) and 'idds_method' in d and 'idds_method_class_id' in d:
            return True
        return False

    @staticmethod
    def load_instance(d):
        module = __import__(d['module'], fromlist=[None])
        cls = getattr(module, d['class'])
        if issubclass(cls, Enum):
            impl = cls(d['attributes']['_value_'])
        else:
            impl = cls()
        return impl

    @staticmethod
    def load_instance_method(d):
        # not do anything. Will load the method in Workflow class.
        return d

    @staticmethod
    def from_dict(d):
        if not d:
            return d

        if DictClass.is_class(d):
            impl = DictClass.load_instance(d)
            for key, value in d['attributes'].items():
                if key == 'logger':
                    continue
                else:
                    value = DictClass.from_dict(value)
                setattr(impl, key, value)
            return impl
        elif DictClass.is_class_method(d):
            impl = DictClass.load_instance_method(d)
            return impl
        elif isinstance(d, dict):
            for k, v in d.items():
                d[k] = DictClass.from_dict(v)
            return d
        elif isinstance(d, list):
            new_d = []
            for k in d:
                new_d.append(DictClass.from_dict(k))
            return new_d
        else:
            return d

        return d
